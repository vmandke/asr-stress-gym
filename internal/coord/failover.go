package coord

import (
	"context"
	"fmt"
	"time"

	"asr-stress-gym/internal/audio"
	"asr-stress-gym/internal/backend"
	"asr-stress-gym/internal/journal"
	"asr-stress-gym/internal/metrics"
	"asr-stress-gym/internal/router"
	"asr-stress-gym/internal/session"
)

// MaxFailoverAttempts bounds the recovery loop — implementation-plan.md
// defect #6: build-plan.md's handleBackendFailure recurses through both
// recovery functions with its attempt guard checked AFTER router.Pick,
// which is unbounded if Pick keeps returning targets that themselves
// fail to recover. HandleBackendFailure below is an explicit, bounded
// for-loop instead, with the guard as the loop condition itself.
const MaxFailoverAttempts = 3

// RecoveryDeps bundles what both recovery algorithms and
// HandleBackendFailure need. Constructed fresh by the caller
// (cmd/gateway) for each failure — this package holds no per-session
// state; CheckpointStore and Router are the only process-lifetime
// pieces, and both are passed in explicitly, not owned here.
type RecoveryDeps struct {
	State       *session.InferenceState
	Emitter     *session.Emitter
	Pipeline    audio.Pipeline
	Journal     *journal.Journal
	Router      *router.Router
	Checkpoints *CheckpointStore
}

// replayChunks pushes chunks to client in order, keeping deps.State's
// Generation/LastAppliedSeq current as each one lands. Shared by both
// recovery modes — the only difference between them is which records get
// Recut into chunks in the first place (a short tail vs. everything since
// the last committed final).
//
// Returns the last chunk's response text, or nil if chunks was empty —
// build-plan.md's Mode 2 sequence is explicit that replay is followed by
// "regenerate the current partial" before resuming; a nil return tells
// the caller there is nothing new to regenerate (e.g. a checkpoint taken
// at exactly the current seq, nothing to replay), so it should NOT
// synthesize a partial the recovery has no actual basis for.
func replayChunks(ctx context.Context, client backend.Client, deps RecoveryDeps, chunks []audio.Chunk) (*string, error) {
	var lastText *string
	for _, c := range chunks {
		resp, err := client.Push(ctx, backend.PushReq{
			Handle:             deps.State.Handle,
			SeqStart:           c.SeqStart,
			SeqEnd:             c.SeqEnd,
			ExpectedGeneration: deps.State.Generation,
			Audio:              c.Bytes,
		})
		if err != nil {
			return nil, fmt.Errorf("coord: replay push seq [%d,%d]: %w", c.SeqStart, c.SeqEnd, err)
		}
		deps.State.Generation = resp.Generation
		deps.State.LastAppliedSeq = resp.LastSeqApplied
		lastText = &resp.Text
	}
	return lastText, nil
}

// RecoverSameModel implements build-plan.md Mode 1: restore a checkpoint
// on a compatible worker and replay only the tail (seq after the
// checkpoint). Degrades to RecoverCrossModel — never partial-restore,
// never coerce — if there is no checkpoint for this session, it fails
// CanRestore (wrong key, or a corrupted/mismatched checksum — chaos
// scenario 5), or the target's Restore call itself errors.
//
// The *string return is the regenerated current partial's text
// (build-plan.md: replay, then "regenerate the current partial", then
// reset+resume) — nil if the tail was empty and there is nothing new to
// show, non-nil (even if it points at "") if the tail replay actually
// ran and this IS the current text.
func RecoverSameModel(ctx context.Context, deps RecoveryDeps, target *router.Worker) (backend.Client, session.PartialResetEvent, *string, error) {
	cp, ok := deps.Checkpoints.Latest(deps.State.SessionID)
	if !ok || !CanRestore(cp, target.CompatibilityKey) {
		metrics.CheckpointDegradedTotal.Add(1)
		return RecoverCrossModel(ctx, deps, target)
	}
	resp, err := target.Client.Restore(ctx, backend.RestoreReq{CheckpointBlob: cp.StateBlob, LastSeqApplied: cp.Seq})
	if err != nil {
		metrics.CheckpointDegradedTotal.Add(1)
		return RecoverCrossModel(ctx, deps, target)
	}

	deps.State.WorkerID = target.ID
	deps.State.Handle = resp.Handle
	deps.State.CompatibilityKey = target.CompatibilityKey
	deps.State.Generation = resp.Generation
	deps.State.LastAppliedSeq = resp.LastSeqApplied
	deps.State.FailoverEpoch++
	resetEv := deps.Emitter.PartialReset() // always — implementation-plan.md defect #9

	tail, err := deps.Pipeline.Recut(deps.Journal.ReadAfter(resp.LastSeqApplied))
	if err != nil {
		return nil, resetEv, nil, fmt.Errorf("coord: recut tail for same-model replay: %w", err)
	}
	text, err := replayChunks(ctx, target.Client, deps, tail)
	if err != nil {
		return nil, resetEv, nil, err
	}
	metrics.FailoverTotal.Add(1)
	metrics.CheckpointRestoresTotal.Add(1)
	return target.Client, resetEv, text, nil
}

// RecoverCrossModel implements build-plan.md Mode 2: fresh state on the
// target (NEVER deserializing the dead worker's state into it — the old
// state is meaningless across an incompatible key) and a full replay from
// the last committed FINAL boundary, bounded exactly there per
// build-plan.md's "Bounding replay cost". Also the degrade-path
// RecoverSameModel falls back to when no valid checkpoint exists, even
// between two workers that DO share a key — build-plan.md's own
// pseudocode uses the same function for both, and this keeps that
// fidelity rather than inventing a third near-identical path.
//
// Because of that reuse, this function does NOT decide whether a failover
// was same-model or cross-model: reaching here says only that a full
// replay happened, which is true of both. HandleBackendFailure owns that
// distinction, since it is where the keys are compared.
func RecoverCrossModel(ctx context.Context, deps RecoveryDeps, target *router.Worker) (backend.Client, session.PartialResetEvent, *string, error) {
	resp, err := target.Client.Open(ctx, backend.OpenReq{
		SessionID:    deps.State.SessionID,
		SampleRateHz: deps.State.SampleRateHz,
		Mode:         string(deps.State.Mode),
	})
	if err != nil {
		return nil, session.PartialResetEvent{}, nil, fmt.Errorf("coord: open fresh state on %s: %w", target.ID, err)
	}

	deps.State.WorkerID = target.ID
	deps.State.Handle = resp.Handle
	deps.State.CompatibilityKey = target.CompatibilityKey
	deps.State.Generation = resp.Generation
	deps.State.FailoverEpoch++
	resetEv := deps.Emitter.PartialReset()

	records := deps.Journal.ReadFromCommitted()
	chunks, err := deps.Pipeline.Recut(records)
	if err != nil {
		return nil, resetEv, nil, fmt.Errorf("coord: recut from committed for cross-model replay: %w", err)
	}
	text, err := replayChunks(ctx, target.Client, deps, chunks)
	if err != nil {
		return nil, resetEv, nil, err
	}
	metrics.FailoverTotal.Add(1)
	return target.Client, resetEv, text, nil
}

// HandleBackendFailure is build-plan.md's handleBackendFailure, corrected
// per implementation-plan.md defect #6 (see MaxFailoverAttempts). Reports
// the failure to the router, unbinds the dead worker, then repeatedly
// picks a replacement (excluding every worker already tried this call)
// and recovers via whichever mode the replacement's compatibility key
// implies, until one succeeds or attempts are exhausted.
//
// Returns the new client to use going forward, the partial.reset event,
// and the regenerated current partial text (nil if the replay had
// nothing to regenerate — see replayChunks) — all three are the
// CALLER's to act on (send the events, swap the client) since this
// package has no access to the events channel or the connection's own
// client variable. Returns an error only once every attempt is
// exhausted, which the caller should treat as session-terminal.
func HandleBackendFailure(ctx context.Context, deps RecoveryDeps, cause error) (backend.Client, session.PartialResetEvent, *string, error) {
	excluded := map[string]bool{deps.State.WorkerID: true}
	deps.Router.Report(deps.State.WorkerID, false, 0)
	if dead, ok := deps.Router.Find(deps.State.WorkerID); ok {
		dead.UnbindSession()
	}

	lastErr := cause
	for attempt := 1; attempt <= MaxFailoverAttempts; attempt++ {
		previous := *deps.State
		target, err := deps.Router.Pick(deps.State.Mode, excluded, deps.State.CompatibilityKey)
		if err != nil {
			return nil, session.PartialResetEvent{}, nil, fmt.Errorf("coord: no replacement worker available: %w", err)
		}

		var client backend.Client
		var resetEv session.PartialResetEvent
		var text *string
		sameKey := target.CompatibilityKey == deps.State.CompatibilityKey
		if sameKey {
			client, resetEv, text, err = RecoverSameModel(ctx, deps, target)
		} else {
			client, resetEv, text, err = RecoverCrossModel(ctx, deps, target)
		}
		if err == nil {
			// Counted HERE, not inside the two recovery functions, because
			// this is the only place the key comparison is actually made.
			// RecoverSameModel degrades by CALLING RecoverCrossModel when
			// no usable checkpoint exists, so a counter incremented down
			// there cannot tell "the replacement had a different key" from
			// "the replacement had the same key but nothing to restore" —
			// which is every same-model failover on the real fleet. See
			// internal/metrics for the two axes.
			if sameKey {
				metrics.FailoverSameModelTotal.Add(1)
			} else {
				metrics.FailoverCrossModelTotal.Add(1)
			}
			target.BindSession()
			deps.Router.Report(target.ID, true, 0)
			return client, resetEv, text, nil
		}

		// The recovery functions install a candidate handle before replaying
		// into it. Replay can itself fail, though. Do not let that unbound
		// candidate become the session's apparent owner: a later retry must
		// start from the last known-good owner/key, and terminal cleanup must
		// not UnbindSession on a worker which was never bound. Close the
		// disposable handle too. PartialReset only constructs an event, so
		// restoring State cannot leave the emitter inconsistent.
		// Handles are opaque only within a worker: two different workers may
		// legitimately issue the same handle string, so ownership must be
		// part of the comparison.
		if deps.State.WorkerID != previous.WorkerID || deps.State.Handle != previous.Handle {
			closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = target.Client.Close(closeCtx, deps.State.Handle)
			cancel()
		}
		*deps.State = previous
		lastErr = err
		excluded[target.ID] = true
		deps.Router.Report(target.ID, false, 0)
	}
	metrics.FailoverExhaustedTotal.Add(1)
	return nil, session.PartialResetEvent{}, nil, fmt.Errorf("coord: failover exhausted after %d attempts: %w", MaxFailoverAttempts, lastErr)
}
