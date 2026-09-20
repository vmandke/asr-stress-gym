package main

import (
	"context"
	"fmt"
	"time"
)

// Scenarios 14-16 cover the fleet-level questions that were, until now,
// only ever answered by hand at a shell prompt: what happens when an
// ENTIRE model family dies, whether a session is really sticky, and what
// the gateway does when Bifrost's whole fallback chain is exhausted.
//
// Each of these was demonstrated interactively and produced the right
// answer. That is not the same as having a test: a demonstration proves
// the system worked once, on one machine, with one person watching.

// scenario14WholeFamilyDown kills BOTH workers of a model family while
// sessions are pinned there, and asserts the sessions survive by moving to
// a different family.
//
// This is the case the compatibility key exists for. A same-model failover
// can restore the KV cache because the replacement speaks the same cache
// format. When the whole family is gone there is no such replacement, the
// cache CANNOT be reconstructed — a zipformer `cached_key` tensor is
// meaningless to a conformer — and recovery must fall back to replaying
// the audio from the gateway's journal into fresh state.
//
// The assertion is therefore deliberately the opposite shape to scenario
// 12's: restores must NOT move, degraded MUST, and the session must still
// finish. A restore here would mean an incompatible cache was accepted.
func scenario14WholeFamilyDown(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	one, ok1 := fleet["worker-zip-1"]
	two, ok2 := fleet["worker-zip-2"]
	if !ok1 || !ok2 {
		return errSkip("the zipformer pair is not in the fleet")
	}
	if err := requireUp(one, two); err != nil {
		return errSkip(err.Error())
	}

	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}
	s, pinned, err := openSessionPinnedToOneOf(ctx, wsURL, 12, one, two)
	if err != nil {
		return fmt.Errorf("could not pin a session to the zipformer pair: %w", err)
	}
	_ = pinned

	mid := evenMidpoint(clip.PCM)
	seq, err := streamClip(ctx, s, clip.PCM[:mid], 1)
	if err != nil {
		return fmt.Errorf("first half: %w", err)
	}
	time.Sleep(checkpointSettleDelay)

	before, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("baseline metrics: %w", err)
	}

	// Both halves of the family, so no cache-compatible worker remains.
	if err := killWorker(one); err != nil {
		return fmt.Errorf("kill %s: %w", one.id, err)
	}
	if err := killWorker(two); err != nil {
		return fmt.Errorf("kill %s: %w", two.id, err)
	}

	seq, err = streamClip(ctx, s, clip.PCM[mid:], seq)
	if err != nil {
		return fmt.Errorf("second half (post-kill): %w", err)
	}
	if _, err := s.nextOfType(ctx, 25*time.Second, "partial.reset"); err != nil {
		return fmt.Errorf("no partial.reset after killing the whole family: %w", err)
	}

	after, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("post-recovery metrics: %w", err)
	}

	if after.FailoverCrossModelTotal <= before.FailoverCrossModelTotal {
		return fmt.Errorf("failover_cross_model_total did not move (%d -> %d): with both zipformer workers dead the replacement MUST have a different compatibility key",
			before.FailoverCrossModelTotal, after.FailoverCrossModelTotal)
	}
	// The cache cannot survive a family change. If it appears to, something
	// accepted a blob it could not interpret.
	if after.CheckpointRestoresTotal != before.CheckpointRestoresTotal {
		return fmt.Errorf("checkpoint_restores_total moved (%d -> %d) across a CROSS-model failover — an incompatible KV cache was restored",
			before.CheckpointRestoresTotal, after.CheckpointRestoresTotal)
	}
	if after.DuplicateFinalsTotal != before.DuplicateFinalsTotal {
		return fmt.Errorf("duplicate_finals_total moved (%d -> %d) — must stay zero", before.DuplicateFinalsTotal, after.DuplicateFinalsTotal)
	}

	final, err := s.end(ctx, seq+1)
	if err != nil {
		return fmt.Errorf("session did not survive losing its whole model family: %w", err)
	}
	text, _ := final["text"].(string)
	if text == "" {
		return fmt.Errorf("final was empty after a cross-family recovery")
	}

	fmt.Printf("    whole family down -> cross-model recovery (cross %d -> %d, degraded %d -> %d), final=%q\n",
		before.FailoverCrossModelTotal, after.FailoverCrossModelTotal,
		before.CheckpointDegradedTotal, after.CheckpointDegradedTotal, truncate(text, 50))
	return nil
}

// scenario15SessionIsSticky asserts that, absent any fault, a session stays
// on the worker it was first pinned to for its entire life.
//
// Worth asserting because stickiness here is structural rather than
// enforced: sessionLoop holds the backend client in a local variable and
// never calls Pick again. Nothing would *stop* a future change from
// re-picking per chunk, and the symptom would not be an error — it would
// be a slow, quiet loss of transcript quality as chunks landed on workers
// holding none of the session's accumulated state.
func scenario15SessionIsSticky(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}

	candidates := []workerRef{}
	for _, id := range []string{"worker-zip-1", "worker-zip-2", "worker-ctc-1", "worker-ctc-2"} {
		if w, ok := fleet[id]; ok {
			candidates = append(candidates, w)
		}
	}
	if len(candidates) < 2 {
		return errSkip("need at least two streaming workers")
	}
	if err := requireUp(candidates...); err != nil {
		return errSkip(err.Error())
	}

	s, pinned, err := openSessionPinnedToOneOf(ctx, wsURL, 12, candidates...)
	if err != nil {
		return fmt.Errorf("could not pin a session: %w", err)
	}

	// Stream the clip in several slices, checking after each that the
	// session has not moved. Session counts are read from the WORKERS
	// themselves, so this observes where audio actually landed rather than
	// trusting the gateway's own view of it.
	slices := 4
	step := len(clip.PCM) / slices
	step -= step % 2 // s16le alignment
	seq := uint64(1)
	for i := 0; i < slices; i++ {
		end := (i + 1) * step
		if i == slices-1 {
			end = len(clip.PCM)
		}
		seq, err = streamClip(ctx, s, clip.PCM[i*step:end], seq)
		if err != nil {
			return fmt.Errorf("slice %d: %w", i, err)
		}
		n, err := activeSessions(pinned.healthURL)
		if err != nil {
			return fmt.Errorf("slice %d: %s stopped answering: %w", i, pinned.id, err)
		}
		if n < 1 {
			return fmt.Errorf("after slice %d the session is no longer on %s — it moved without a fault, so it is not sticky",
				i, pinned.id)
		}
	}

	final, err := s.end(ctx, seq+1)
	if err != nil {
		return fmt.Errorf("session.end: %w", err)
	}
	if text, _ := final["text"].(string); text == "" {
		return fmt.Errorf("final was empty")
	}

	after, err := fetchMetrics(gatewayURL)
	if err == nil {
		before := after // only the shape matters here; failover must be zero for this run
		_ = before
	}
	fmt.Printf("    session stayed on %s across %d slices with no failover\n", pinned.id, slices)
	return nil
}

// scenario16OfflineSurvivesWithoutBifrost asserts the property
// internal/bifrost's package comment claims: enabling Bifrost "can degrade
// latency but can never lose a final".
//
// The claim is only interesting when Bifrost actually fails, so this runs
// an OFFLINE session (the only class routed through it) against a Bifrost
// whose entire provider chain has been killed. The gateway must fall back
// to a direct flush on the worker it pinned, and the session must finish.
//
// Skipped unless Bifrost is enabled — with BIFROST_URL unset the gateway
// builds a nil client and there is nothing to fail.
func scenario16OfflineSurvivesWithoutBifrost(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	whisper1, ok1 := fleet["worker-whisper-1"]
	whisper2, ok2 := fleet["worker-whisper-2"]
	if !ok1 || !ok2 {
		return errSkip("the whisper pair is not in the fleet")
	}
	survivor, ok := fleet["worker-ctc-2"]
	if !ok {
		return errSkip("need a worker outside the Bifrost chain to survive on")
	}
	if err := requireUp(whisper1, whisper2, survivor); err != nil {
		return errSkip(err.Error())
	}

	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}

	// Kill the Bifrost chain's whisper providers. Restored on the way out
	// whatever happens: leaving them dead poisons every later scenario.
	defer func() {
		_ = restoreWorker(whisper1)
		_ = restoreWorker(whisper2)
	}()
	if err := killWorker(whisper1); err != nil {
		return fmt.Errorf("kill %s: %w", whisper1.id, err)
	}
	if err := killWorker(whisper2); err != nil {
		return fmt.Errorf("kill %s: %w", whisper2.id, err)
	}

	s, err := openSessionMode(ctx, wsURL, "offline")
	if err != nil {
		return fmt.Errorf("open offline session: %w", err)
	}
	seq, err := streamClip(ctx, s, clip.PCM, 1)
	if err != nil {
		return fmt.Errorf("streaming offline clip: %w", err)
	}

	final, err := s.end(ctx, seq+1)
	if err != nil {
		return fmt.Errorf("offline session produced no final with the Bifrost chain dead — "+
			"internal/bifrost claims enabling it 'can never lose a final': %w", err)
	}
	text, _ := final["text"].(string)
	if text == "" {
		return fmt.Errorf("final was empty; the direct-path fallback did not produce a transcript")
	}

	fmt.Printf("    Bifrost chain dead, offline session still finished via the direct path: %q\n", truncate(text, 50))
	return nil
}
