package main

import (
	"context"
	"fmt"
	"time"
)

// checkpointSettleDelay is generous on purpose: these are local HTTP
// calls between containers on one host, so single-digit milliseconds is
// the realistic cost — this just guards against container/scheduler
// jitter, not a real latency budget.
const checkpointSettleDelay = 500 * time.Millisecond

// scenario2SameModelCrash: build-plan.md demo 2 — "SIGKILL worker A ->
// compatible worker chosen, checkpoint restored, tail replayed."
func scenario2SameModelCrash(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}
	s, pinned, err := openSessionPinnedToOneOf(ctx, wsURL, 10, fleet["worker-a"], fleet["worker-b"])
	if err != nil {
		return fmt.Errorf("could not get a session pinned to worker-a or worker-b: %w", err)
	}

	mid := evenMidpoint(clip.PCM)
	seq, err := streamClip(ctx, s, clip.PCM[:mid], 1)
	if err != nil {
		return fmt.Errorf("first half: %w", err)
	}
	time.Sleep(checkpointSettleDelay) // let the async checkpoint land

	before, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch baseline metrics: %w", err)
	}

	if err := killWorker(pinned); err != nil {
		return fmt.Errorf("kill %s: %w", pinned.id, err)
	}

	seq, err = streamClip(ctx, s, clip.PCM[mid:], seq)
	if err != nil {
		return fmt.Errorf("second half (post-kill): %w", err)
	}

	if _, err := s.nextOfType(ctx, 15*time.Second, "partial.reset"); err != nil {
		return fmt.Errorf("waiting for partial.reset after killing %s: %w", pinned.id, err)
	}

	after, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch post-recovery metrics: %w", err)
	}
	if after.DuplicateFinalsTotal != before.DuplicateFinalsTotal {
		return fmt.Errorf("duplicate_finals_total changed (%d -> %d) — must stay zero", before.DuplicateFinalsTotal, after.DuplicateFinalsTotal)
	}
	if after.FailoverSameModelTotal <= before.FailoverSameModelTotal {
		return fmt.Errorf("failover_same_model_total did not increase (%d -> %d) — this scenario claims recovery onto a CACHE-COMPATIBLE worker, not just any recovery",
			before.FailoverSameModelTotal, after.FailoverSameModelTotal)
	}

	// The warm-checkpoint tier is the second, independent axis: having
	// chosen a compatible worker, did the checkpoint actually get
	// restored, or did it degrade to audio replay? Exactly one must have
	// happened — asserting the SUM rather than either one keeps this
	// scenario adapter-independent. With ADAPTER=mock it restores; with
	// ADAPTER=zipformer it degrades, because sherpa-onnx cannot serialize
	// inference state. Both are correct outcomes; what would be a bug is
	// neither counter moving, which would mean the cheap path was never
	// even attempted on a worker that shares the key.
	warmBefore := before.CheckpointRestoresTotal + before.CheckpointDegradedTotal
	warmAfter := after.CheckpointRestoresTotal + after.CheckpointDegradedTotal
	if warmAfter <= warmBefore {
		return fmt.Errorf("neither checkpoint_restores_total nor checkpoint_degraded_total moved (%d -> %d) — a same-model failover must at least ATTEMPT the warm tier",
			warmBefore, warmAfter)
	}

	final, err := s.end(ctx, seq)
	if err != nil {
		return fmt.Errorf("session.end: %w", err)
	}
	if text, _ := final["text"].(string); text == "" {
		return fmt.Errorf("final carried empty text after same-model recovery")
	}
	return nil
}

// scenario3CrossModelCrash: build-plan.md demo 3 — "force model A
// unavailable -> 'cache incompatible' logged, fresh state, replay,
// partial.reset, finals preserved." Kills BOTH worker-a and worker-b (the
// only same-key pair) so whichever one was pinned, the router has no
// same-key candidate left at all and MUST cross to a different key.
func scenario3CrossModelCrash(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}
	s, _, err := openSessionPinnedToOneOf(ctx, wsURL, 10, fleet["worker-a"], fleet["worker-b"])
	if err != nil {
		return fmt.Errorf("could not get a session pinned to worker-a or worker-b: %w", err)
	}

	mid := evenMidpoint(clip.PCM)
	seq, err := streamClip(ctx, s, clip.PCM[:mid], 1)
	if err != nil {
		return fmt.Errorf("first half: %w", err)
	}
	time.Sleep(checkpointSettleDelay)

	before, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch baseline metrics: %w", err)
	}

	// Deliberately unconditional: whichever of a/b was pinned dies for
	// real; the other dies too so NO same-key candidate survives, forcing
	// a genuinely cross-model recovery rather than leaving it to chance
	// which one the router would have preferred.
	if err := killWorker(fleet["worker-a"]); err != nil {
		return fmt.Errorf("kill worker-a: %w", err)
	}
	if err := killWorker(fleet["worker-b"]); err != nil {
		return fmt.Errorf("kill worker-b: %w", err)
	}

	seq, err = streamClip(ctx, s, clip.PCM[mid:], seq)
	if err != nil {
		return fmt.Errorf("second half (post-kill): %w", err)
	}

	if _, err := s.nextOfType(ctx, 15*time.Second, "partial.reset"); err != nil {
		return fmt.Errorf("waiting for partial.reset after killing both same-key workers: %w", err)
	}

	after, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch post-recovery metrics: %w", err)
	}
	if after.DuplicateFinalsTotal != before.DuplicateFinalsTotal {
		return fmt.Errorf("duplicate_finals_total changed (%d -> %d) — must stay zero", before.DuplicateFinalsTotal, after.DuplicateFinalsTotal)
	}
	if after.FailoverCrossModelTotal <= before.FailoverCrossModelTotal {
		return fmt.Errorf("failover_cross_model_total did not increase (%d -> %d) — with both same-key workers dead, recovery MUST have crossed to a different key",
			before.FailoverCrossModelTotal, after.FailoverCrossModelTotal)
	}

	final, err := s.end(ctx, seq)
	if err != nil {
		return fmt.Errorf("session.end: %w", err)
	}
	if text, _ := final["text"].(string); text == "" {
		return fmt.Errorf("final carried empty text after cross-model recovery — finals must be preserved")
	}
	return nil
}

// scenario5CorruptCheckpoint: build-plan.md demo 5 — "corrupt the blob ->
// validation fails, falls back to audio replay, session still succeeds."
// Uses the gateway's debug hook (checkpoints live gateway-side — see
// internal/backend.CheckpointResp's doc comment) rather than the
// worker-side /admin/corrupt flag: both routes exercise the same
// CanRestore check, and corrupting where the checkpoint actually lives
// is the more direct proof.
func scenario5CorruptCheckpoint(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}
	// Checkpoint corruption is meaningful only on the one adapter that can
	// actually serialize a checkpoint. Before M5 every worker was mock, so
	// an unpinned session happened to satisfy that precondition; on the real
	// heterogeneous fleet it made this scenario a coin flip between mock and
	// a permanent 501 from a real adapter. Pin deliberately to the warm tier
	// the scenario is testing.
	s, pinned, err := openSessionPinnedToOneOf(ctx, wsURL, 10, fleet["worker-mock"])
	if err != nil {
		return err
	}

	mid := evenMidpoint(clip.PCM)
	seq, err := streamClip(ctx, s, clip.PCM[:mid], 1)
	if err != nil {
		return fmt.Errorf("first half: %w", err)
	}
	time.Sleep(checkpointSettleDelay)

	corrupted, err := corruptCheckpoint(gatewayURL, s.sessionID)
	if err != nil {
		return fmt.Errorf("corrupt-checkpoint call: %w", err)
	}
	if !corrupted {
		return fmt.Errorf("gateway reported no checkpoint to corrupt for session %s — the async checkpoint likely hadn't landed yet", s.sessionID)
	}

	before, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch baseline metrics: %w", err)
	}
	if err := killWorker(pinned); err != nil {
		return fmt.Errorf("kill %s: %w", pinned.id, err)
	}

	seq, err = streamClip(ctx, s, clip.PCM[mid:], seq)
	if err != nil {
		return fmt.Errorf("second half (post-kill): %w", err)
	}

	if _, err := s.nextOfType(ctx, 15*time.Second, "partial.reset"); err != nil {
		return fmt.Errorf("waiting for partial.reset after killing %s (corrupted checkpoint): %w", pinned.id, err)
	}

	after, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch post-recovery metrics: %w", err)
	}
	if after.DuplicateFinalsTotal != before.DuplicateFinalsTotal {
		return fmt.Errorf("duplicate_finals_total changed (%d -> %d) — must stay zero", before.DuplicateFinalsTotal, after.DuplicateFinalsTotal)
	}
	// This scenario's claim is about the CHECKPOINT, not about which
	// worker was chosen — the session opens unpinned, so the replacement
	// may legitimately share the dead worker's key or not, and asserting
	// on same/cross would be asserting on a coin flip. What must hold
	// unconditionally is invariant 8: a corrupted blob is never restored
	// from, and the session survives anyway on audio replay.
	if after.CheckpointRestoresTotal != before.CheckpointRestoresTotal {
		return fmt.Errorf("checkpoint_restores_total increased (%d -> %d) — a CORRUPTED checkpoint must never be restored from",
			before.CheckpointRestoresTotal, after.CheckpointRestoresTotal)
	}
	if after.FailoverTotal <= before.FailoverTotal {
		return fmt.Errorf("failover_total did not increase (%d -> %d) — the session must have recovered, via audio replay, despite the corrupted checkpoint",
			before.FailoverTotal, after.FailoverTotal)
	}

	final, err := s.end(ctx, seq)
	if err != nil {
		return fmt.Errorf("session.end: %w", err)
	}
	if text, _ := final["text"].(string); text == "" {
		return fmt.Errorf("final carried empty text — the session did not actually succeed despite the corrupted checkpoint")
	}
	return nil
}
