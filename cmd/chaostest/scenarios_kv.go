package main

import (
	"context"
	"fmt"
	"time"
)

// Scenarios 12 and 13 exercise the thing no earlier scenario could: a REAL
// model whose inference state is genuinely serializable.
//
// Scenario 2 deliberately asserts only the SUM of restores+degraded, so it
// passes whether the adapter can checkpoint or not — with `mock` it
// restores, with `zipformer` it degrades, and both are correct. That
// adapter-independence is what made it useful across M3-M10, and it is
// also why it cannot prove the interesting claim. Scenario 12 does: it
// demands `checkpoint_restores_total` specifically, on a fleet pair
// running real weights.
//
// These require the `kv` compose profile (worker-f / worker-g). Without it
// they are skipped rather than failed — a fleet that was never asked to
// start them is not a broken fleet.

// scenario12KVCheckpointRestore: kill a zipformer_kv worker mid-utterance
// and require that recovery RESTORED the cache rather than replaying audio.
//
// This is the sentence the repository could not previously say.
func scenario12KVCheckpointRestore(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	f, okF := fleet["worker-zip-1"]
	g, okG := fleet["worker-zip-2"]
	if !okF || !okG {
		return errSkip("worker-zip-1/worker-zip-2 not in the fleet — start the kv profile: docker compose --profile kv up -d")
	}
	if err := requireUp(f, g); err != nil {
		return errSkip(err.Error())
	}

	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}

	s, pinned, err := openSessionPinnedToOneOf(ctx, wsURL, 12, f, g)
	if err != nil {
		return fmt.Errorf("could not pin a session to worker-zip-1 or worker-zip-2: %w", err)
	}

	mid := evenMidpoint(clip.PCM)
	seq, err := streamClip(ctx, s, clip.PCM[:mid], 1)
	if err != nil {
		return fmt.Errorf("first half: %w", err)
	}
	time.Sleep(checkpointSettleDelay) // the checkpoint is async and best-effort

	before, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("baseline metrics: %w", err)
	}

	if err := killWorker(pinned); err != nil {
		return fmt.Errorf("kill %s: %w", pinned.id, err)
	}

	seq, err = streamClip(ctx, s, clip.PCM[mid:], seq)
	if err != nil {
		return fmt.Errorf("second half (post-kill): %w", err)
	}
	if _, err := s.nextOfType(ctx, 20*time.Second, "partial.reset"); err != nil {
		return fmt.Errorf("no partial.reset after killing %s: %w", pinned.id, err)
	}

	after, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("post-recovery metrics: %w", err)
	}

	if after.FailoverSameModelTotal <= before.FailoverSameModelTotal {
		return fmt.Errorf("failover_same_model_total did not move (%d -> %d): worker-zip-1 and worker-zip-2 must share a compatibility key",
			before.FailoverSameModelTotal, after.FailoverSameModelTotal)
	}
	// The claim this scenario exists for. Not the sum — the restore.
	if after.CheckpointRestoresTotal <= before.CheckpointRestoresTotal {
		return fmt.Errorf(
			"checkpoint_restores_total did not increase (%d -> %d) while degraded went %d -> %d: "+
				"the session recovered by REPLAYING AUDIO, not by restoring the KV cache",
			before.CheckpointRestoresTotal, after.CheckpointRestoresTotal,
			before.CheckpointDegradedTotal, after.CheckpointDegradedTotal)
	}
	if after.DuplicateFinalsTotal != before.DuplicateFinalsTotal {
		return fmt.Errorf("duplicate_finals_total moved (%d -> %d) — must stay zero",
			before.DuplicateFinalsTotal, after.DuplicateFinalsTotal)
	}

	final, err := s.end(ctx, seq+1)
	if err != nil {
		return fmt.Errorf("no final after a KV-restored recovery: %w", err)
	}
	text, _ := final["text"].(string)
	if text == "" {
		return fmt.Errorf("final was empty after restoring a KV checkpoint")
	}

	fmt.Printf("    restored the KV cache on failover from %s (restores %d -> %d), final=%q\n",
		pinned.id, before.CheckpointRestoresTotal, after.CheckpointRestoresTotal, truncate(text, 60))
	return nil
}

// scenario13CorruptKVCheckpointDegrades: corrupt a REAL KV checkpoint and
// require that it is REFUSED rather than coerced, that recovery falls back
// to audio replay, and that the session still succeeds.
//
// Scenario 5 already does this shape — but only ever against `mock`, whose
// "checkpoint" is a pickled integer. This one corrupts 1.09 MB of real
// attention tensors, which is where validation either holds or does not.
// build-plan.md's rule: "Validation failure means audio replay. Never
// partial-restore, never coerce."
func scenario13CorruptKVCheckpointDegrades(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	f, okF := fleet["worker-zip-1"]
	g, okG := fleet["worker-zip-2"]
	if !okF || !okG {
		return errSkip("worker-zip-1/worker-zip-2 not in the fleet — start the kv profile")
	}
	if err := requireUp(f, g); err != nil {
		return errSkip(err.Error())
	}

	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}

	s, pinned, err := openSessionPinnedToOneOf(ctx, wsURL, 12, f, g)
	if err != nil {
		return fmt.Errorf("could not pin a session to worker-zip-1 or worker-zip-2: %w", err)
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
		return fmt.Errorf("gateway had no checkpoint to corrupt for %s — the async checkpoint had not landed", s.sessionID)
	}

	before, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("baseline metrics: %w", err)
	}

	if err := killWorker(pinned); err != nil {
		return fmt.Errorf("kill %s: %w", pinned.id, err)
	}
	seq, err = streamClip(ctx, s, clip.PCM[mid:], seq)
	if err != nil {
		return fmt.Errorf("second half (post-kill): %w", err)
	}
	if _, err := s.nextOfType(ctx, 20*time.Second, "partial.reset"); err != nil {
		return fmt.Errorf("no partial.reset after killing %s: %w", pinned.id, err)
	}

	after, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("post-recovery metrics: %w", err)
	}

	// The corrupted blob must NOT have been imported. Degraded is the
	// correct outcome here, and a restore would be the bug.
	if after.CheckpointRestoresTotal != before.CheckpointRestoresTotal {
		return fmt.Errorf("checkpoint_restores_total moved (%d -> %d) after the blob was corrupted — "+
			"a corrupt KV checkpoint was accepted, which is worse than any failure to recover",
			before.CheckpointRestoresTotal, after.CheckpointRestoresTotal)
	}
	if after.CheckpointDegradedTotal <= before.CheckpointDegradedTotal {
		return fmt.Errorf("checkpoint_degraded_total did not move (%d -> %d): the warm tier was not even attempted",
			before.CheckpointDegradedTotal, after.CheckpointDegradedTotal)
	}
	if after.DuplicateFinalsTotal != before.DuplicateFinalsTotal {
		return fmt.Errorf("duplicate_finals_total moved (%d -> %d) — must stay zero",
			before.DuplicateFinalsTotal, after.DuplicateFinalsTotal)
	}

	// And the session must still finish, from audio replay alone.
	final, err := s.end(ctx, seq+1)
	if err != nil {
		return fmt.Errorf("session did not survive a corrupt KV checkpoint: %w", err)
	}
	text, _ := final["text"].(string)
	if text == "" {
		return fmt.Errorf("final was empty after degrading to audio replay")
	}

	fmt.Printf("    corrupt KV checkpoint refused; degraded to replay (%d -> %d) and the session still finished: %q\n",
		before.CheckpointDegradedTotal, after.CheckpointDegradedTotal, truncate(text, 60))
	return nil
}

// --- helpers ---------------------------------------------------------

// skipErr marks a scenario that could not run because its preconditions
// are absent, as distinct from one that ran and failed. The runner prints
// it and moves on: a fleet started without the `kv` profile is not broken.
type skipErr struct{ reason string }

func (e skipErr) Error() string { return "SKIP: " + e.reason }

func errSkip(reason string) error { return skipErr{reason} }

func isSkip(err error) bool {
	_, ok := err.(skipErr)
	return ok
}

// requireUp confirms the KV workers are actually serving, not merely
// configured. A worker whose child was killed by an earlier scenario looks
// present in WORKER_URLS but answers nothing.
func requireUp(workers ...workerRef) error {
	for _, w := range workers {
		if _, err := activeSessions(w.healthURL); err != nil {
			return fmt.Errorf("%s is not answering at %s (%v)", w.id, w.healthURL, err)
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
