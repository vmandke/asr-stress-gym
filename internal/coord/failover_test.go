package coord

import (
	"context"
	"errors"
	"testing"

	"asr-stress-gym/internal/audio"
	"asr-stress-gym/internal/backend"
	"asr-stress-gym/internal/journal"
	"asr-stress-gym/internal/metrics"
	"asr-stress-gym/internal/router"
	"asr-stress-gym/internal/session"
	"asr-stress-gym/internal/wire"
)

const testSampleRateHz = 16000
const testChunkMs = 40 // 2 nominal 20ms frames per chunk — compact test fixtures, still exercises multiple chunks

// driveFrames ingests frames [fromSeq, toSeq) at a nominal 20ms each into
// pipeline, journals every one (mirroring cmd/gateway/conn.go's
// handleAudioFrame), and pushes any resulting Ready() chunks to client,
// keeping state's Generation/LastAppliedSeq current — the same shape a
// live session's hot path follows, so tests exercise a REALISTIC
// journal/pipeline/backend combination rather than hand-picked fixtures
// that might not correspond to anything the real system would produce.
func driveFrames(t *testing.T, p audio.Pipeline, j *journal.Journal, client backend.Client, state *session.InferenceState, fromSeq, toSeq uint64) {
	t.Helper()
	for seq := fromSeq; seq < toSeq; seq++ {
		f := wire.Frame{Type: wire.MsgAudio, Seq: seq, NumSamples: 320, Payload: []byte{byte(seq), byte(seq >> 8)}}
		ref, err := p.Ingest(f)
		if err != nil {
			t.Fatalf("Ingest seq=%d: %v", seq, err)
		}
		j.Append(audio.Record{Seq: seq, DurationMs: ref.DurationMs, Voiced: ref.Voiced, Payload: f.Payload})

		for _, c := range p.Ready() {
			resp, err := client.Push(context.Background(), backend.PushReq{
				Handle: state.Handle, SeqStart: c.SeqStart, SeqEnd: c.SeqEnd,
				ExpectedGeneration: state.Generation, Audio: c.Bytes,
			})
			if err != nil {
				t.Fatalf("driveFrames: push seq[%d,%d]: %v", c.SeqStart, c.SeqEnd, err)
			}
			state.Generation = resp.Generation
			state.LastAppliedSeq = resp.LastSeqApplied
		}
	}
}

// setup builds one pre-failure session: an OLD worker (via oldBackend)
// that has already processed frames [0, checkpointAtSeq) when a
// checkpoint is taken, then continues to [checkpointAtSeq, totalSeq)
// before it "dies" (the test stops using oldBackend and calls a recovery
// function directly). Returns everything a recovery function needs.
func setup(t *testing.T, key session.CacheCompatibilityKey, checkpointAtSeq, totalSeq uint64, store *CheckpointStore) (RecoveryDeps, *fakeBackend) {
	t.Helper()
	state := session.NewInferenceState("s1", session.ModeOnline)
	state.SampleRateHz = testSampleRateHz
	state.CompatibilityKey = key
	state.WorkerID = "worker-old"
	emitter := session.NewEmitter(state)
	pipeline := audio.NewPassthroughPipeline(testSampleRateHz, testChunkMs)
	j := journal.New(1000)

	old := newFakeBackend()
	openResp, err := old.Open(context.Background(), backend.OpenReq{SessionID: state.SessionID, SampleRateHz: testSampleRateHz, Mode: "online"})
	if err != nil {
		t.Fatalf("old.Open: %v", err)
	}
	state.Handle = openResp.Handle

	driveFrames(t, pipeline, j, old, state, 0, checkpointAtSeq)
	if store != nil {
		cp, err := old.Checkpoint(context.Background(), state.Handle)
		if err != nil {
			t.Fatalf("old.Checkpoint: %v", err)
		}
		store.Store(state.SessionID, key, cp)
	}
	driveFrames(t, pipeline, j, old, state, checkpointAtSeq, totalSeq)

	rt := router.New(nil) // Router is only used by HandleBackendFailure, not the recovery functions directly; tests that need Pick construct their own
	deps := RecoveryDeps{
		State: state, Emitter: emitter, Pipeline: pipeline, Journal: j,
		Router: rt, Checkpoints: store,
	}
	return deps, old
}

func mockCaps() backend.Capabilities {
	return backend.Capabilities{Streaming: true, Serializable: true, Modes: []string{"online", "offline"}}
}

// --- RecoverSameModel ---

func TestRecoverSameModelWithValidCheckpointReplaysOnlyTheTail(t *testing.T) {
	store := NewCheckpointStore()
	deps, _ := setup(t, "K1", 4, 10, store) // checkpoint after 4 frames, 10 total

	target := router.NewWorker("worker-b", newFakeBackend(), "K1", mockCaps())
	client, resetEv, _, err := RecoverSameModel(context.Background(), deps, target)
	if err != nil {
		t.Fatalf("RecoverSameModel: %v", err)
	}
	if resetEv.Type != "partial.reset" {
		t.Fatalf("got %+v", resetEv)
	}
	if deps.State.WorkerID != "worker-b" || deps.State.FailoverEpoch != 1 {
		t.Fatalf("state after recovery: WorkerID=%s FailoverEpoch=%d, want worker-b/1", deps.State.WorkerID, deps.State.FailoverEpoch)
	}

	// The new worker's accumulated text must be the CHECKPOINT's text
	// (frames 0-3) plus ONLY the tail (frames 4-9) — never re-deriving
	// frames 0-3 from scratch, and never missing 4-9. deps.State.Handle
	// was already rebound to the new worker's handle by RecoverSameModel.
	flush, err := client.Flush(context.Background(), deps.State.Handle)
	if err != nil {
		t.Fatalf("Flush on new worker: %v", err)
	}
	if flush.Text == "" {
		t.Fatal("new worker's text is empty — checkpoint state did not carry over")
	}
	// The checkpoint blob (frames 0-3's accumulated text) must be a
	// PREFIX of the final text — proving the tail was APPENDED, not
	// replacing the checkpointed history.
	cp, _ := store.Latest(deps.State.SessionID)
	if len(flush.Text) <= len(cp.StateBlob) {
		t.Fatalf("final text %q is not longer than the checkpoint blob %q — tail was not replayed", flush.Text, cp.StateBlob)
	}
	if flush.Text[:len(cp.StateBlob)] != string(cp.StateBlob) {
		t.Fatalf("final text %q does not start with checkpoint blob %q — checkpoint state was not preserved", flush.Text, cp.StateBlob)
	}
}

func TestRecoverSameModelDegradesWithNoCheckpoint(t *testing.T) {
	deps, _ := setup(t, "K1", 4, 10, nil) // store=nil: never checkpointed
	target := router.NewWorker("worker-b", newFakeBackend(), "K1", mockCaps())

	client, resetEv, _, err := RecoverSameModel(context.Background(), deps, target)
	if err != nil {
		t.Fatalf("RecoverSameModel: %v", err)
	}
	if resetEv.Type != "partial.reset" {
		t.Fatalf("got %+v", resetEv)
	}
	fb := target.Client.(*fakeBackend)
	if fb.restoreCalls != 0 {
		t.Fatalf("Restore was called %d times, want 0 — no checkpoint exists, so this must go straight to fresh state", fb.restoreCalls)
	}
	if fb.openCalls != 1 {
		t.Fatalf("Open was called %d times, want 1 (the cross-model degrade path)", fb.openCalls)
	}

	// Fresh state replayed from committed (seq 0, nothing committed yet)
	// must reproduce ALL 10 frames' worth of content — the full history,
	// not just the post-"checkpoint" tail, since there was no checkpoint.
	flush, _ := client.Flush(context.Background(), deps.State.Handle)
	if flush.Text == "" {
		t.Fatal("fresh-state replay produced no text")
	}
}

func TestRecoverSameModelDegradesOnWrongKeyCheckpoint(t *testing.T) {
	store := NewCheckpointStore()
	deps, _ := setup(t, "K1", 4, 10, store)
	// A checkpoint exists, but for a DIFFERENT key than the target — must
	// not be trusted (build-plan.md: "Treat all of these as incompatible
	// unless explicitly proven otherwise").
	target := router.NewWorker("worker-c", newFakeBackend(), "K2", mockCaps())

	_, _, _, err := RecoverSameModel(context.Background(), deps, target)
	if err != nil {
		t.Fatalf("RecoverSameModel: %v", err)
	}
	fb := target.Client.(*fakeBackend)
	if fb.restoreCalls != 0 {
		t.Fatalf("Restore was called %d times, want 0 — checkpoint key doesn't match target's", fb.restoreCalls)
	}
	if fb.openCalls != 1 {
		t.Fatalf("Open was called %d times, want 1", fb.openCalls)
	}
}

func TestRecoverSameModelDegradesOnCorruptedChecksum(t *testing.T) {
	store := NewCheckpointStore()
	deps, _ := setup(t, "K1", 4, 10, store)
	store.Corrupt(deps.State.SessionID) // chaos scenario 5's mechanism
	target := router.NewWorker("worker-b", newFakeBackend(), "K1", mockCaps())

	client, _, _, err := RecoverSameModel(context.Background(), deps, target)
	if err != nil {
		t.Fatalf("RecoverSameModel: %v, want a successful degrade to audio replay, not an error — build-plan.md: session still succeeds", err)
	}
	fb := target.Client.(*fakeBackend)
	if fb.restoreCalls != 0 {
		t.Fatalf("Restore was called %d times, want 0 — corrupted checksum must be caught before ever calling Restore", fb.restoreCalls)
	}
	flush, _ := client.Flush(context.Background(), deps.State.Handle)
	if flush.Text == "" {
		t.Fatal("degraded fresh-state replay produced no text — session did not actually succeed")
	}
}

func TestRecoverSameModelDegradesWhenRestoreItselfErrors(t *testing.T) {
	store := NewCheckpointStore()
	deps, _ := setup(t, "K1", 4, 10, store)
	fb := newFakeBackend()
	fb.restoreErr = errors.New("worker rejected the checkpoint")
	target := router.NewWorker("worker-b", fb, "K1", mockCaps())

	_, _, _, err := RecoverSameModel(context.Background(), deps, target)
	if err != nil {
		t.Fatalf("RecoverSameModel: %v, want a degrade to cross-model on Restore failure", err)
	}
	if fb.restoreCalls != 1 {
		t.Fatalf("Restore was called %d times, want exactly 1 (attempted, then degraded)", fb.restoreCalls)
	}
	if fb.openCalls != 1 {
		t.Fatalf("Open was called %d times, want 1 (the degrade path)", fb.openCalls)
	}
}

// --- RecoverCrossModel ---

func TestRecoverCrossModelNeverCallsRestore(t *testing.T) {
	deps, _ := setup(t, "K1", 4, 10, nil)
	target := router.NewWorker("worker-c", newFakeBackend(), "K2", mockCaps())

	_, resetEv, _, err := RecoverCrossModel(context.Background(), deps, target)
	if err != nil {
		t.Fatalf("RecoverCrossModel: %v", err)
	}
	fb := target.Client.(*fakeBackend)
	if fb.restoreCalls != 0 {
		t.Fatalf("Restore was called %d times, want 0 — build-plan.md rule: never deserialize M1 state into M2", fb.restoreCalls)
	}
	if fb.openCalls != 1 {
		t.Fatalf("Open was called %d times, want 1", fb.openCalls)
	}
	if resetEv.FailoverEpoch != 1 {
		t.Fatalf("partial.reset FailoverEpoch = %d, want 1", resetEv.FailoverEpoch)
	}
}

func TestRecoverCrossModelReplaysFromCommittedNotFromCheckpoint(t *testing.T) {
	// Even WITH a valid checkpoint present, RecoverCrossModel (called
	// directly — e.g. because the router picked an incompatible worker in
	// the first place) must ignore it entirely and replay from the last
	// committed final, per build-plan.md Mode 2.
	store := NewCheckpointStore()
	deps, _ := setup(t, "K1", 4, 10, store)
	target := router.NewWorker("worker-c", newFakeBackend(), "K2", mockCaps())

	client, _, _, err := RecoverCrossModel(context.Background(), deps, target)
	if err != nil {
		t.Fatalf("RecoverCrossModel: %v", err)
	}
	flush, _ := client.Flush(context.Background(), deps.State.Handle)
	// Nothing has been committed (no FINAL yet), so ReadFromCommitted
	// returns everything — all 10 frames' worth, not just the 6-frame tail
	// a checkpoint-aware path would have used.
	if flush.Text == "" {
		t.Fatal("cross-model replay from committed produced no text")
	}
}

func TestRecoverCrossModelFailsCleanlyWhenOpenErrors(t *testing.T) {
	deps, _ := setup(t, "K1", 4, 10, nil)
	fb := newFakeBackend()
	fb.openErr = errors.New("worker unreachable")
	target := router.NewWorker("worker-c", fb, "K2", mockCaps())

	_, _, _, err := RecoverCrossModel(context.Background(), deps, target)
	if err == nil {
		t.Fatal("expected an error when the target's Open itself fails")
	}
}

// --- HandleBackendFailure: the bounded loop (implementation-plan.md defect #6) ---

func TestHandleBackendFailureSucceedsOnFirstAttempt(t *testing.T) {
	deps, _ := setup(t, "K1", 4, 10, nil)
	b := router.NewWorker("worker-b", newFakeBackend(), "K1", mockCaps())
	deps.Router = router.New([]*router.Worker{b})

	client, resetEv, _, err := HandleBackendFailure(context.Background(), deps, errors.New("connection reset"))
	if err != nil {
		t.Fatalf("HandleBackendFailure: %v", err)
	}
	if client == nil {
		t.Fatal("expected a non-nil client on success")
	}
	if resetEv.Type != "partial.reset" {
		t.Fatalf("got %+v", resetEv)
	}
	if b.Outstanding() != 1 {
		t.Fatalf("winning worker's Outstanding = %d, want 1 (bound after a successful recovery)", b.Outstanding())
	}
}

func TestHandleBackendFailureUnbindsDeadWorkerFirst(t *testing.T) {
	deps, _ := setup(t, "K1", 4, 10, nil)
	dead := router.NewWorker("worker-old", newFakeBackend(), "K1", mockCaps())
	dead.BindSession() // the session was pinned here before it died
	good := router.NewWorker("worker-b", newFakeBackend(), "K1", mockCaps())
	deps.Router = router.New([]*router.Worker{dead, good})

	if _, _, _, err := HandleBackendFailure(context.Background(), deps, errors.New("dead")); err != nil {
		t.Fatalf("HandleBackendFailure: %v", err)
	}
	if dead.Outstanding() != 0 {
		t.Fatalf("dead worker's Outstanding = %d, want 0 (unbound on failure)", dead.Outstanding())
	}
}

// This is defect #6's own test: a target that is PICKED but whose
// recovery itself keeps failing must not retry it forever, and must not
// recurse unboundedly — it tries a bounded number of DIFFERENT targets
// and then gives up cleanly.
func TestHandleBackendFailureRetriesADifferentTargetOnRecoveryFailure(t *testing.T) {
	deps, _ := setup(t, "K1", 4, 10, nil)

	badFB := newFakeBackend()
	badFB.openErr = errors.New("this worker is also broken")
	bad := router.NewWorker("worker-bad", badFB, "K2", mockCaps()) // K2: forces RecoverCrossModel's Open path, which badFB fails
	good := router.NewWorker("worker-good", newFakeBackend(), "K2", mockCaps())
	deps.Router = router.New([]*router.Worker{bad, good})

	// Exclude nothing up front; Pick's own scoring may choose either
	// first since both share a key and neither is preferred over the
	// other by compatibility (deps.State.CompatibilityKey is K1, matching
	// neither) — so this test only asserts the OUTCOME (eventual
	// success), not which one was tried first.
	client, _, _, err := HandleBackendFailure(context.Background(), deps, errors.New("original failure"))
	if err != nil {
		t.Fatalf("HandleBackendFailure: %v, want eventual success via the other worker", err)
	}
	if client == nil {
		t.Fatal("expected a non-nil client")
	}
}

func TestHandleBackendFailureExhaustsBoundedAttempts(t *testing.T) {
	deps, _ := setup(t, "K1", 4, 10, nil)

	var workers []*router.Worker
	for i := 0; i < MaxFailoverAttempts+2; i++ {
		fb := newFakeBackend()
		fb.openErr = errors.New("permanently broken")
		workers = append(workers, router.NewWorker(fakeWorkerID(i), fb, "K2", mockCaps()))
	}
	deps.Router = router.New(workers)

	_, _, _, err := HandleBackendFailure(context.Background(), deps, errors.New("original failure"))
	if err == nil {
		t.Fatal("expected an error: every available target's recovery fails")
	}

	totalOpenAttempts := 0
	for _, w := range workers {
		totalOpenAttempts += w.Client.(*fakeBackend).openCalls
	}
	if totalOpenAttempts > MaxFailoverAttempts {
		t.Fatalf("made %d Open attempts across all targets, want at most %d (the bound) — defect #6: this must be a bounded loop, not unbounded recursion",
			totalOpenAttempts, MaxFailoverAttempts)
	}
}

// A replay failure happens after RecoverCrossModel has opened a temporary
// handle and installed it in State.  That candidate is not yet bound, so it
// must be cleaned up and State restored before the bounded loop gives up;
// otherwise cmd/gateway's defer would unbind the wrong worker and drive its
// outstanding count negative.
func TestHandleBackendFailureRestoresOwnerAfterReplayFailure(t *testing.T) {
	deps, _ := setup(t, "K1", 4, 10, nil)
	previous := *deps.State

	badBackend := newFakeBackend()
	badBackend.pushErr = errors.New("replacement died while replaying")
	bad := router.NewWorker("worker-bad", badBackend, "K2", mockCaps())
	deps.Router = router.New([]*router.Worker{bad})

	if _, _, _, err := HandleBackendFailure(context.Background(), deps, errors.New("original worker died")); err == nil {
		t.Fatal("expected recovery to fail when its only replacement dies during replay")
	}
	if got := *deps.State; got != previous {
		t.Fatalf("state after failed recovery = %+v, want original owner %+v", got, previous)
	}
	if bad.Outstanding() != 0 {
		t.Fatalf("failed replacement Outstanding = %d, want 0 (it was never bound)", bad.Outstanding())
	}
	badBackend.mu.Lock()
	remaining := len(badBackend.records)
	badBackend.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("failed replacement left %d temporary handle(s) open", remaining)
	}
}

func TestHandleBackendFailureReturnsNoCapacityWhenNoOtherWorkerExists(t *testing.T) {
	deps, _ := setup(t, "K1", 4, 10, nil)
	deps.Router = router.New(nil) // no workers at all

	_, _, _, err := HandleBackendFailure(context.Background(), deps, errors.New("dead"))
	if err == nil {
		t.Fatal("expected an error when there is no capacity")
	}
}

func fakeWorkerID(i int) string {
	return "worker-bad-" + string(rune('a'+i))
}

// --- the two failover axes, counted separately ---

// readCounters snapshots the process-lifetime metrics these tests assert
// deltas on. The counters are package-level atomics shared by every test
// in this binary, so an absolute value is meaningless here; only the
// change across one call is.
func readCounters() (sameModel, crossModel, restores, degraded int64) {
	return metrics.FailoverSameModelTotal.Load(),
		metrics.FailoverCrossModelTotal.Load(),
		metrics.CheckpointRestoresTotal.Load(),
		metrics.CheckpointDegradedTotal.Load()
}

// A same-key replacement that CANNOT restore a checkpoint is still a
// same-model failover. It just recovers by replaying audio instead of by
// restoring state.
//
// This is the case the whole real fleet lives in: worker-a and worker-b
// run identical weights and advertise one compatibility key, and neither
// can serialize inference state, because sherpa-onnx has no API for it.
// Before M5 the only same-key pair in the fleet was two mock workers —
// and mock IS serializable — so "same key" and "checkpoint restored" had
// the same answer in every case that existed and the code conflated them:
// the degrade path incremented failover_cross_model_total because it
// reached that counter by CALLING RecoverCrossModel. Standing the real
// adapters up made chaos scenario 2 fail with
// failover_same_model_total 0 -> 0 on a failover between two workers
// running the same model, which is how this was found.
func TestSameKeyFailoverWithoutACheckpointStillCountsAsSameModel(t *testing.T) {
	deps, _ := setup(t, "K1", 4, 10, nil) // nil store: nothing to restore, exactly like a non-serializable adapter
	b := router.NewWorker("worker-b", newFakeBackend(), "K1", mockCaps())
	deps.Router = router.New([]*router.Worker{b})

	same0, cross0, restores0, degraded0 := readCounters()
	if _, _, _, err := HandleBackendFailure(context.Background(), deps, errors.New("dead")); err != nil {
		t.Fatalf("HandleBackendFailure: %v", err)
	}
	same1, cross1, restores1, degraded1 := readCounters()

	if same1 != same0+1 {
		t.Errorf("failover_same_model_total %d -> %d, want +1: the replacement shares the key", same0, same1)
	}
	if cross1 != cross0 {
		t.Errorf("failover_cross_model_total %d -> %d, want unchanged: no key boundary was crossed", cross0, cross1)
	}
	if restores1 != restores0 {
		t.Errorf("checkpoint_restores_total %d -> %d, want unchanged: there was no checkpoint to restore", restores0, restores1)
	}
	if degraded1 != degraded0+1 {
		t.Errorf("checkpoint_degraded_total %d -> %d, want +1: the warm tier was attempted and fell through to replay", degraded0, degraded1)
	}
}

// The counterpart: a same-key replacement WITH a valid checkpoint counts
// on both axes — same-model, and the warm tier actually paid off. This is
// what worker-mock does, and the only place in the fleet it happens.
func TestSameKeyFailoverWithACheckpointCountsAsARestore(t *testing.T) {
	store := NewCheckpointStore()
	deps, _ := setup(t, "K1", 4, 10, store)
	b := router.NewWorker("worker-b", newFakeBackend(), "K1", mockCaps())
	deps.Router = router.New([]*router.Worker{b})

	same0, _, restores0, degraded0 := readCounters()
	if _, _, _, err := HandleBackendFailure(context.Background(), deps, errors.New("dead")); err != nil {
		t.Fatalf("HandleBackendFailure: %v", err)
	}
	same1, _, restores1, degraded1 := readCounters()

	if same1 != same0+1 {
		t.Errorf("failover_same_model_total %d -> %d, want +1", same0, same1)
	}
	if restores1 != restores0+1 {
		t.Errorf("checkpoint_restores_total %d -> %d, want +1: a valid checkpoint was restored", restores0, restores1)
	}
	if degraded1 != degraded0 {
		t.Errorf("checkpoint_degraded_total %d -> %d, want unchanged", degraded0, degraded1)
	}
}

// A different-key replacement is a cross-model failover and never touches
// the warm tier at all — build-plan.md's rule that state from one model is
// never deserialized into another.
func TestDifferentKeyFailoverCountsAsCrossModelAndSkipsTheWarmTier(t *testing.T) {
	store := NewCheckpointStore()
	deps, _ := setup(t, "K1", 4, 10, store) // a checkpoint EXISTS; it must simply never be considered
	c := router.NewWorker("worker-c", newFakeBackend(), "K2", mockCaps())
	deps.Router = router.New([]*router.Worker{c})

	same0, cross0, restores0, degraded0 := readCounters()
	if _, _, _, err := HandleBackendFailure(context.Background(), deps, errors.New("dead")); err != nil {
		t.Fatalf("HandleBackendFailure: %v", err)
	}
	same1, cross1, restores1, degraded1 := readCounters()

	if cross1 != cross0+1 {
		t.Errorf("failover_cross_model_total %d -> %d, want +1", cross0, cross1)
	}
	if same1 != same0 {
		t.Errorf("failover_same_model_total %d -> %d, want unchanged", same0, same1)
	}
	if restores1 != restores0 || degraded1 != degraded0 {
		t.Errorf("warm-tier counters moved (restores %d->%d, degraded %d->%d) — a cross-model failover must not even attempt a restore",
			restores0, restores1, degraded0, degraded1)
	}
}
