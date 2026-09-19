package coord

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"asr-stress-gym/internal/backend"
)

// fakeRecord mirrors worker/state.py's SessionRecord closely enough that
// fakeBackend's Push/Restore/Checkpoint behave like the real worker:
// idempotent replay, generation compare-and-commit, and a Checkpoint
// blob that genuinely round-trips through Restore rather than being an
// opaque no-op.
type fakeRecord struct {
	generation     uint64
	lastSeqApplied uint64
	lastText       string
}

// fakeBackend is a backend.Client that lives entirely in Go memory — no
// HTTP, no Python — fast enough to exercise the retry/exhaustion loop in
// HandleBackendFailure hundreds of times per test run. Its Checkpoint
// blob IS the accumulated text (not a real serialized model), which is
// enough to prove REPLAY CORRECTNESS end to end: if RecoverSameModel's
// tail replay is wrong, the accumulated text on the new handle will
// visibly not match what a correct implementation would produce.
type fakeBackend struct {
	mu      sync.Mutex
	records map[string]*fakeRecord
	nextID  int

	openErr            error
	restoreErr         error
	pushErr            error // returned by every Push from failAfterPushCount onward (0 = never)
	failAfterPushCount int
	pushCount          int

	openCalls    int
	restoreCalls int
	pushSpans    [][2]uint64 // (seqStart, seqEnd) of every successful push, in order
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{records: make(map[string]*fakeRecord)}
}

func (f *fakeBackend) Open(ctx context.Context, r backend.OpenReq) (backend.OpenResp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls++
	if f.openErr != nil {
		return backend.OpenResp{}, f.openErr
	}
	f.nextID++
	handle := fmt.Sprintf("fake-open-%d", f.nextID)
	f.records[handle] = &fakeRecord{}
	return backend.OpenResp{Handle: handle, Generation: 0}, nil
}

func (f *fakeBackend) Push(ctx context.Context, r backend.PushReq) (backend.PushResp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushCount++
	if f.pushErr != nil && (f.failAfterPushCount == 0 || f.pushCount >= f.failAfterPushCount) {
		return backend.PushResp{}, f.pushErr
	}
	rec, ok := f.records[r.Handle]
	if !ok {
		return backend.PushResp{}, errors.New("fake: unknown handle")
	}
	if r.SeqEnd <= rec.lastSeqApplied {
		return backend.PushResp{Text: rec.lastText, LastSeqApplied: rec.lastSeqApplied, Generation: rec.generation}, nil
	}
	if rec.generation != r.ExpectedGeneration {
		return backend.PushResp{}, backend.ErrStaleGeneration
	}
	rec.generation++
	rec.lastSeqApplied = r.SeqEnd
	rec.lastText += fmt.Sprintf("[%d-%d]", r.SeqStart, r.SeqEnd)
	f.pushSpans = append(f.pushSpans, [2]uint64{r.SeqStart, r.SeqEnd})
	return backend.PushResp{Text: rec.lastText, LastSeqApplied: rec.lastSeqApplied, Generation: rec.generation}, nil
}

func (f *fakeBackend) Flush(ctx context.Context, handle string) (backend.FlushResp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.records[handle]
	if !ok {
		return backend.FlushResp{}, errors.New("fake: unknown handle")
	}
	return backend.FlushResp{Text: rec.lastText, Final: true}, nil
}

func (f *fakeBackend) Restore(ctx context.Context, r backend.RestoreReq) (backend.RestoreResp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restoreCalls++
	if f.restoreErr != nil {
		return backend.RestoreResp{}, f.restoreErr
	}
	f.nextID++
	handle := fmt.Sprintf("fake-restored-%d", f.nextID)
	// "Deserialize": the blob IS the accumulated text (see Checkpoint), so
	// a genuine round trip continues it rather than starting fresh.
	f.records[handle] = &fakeRecord{generation: 0, lastSeqApplied: r.LastSeqApplied, lastText: string(r.CheckpointBlob)}
	return backend.RestoreResp{Handle: handle, Generation: 0, LastSeqApplied: r.LastSeqApplied}, nil
}

func (f *fakeBackend) Checkpoint(ctx context.Context, handle string) (backend.CheckpointResp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.records[handle]
	if !ok {
		return backend.CheckpointResp{}, errors.New("fake: unknown handle")
	}
	return backend.CheckpointResp{CheckpointBlob: []byte(rec.lastText), Generation: rec.generation, LastSeqApplied: rec.lastSeqApplied}, nil
}

func (f *fakeBackend) Close(ctx context.Context, handle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.records, handle)
	return nil
}

func (f *fakeBackend) Health(ctx context.Context) (backend.WorkerAdvert, error) {
	return backend.WorkerAdvert{}, nil
}

var _ backend.Client = (*fakeBackend)(nil)
