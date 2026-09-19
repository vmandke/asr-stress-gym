package router

import (
	"context"
	"sync"
	"testing"
	"time"

	"asr-stress-gym/internal/backend"
	"asr-stress-gym/internal/session"
)

// nopClient is a backend.Client that satisfies the interface but is
// never actually called by these tests — Router only needs a Worker's
// static identity and health state, never its Client, to make a
// decision.
type nopClient struct{}

func (nopClient) Open(context.Context, backend.OpenReq) (backend.OpenResp, error) {
	return backend.OpenResp{}, nil
}
func (nopClient) Push(context.Context, backend.PushReq) (backend.PushResp, error) {
	return backend.PushResp{}, nil
}
func (nopClient) Flush(context.Context, string) (backend.FlushResp, error) {
	return backend.FlushResp{}, nil
}
func (nopClient) Restore(context.Context, backend.RestoreReq) (backend.RestoreResp, error) {
	return backend.RestoreResp{}, nil
}
func (nopClient) Checkpoint(context.Context, string) (backend.CheckpointResp, error) {
	return backend.CheckpointResp{}, nil
}
func (nopClient) Close(context.Context, string) error { return nil }
func (nopClient) Health(context.Context) (backend.WorkerAdvert, error) {
	return backend.WorkerAdvert{}, nil
}

func streamingCaps() backend.Capabilities {
	return backend.Capabilities{Streaming: true, Serializable: true, Modes: []string{"online", "offline"}}
}

func offlineOnlyCaps() backend.Capabilities {
	return backend.Capabilities{Streaming: false, Serializable: false, Modes: []string{"offline"}}
}

func newTestWorker(id string, key session.CacheCompatibilityKey, caps backend.Capabilities) *Worker {
	return NewWorker(id, nopClient{}, key, caps)
}

func TestPickPrefersCompatibleKey(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	b := newTestWorker("b", "K2", streamingCaps())
	r := New([]*Worker{a, b})

	got, err := r.Pick(session.ModeOnline, nil, "K2")
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.ID != "b" {
		t.Fatalf("got %s, want b (matches prefer=K2)", got.ID)
	}
}

func TestPickPrefersLeastOutstanding(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	b := newTestWorker("b", "K1", streamingCaps())
	a.BindSession()
	a.BindSession()
	a.BindSession() // a has 3 outstanding, b has 0 — same key, so prefer doesn't break the tie
	r := New([]*Worker{a, b})

	got, err := r.Pick(session.ModeOnline, nil, "")
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.ID != "b" {
		t.Fatalf("got %s, want b (fewer outstanding)", got.ID)
	}
}

func TestPickExcludesGivenWorkers(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	b := newTestWorker("b", "K1", streamingCaps())
	r := New([]*Worker{a, b})

	got, err := r.Pick(session.ModeOnline, map[string]bool{"a": true}, "")
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.ID != "b" {
		t.Fatalf("got %s, want b (a excluded)", got.ID)
	}
}

func TestPickFiltersNonStreamingForOnlineMode(t *testing.T) {
	offline := newTestWorker("offline-only", "K1", offlineOnlyCaps())
	r := New([]*Worker{offline})

	if _, err := r.Pick(session.ModeOnline, nil, ""); err != ErrNoCapacity {
		t.Fatalf("got %v, want ErrNoCapacity (non-streaming worker must be filtered for online mode)", err)
	}
	got, err := r.Pick(session.ModeOffline, nil, "")
	if err != nil {
		t.Fatalf("Pick(offline): %v", err)
	}
	if got.ID != "offline-only" {
		t.Fatalf("got %s, want offline-only (offline mode doesn't need streaming)", got.ID)
	}
}

func TestPickReturnsErrNoCapacityWhenEmpty(t *testing.T) {
	r := New(nil)
	if _, err := r.Pick(session.ModeOnline, nil, ""); err != ErrNoCapacity {
		t.Fatalf("got %v, want ErrNoCapacity", err)
	}
}

// build-plan.md's own gray-failure motivating example: a backend that
// errors on more than half its recent calls must be ejected outright.
func TestReportEjectsOnHighErrorRate(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	r := New([]*Worker{a})

	for i := 0; i < 10; i++ {
		r.Report("a", false, 10*time.Millisecond)
	}
	if got := a.Status(); got != Ejected {
		t.Fatalf("status = %v, want Ejected after 10/10 failures", got)
	}
	if _, err := r.Pick(session.ModeOnline, nil, ""); err != ErrNoCapacity {
		t.Fatalf("Pick after ejection: got %v, want ErrNoCapacity (only worker is ejected, timer not expired)", err)
	}
}

func TestReportDegradesOnModerateErrorRate(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	r := New([]*Worker{a})

	// 2/10 = 20% error rate: > 0.1 (Degraded threshold), <= 0.5 (Eject threshold).
	for i := 0; i < 8; i++ {
		r.Report("a", true, time.Millisecond)
	}
	for i := 0; i < 2; i++ {
		r.Report("a", false, time.Millisecond)
	}
	if got := a.Status(); got != Degraded {
		t.Fatalf("status = %v, want Degraded", got)
	}
	// Degraded is still usable — build-plan.md's filter only excludes Ejected.
	if _, err := r.Pick(session.ModeOnline, nil, ""); err != nil {
		t.Fatalf("Pick on Degraded worker: %v, want success", err)
	}
}

// The gray-failure case build-plan.md calls out by name: "The failure
// that defeats naive health checks is the backend that is slow but
// alive." A worker with zero errors but latency far above its peers must
// still be ejected.
func TestReportEjectsOnGrayFailure(t *testing.T) {
	fast := newTestWorker("fast", "K1", streamingCaps())
	slow := newTestWorker("slow", "K1", streamingCaps())
	r := New([]*Worker{fast, slow})

	for i := 0; i < 20; i++ {
		r.Report("fast", true, 10*time.Millisecond)
	}
	// Establish slow's OWN baseline first at a still-reasonable latency so
	// it doesn't eject on the very first sample (clusterP95 needs >0
	// samples from OTHER workers to compare against, and eject requires
	// myP95 > 3x clusterP95).
	for i := 0; i < 20; i++ {
		r.Report("slow", true, 500*time.Millisecond) // 50x fast's latency
	}
	if got := slow.Status(); got != Ejected {
		t.Fatalf("slow worker status = %v, want Ejected (gray failure: alive, but >>3x cluster p95)", got)
	}
	if got := fast.Status(); got != Healthy {
		t.Fatalf("fast worker status = %v, want Healthy", got)
	}
}

// Ejection is temporary: after the timer, exactly one probe is allowed,
// and a successful probe restores Healthy.
func TestProbeRecoversOnSuccess(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	a.backoff = 10 * time.Millisecond // shrink the timer so the test doesn't sleep for real seconds
	r := New([]*Worker{a})

	for i := 0; i < 10; i++ {
		r.Report("a", false, time.Millisecond)
	}
	if got := a.Status(); got != Ejected {
		t.Fatalf("status = %v, want Ejected", got)
	}

	time.Sleep(15 * time.Millisecond) // past the shrunk ejectAt

	got, err := r.Pick(session.ModeOnline, nil, "")
	if err != nil {
		t.Fatalf("Pick during probe window: %v, want the ejected worker selected as the probe", err)
	}
	if got.ID != "a" {
		t.Fatalf("got %s, want a", got.ID)
	}

	r.Report("a", true, time.Millisecond) // the probe succeeds
	if got := a.Status(); got != Healthy {
		t.Fatalf("status after successful probe = %v, want Healthy", got)
	}
}

func TestProbeReEjectsWithBackoffOnFailure(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	a.backoff = 10 * time.Millisecond
	r := New([]*Worker{a})

	for i := 0; i < 10; i++ {
		r.Report("a", false, time.Millisecond)
	}
	time.Sleep(15 * time.Millisecond)
	if _, err := r.Pick(session.ModeOnline, nil, ""); err != nil {
		t.Fatalf("Pick during probe window: %v", err)
	}

	r.Report("a", false, time.Millisecond) // the probe ALSO fails
	a.mu.Lock()
	gotBackoff := a.backoff
	gotStatus := a.status
	a.mu.Unlock()
	if gotStatus != Ejected {
		t.Fatalf("status after failed probe = %v, want still Ejected", gotStatus)
	}
	if gotBackoff != 20*time.Millisecond {
		t.Fatalf("backoff after failed probe = %v, want doubled to 20ms", gotBackoff)
	}
}

// The bug this test exists to catch: Pick evaluating (not necessarily
// choosing) MULTIPLE ejected-and-expired candidates in one call must not
// strand the non-chosen ones in a permanent probing=true state that no
// Report() will ever resolve, since no session is ever opened against
// them.
func TestNonChosenProbeCandidateIsNotStranded(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	b := newTestWorker("b", "K1", streamingCaps())
	a.backoff, b.backoff = 10*time.Millisecond, 10*time.Millisecond
	r := New([]*Worker{a, b})

	for i := 0; i < 10; i++ {
		r.Report("a", false, time.Millisecond)
		r.Report("b", false, time.Millisecond)
	}
	time.Sleep(15 * time.Millisecond)

	first, err := r.Pick(session.ModeOnline, nil, "")
	if err != nil {
		t.Fatalf("first Pick: %v", err)
	}
	var loser *Worker
	if first.ID == "a" {
		loser = b
	} else {
		loser = a
	}
	loser.mu.Lock()
	stranded := loser.probing
	loser.mu.Unlock()
	if stranded {
		t.Fatalf("%s was only CONSIDERED (not chosen) but ended up probing=true — it will never be resolved", loser.ID)
	}

	// Confirm the loser is still genuinely eject-and-expired (not somehow
	// promoted to eligible-forever): it should be pickable on ITS OWN,
	// excluding the winner.
	second, err := r.Pick(session.ModeOnline, map[string]bool{first.ID: true}, "")
	if err != nil {
		t.Fatalf("second Pick (excluding winner): %v", err)
	}
	if second.ID != loser.ID {
		t.Fatalf("got %s, want %s", second.ID, loser.ID)
	}
}

// Concurrency: many goroutines racing Pick/Report on a shared Router must
// never produce more than one outstanding probe per worker, and must
// never panic/deadlock. Run with -race.
func TestConcurrentPickAndReportIsRaceFree(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	b := newTestWorker("b", "K2", streamingCaps())
	r := New([]*Worker{a, b})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, err := r.Pick(session.ModeOnline, nil, session.CacheCompatibilityKey("K1"))
			if err != nil {
				return
			}
			w.BindSession()
			r.Report(w.ID, i%3 != 0, time.Duration(i%5)*time.Millisecond)
			w.UnbindSession()
		}(i)
	}
	wg.Wait()
}

// Found by a chaos test's client-side retry loop landing on the SAME
// worker ten times in a row against a real, long-running gateway
// process — not by inspection. Every candidate ties at construction (no
// Outstanding, no latency samples yet, no prefer key on a first-ever
// open), and a plain `for range` over r.workers resolves every tie to
// whichever worker happens to be first in that slice — fixed once at
// buildRouter's construction (from a map, whose iteration order
// randomizes only across separate program runs, never across separate
// calls within one). Every session from one gateway process would pile
// onto the same single worker until load differentiated them.
func TestPickDistributesAcrossTiedCandidates(t *testing.T) {
	workers := []*Worker{
		newTestWorker("a", "K1", streamingCaps()),
		newTestWorker("b", "K1", streamingCaps()),
		newTestWorker("c", "K1", streamingCaps()),
	}
	r := New(workers)

	seen := map[string]bool{}
	const attempts = 200 // P(missing a fair 3-way split entirely) is astronomically small; this only guards against the old "always first" bug
	for i := 0; i < attempts; i++ {
		w, err := r.Pick(session.ModeOnline, nil, "")
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		seen[w.ID] = true
	}
	if len(seen) < 2 {
		t.Fatalf("Pick returned only %v across %d calls with all workers tied — ties are not being distributed", seen, attempts)
	}
}
