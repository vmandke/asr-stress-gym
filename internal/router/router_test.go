package router

import (
	"context"
	"errors"
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

// The probe has to survive COMPETITION, which TestProbeRecoversOnSuccess
// cannot show because its fleet has one worker and the ejected one wins by
// default.
//
// An ejected worker's latencyP95 is frozen at whatever ejected it: no
// traffic reaches it, so no new sample can arrive. Scoring it against a
// healthy peer therefore compares live data with a fossil, and the fossil
// always loses — even against a peer carrying the entire fleet:
//
//	ejected, 0 sessions   (0+1) * 0.664 = 0.664
//	healthy, 5 sessions   (5+1) * 0.019 = 0.114
//
// Observed live before the fix: a worker whose process had been restored
// and was answering /health within 5s stayed ejected at a frozen 664ms
// while one peer carried every session. Recovery was gated on winning a
// competition that ejection made unwinnable.
func TestAnEjectedWorkerIsProbedEvenWhenAHealthyPeerScoresFarBetter(t *testing.T) {
	slow := newTestWorker("slow", "K1", streamingCaps())
	fast := newTestWorker("fast", "K1", streamingCaps())
	r := New([]*Worker{slow, fast})

	// Past minLatencySamples, and far enough apart to trip the gray-failure
	// multiple — the real ejection path, not a synthetic status poke.
	for i := 0; i < 25; i++ {
		r.Report("fast", true, 19*time.Millisecond)
		r.Report("slow", true, 664*time.Millisecond)
	}
	if got := slow.Status(); got != Ejected {
		t.Fatalf("slow status = %v, want Ejected", got)
	}
	if got := fast.Status(); got != Healthy {
		t.Fatalf("fast status = %v, want Healthy", got)
	}

	// The healthy peer is carrying the whole fleet and STILL scores better
	// than the ejected worker's frozen p95. This is the configuration the
	// old code could never escape.
	for i := 0; i < 5; i++ {
		fast.BindSession()
	}

	slow.mu.Lock()
	slow.ejectAt = time.Now().Add(-time.Millisecond) // backoff has expired
	slow.mu.Unlock()

	got, err := r.Pick(session.ModeOnline, nil, "")
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.ID != "slow" {
		t.Fatalf("Pick chose %s; the ejected worker is owed a trial and can never\n"+
			"win on score, so it would stay ejected forever and its p95 would stay\n"+
			"frozen at the value that ejected it", got.ID)
	}

	// Exactly one session is exposed to a recovering worker: beginProbe
	// marks it probing, which makes it ineligible until the outcome
	// resolves. Otherwise a sick worker would take the whole ramp.
	next, err := r.Pick(session.ModeOnline, nil, "")
	if err != nil {
		t.Fatalf("second Pick: %v", err)
	}
	if next.ID != "fast" {
		t.Fatalf("second Pick chose %s, want fast — only ONE trial may be outstanding", next.ID)
	}

	r.Report("slow", true, 20*time.Millisecond) // the trial succeeds
	if got := slow.Status(); got != Healthy {
		t.Fatalf("status after a successful trial = %v, want Healthy", got)
	}
	// And it must rejoin scoring with no memory of the sickness, or the
	// gray-failure check would eject it again on the next evaluation.
	slow.mu.Lock()
	n := len(slow.latencies)
	slow.mu.Unlock()
	if n > 1 {
		t.Errorf("latency window kept %d stale samples after recovery; the frozen\n"+
			"p95 would re-eject it immediately", n)
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

func TestMarkUnhealthyImmediatelyRemovesADeadWorkerFromSelection(t *testing.T) {
	w := newTestWorker("worker-a", "K1", streamingCaps())
	r := New([]*Worker{w})
	r.MarkUnhealthy(w.ID)
	if got := w.Status(); got != Ejected {
		t.Fatalf("status = %v, want Ejected after direct health failure", got)
	}
	if _, err := r.Pick(session.ModeOnline, nil, ""); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("Pick error = %v, want ErrNoCapacity for dead worker", err)
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

// --- graceful drain (M9) ------------------------------------------------

func TestDrainingWorkerIsNotPicked(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	b := newTestWorker("b", "K1", streamingCaps())
	r := New([]*Worker{a, b})

	a.SetDraining(true)
	for i := 0; i < 20; i++ {
		got, err := r.Pick(session.ModeOnline, nil, "")
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		if got.ID == "a" {
			t.Fatal("picked a draining worker")
		}
	}

	a.SetDraining(false)
	var sawA bool
	for i := 0; i < 50 && !sawA; i++ {
		got, _ := r.Pick(session.ModeOnline, nil, "")
		sawA = got.ID == "a"
	}
	if !sawA {
		t.Error("worker never returned to rotation after drain was lifted")
	}
}

// Draining is an operator decision, not a health signal. If it were
// modelled as a Status, the probe/recovery machinery would eventually
// un-drain a worker somebody is deliberately taking out of service.
func TestDrainingIsNotAHealthStateAndSurvivesSuccessfulCalls(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	a.SetDraining(true)

	if got := a.Status(); got != Healthy {
		t.Errorf("status = %v, want Healthy — draining must not mark a worker unhealthy", got)
	}
	for i := 0; i < 50; i++ {
		a.report(true, 5*time.Millisecond, 5*time.Millisecond, time.Now())
	}
	if !a.Draining() {
		t.Error("successful calls cleared the drain flag")
	}
}

// A drain must leave in-flight sessions alone — that is what makes it
// graceful, and what makes drain-then-kill a different experiment from
// kill alone.
func TestDrainDoesNotDisturbBoundSessions(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	a.BindSession()
	a.BindSession()
	a.SetDraining(true)
	if got := a.Outstanding(); got != 2 {
		t.Errorf("outstanding = %d, want 2", got)
	}
}

func TestDrainingEveryWorkerYieldsNoCapacity(t *testing.T) {
	a := newTestWorker("a", "K1", streamingCaps())
	r := New([]*Worker{a})
	a.SetDraining(true)
	if _, err := r.Pick(session.ModeOnline, nil, ""); !errors.Is(err, ErrNoCapacity) {
		t.Errorf("err = %v, want ErrNoCapacity", err)
	}
}

// --- gray-failure sample floor (M9) -------------------------------------

// The bug: p95 here is nearest-rank over a rolling window, so below 20
// samples `sorted[int(0.95n)]` IS `sorted[n-1]` — the maximum. A worker's
// first inference is slow on every real adapter (weights page in, caches
// warm), so the rule ejected healthy workers for being cold, which stopped
// their traffic, which froze the window at that same slow sample.
//
// Observed live: worker-c ejected at 4.7x on 7 samples with p95 39.8ms.
func TestGrayFailureIgnoresWorkersWithTooFewSamples(t *testing.T) {
	slow := newTestWorker("slow", "K1", streamingCaps())
	fast := newTestWorker("fast", "K1", streamingCaps())
	r := New([]*Worker{slow, fast})

	// Give the fleet a fast baseline.
	for i := 0; i < 50; i++ {
		r.Report("fast", true, 5*time.Millisecond)
	}
	// One very slow call on the other worker — its cold first inference.
	r.Report("slow", true, 500*time.Millisecond)

	if got := slow.Status(); got == Ejected {
		t.Fatalf("worker ejected on %d sample(s); the rule must not judge a worker "+
			"before it has %d", 1, minLatencySamples)
	}
}

// ...but a worker that is genuinely, persistently slow still gets ejected
// once there is enough evidence. The floor delays the judgement; it must
// not remove it.
func TestGrayFailureStillEjectsAPersistentlySlowWorker(t *testing.T) {
	slow := newTestWorker("slow", "K1", streamingCaps())
	fast := newTestWorker("fast", "K1", streamingCaps())
	r := New([]*Worker{slow, fast})

	for i := 0; i < 50; i++ {
		r.Report("fast", true, 5*time.Millisecond)
	}
	for i := 0; i < minLatencySamples+5; i++ {
		r.Report("slow", true, 500*time.Millisecond)
	}

	if got := slow.Status(); got != Ejected {
		t.Errorf("status = %v, want Ejected — %d slow samples is enough evidence",
			got, minLatencySamples+5)
	}
}

// The sample floor must not weaken error-rate ejection, which needs no
// such protection: a failure is a failure on the first call.
func TestErrorRateEjectionIsUnaffectedByTheSampleFloor(t *testing.T) {
	w := newTestWorker("bad", "K1", streamingCaps())
	r := New([]*Worker{w})
	for i := 0; i < 10; i++ {
		r.Report("bad", false, 0)
	}
	if got := w.Status(); got != Ejected {
		t.Errorf("status = %v, want Ejected on a 100%% error rate", got)
	}
}

// A fleet of different models has no single latency standard: the mock
// adapter answers in ~8ms because it does no inference, a real one takes
// ~40ms because it does. Comparing them ejects real workers for being
// real — observed live, with worker-a (41ms) and worker-c (34ms) both
// ejected against an 8ms mock-set baseline at a 0.00 error rate.
func TestGrayFailureOnlyComparesWorkersRunningTheSameModel(t *testing.T) {
	mock := newTestWorker("mock", "K-MOCK", streamingCaps())    // no real inference
	real1 := newTestWorker("real-1", "K-REAL", streamingCaps()) // a real adapter
	real2 := newTestWorker("real-2", "K-REAL", streamingCaps())
	r := New([]*Worker{mock, real1, real2})

	for i := 0; i < 50; i++ {
		r.Report("mock", true, 8*time.Millisecond)
		r.Report("real-1", true, 40*time.Millisecond)
		r.Report("real-2", true, 40*time.Millisecond)
	}

	if got := real1.Status(); got == Ejected {
		t.Errorf("real-1 status = %v: a real adapter was ejected for being slower than a mock", got)
	}
	if got := real2.Status(); got == Ejected {
		t.Errorf("real-2 status = %v: a real adapter was ejected for being slower than a mock", got)
	}
}

// ...and a worker that is slow RELATIVE TO ITS OWN MODEL still gets
// caught. That is the signal the rule exists for.
func TestGrayFailureEjectsAWorkerSlowerThanItsSameModelPeers(t *testing.T) {
	fast1 := newTestWorker("fast-1", "K-REAL", streamingCaps())
	fast2 := newTestWorker("fast-2", "K-REAL", streamingCaps())
	slow := newTestWorker("slow", "K-REAL", streamingCaps())
	r := New([]*Worker{fast1, fast2, slow})

	for i := 0; i < 50; i++ {
		r.Report("fast-1", true, 40*time.Millisecond)
		r.Report("fast-2", true, 40*time.Millisecond)
		r.Report("slow", true, 400*time.Millisecond)
	}

	if got := slow.Status(); got != Ejected {
		t.Errorf("status = %v, want Ejected — 10x its same-model peers", got)
	}
}

// With one worker per model there is nothing to be an outlier from, so
// the rule is simply off. That is the honest answer, not a gap: a lone
// worker has no peer whose latency could establish what "normal" is.
// Error-rate ejection still covers it.
func TestGrayFailureIsDisabledForAWorkerWithNoSameModelPeer(t *testing.T) {
	lonely := newTestWorker("lonely", "K-ALONE", streamingCaps())
	other := newTestWorker("other", "K-OTHER", streamingCaps())
	r := New([]*Worker{lonely, other})

	for i := 0; i < 50; i++ {
		r.Report("other", true, 5*time.Millisecond)
		r.Report("lonely", true, 900*time.Millisecond) // wildly slower than the fleet
	}
	if got := lonely.Status(); got == Ejected {
		t.Errorf("status = %v: ejected with no same-model peer to be judged against", got)
	}

	// But errors still eject it.
	for i := 0; i < 50; i++ {
		r.Report("lonely", false, 0)
	}
	if got := lonely.Status(); got != Ejected {
		t.Errorf("status = %v, want Ejected on errors regardless of peers", got)
	}
}

// --- pools (M11): the Bifrost fallback chain's source of truth ----------

// A pool is the set of workers that share a compatibility key. That is the
// same test that decides whether a KV cache can be imported, so a chain
// built from it can never cross a model family — by construction, not by
// convention.
func TestPoolIsTheSameKeyCohortWithSelfFirst(t *testing.T) {
	zip1 := newTestWorker("zip-1", "K-ZIP", streamingCaps())
	zip2 := newTestWorker("zip-2", "K-ZIP", streamingCaps())
	ctc1 := newTestWorker("ctc-1", "K-CTC", streamingCaps())
	r := New([]*Worker{zip1, ctc1, zip2})

	pool := r.Pool("zip-1")
	if len(pool) != 2 {
		t.Fatalf("pool = %d workers, want 2", len(pool))
	}
	if pool[0].ID != "zip-1" {
		t.Errorf("pool[0] = %q, want the worker itself first (it is the Bifrost primary)", pool[0].ID)
	}
	for _, w := range pool {
		if w.CompatibilityKey != "K-ZIP" {
			t.Errorf("pool contains %q with key %q — a fallback chain must never cross a model family",
				w.ID, w.CompatibilityKey)
		}
	}
}

func TestPoolOfALoneWorkerIsJustItself(t *testing.T) {
	lone := newTestWorker("lone", "K-ONLY", streamingCaps())
	other := newTestWorker("other", "K-OTHER", streamingCaps())
	r := New([]*Worker{lone, other})

	pool := r.Pool("lone")
	if len(pool) != 1 || pool[0].ID != "lone" {
		t.Errorf("pool = %v, want just the worker itself — with no twin there is nowhere safe to fall back to", pool)
	}
}

func TestPoolOfAnUnknownWorkerIsEmpty(t *testing.T) {
	r := New([]*Worker{newTestWorker("a", "K", streamingCaps())})
	if got := r.Pool("gone"); got != nil {
		t.Errorf("Pool(unknown) = %v, want nil so the caller falls back to its configured default", got)
	}
}

// Draining and health are deliberately NOT filtered here: a pool is an
// identity statement ("these are interchangeable"), not an availability
// one. Bifrost does its own health tracking and retry across the chain, so
// excluding a temporarily-unhealthy peer would remove the very fallback
// the chain exists to provide.
func TestPoolIgnoresHealthAndDraining(t *testing.T) {
	a := newTestWorker("a", "K", streamingCaps())
	b := newTestWorker("b", "K", streamingCaps())
	r := New([]*Worker{a, b})
	b.SetDraining(true)
	for i := 0; i < 50; i++ {
		r.Report("b", false, 0) // eject it
	}
	if got := len(r.Pool("a")); got != 2 {
		t.Errorf("pool = %d, want 2 — membership is identity, not availability", got)
	}
}
