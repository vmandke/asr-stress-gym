// Package router selects and pins a worker per session: filter (health,
// capability match for mode) then score (least-outstanding, compatibility-
// key preference) — docs/build-plan.md "Selection", extended by
// docs/implementation-plan.md "Capability-aware routing". A library
// inside the gateway process, not a service — see build-plan.md "Hop 3":
// one Router instance is constructed once in cmd/gateway/main.go and
// shared (by reference) across every connection's sessionLoop.
//
// Health is entirely REACTIVE, not polled: build-plan.md's own Evaluate
// is driven by observed outcomes (Report), so there is no background
// health-check goroutine here. A worker's identity (CompatibilityKey,
// Capabilities) is learned once via Health() at startup
// (cmd/gateway/main.go) and never changes for that worker's lifetime in
// this process.
package router

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"asr-stress-gym/internal/backend"
	"asr-stress-gym/internal/metrics"
	"asr-stress-gym/internal/session"
)

// ErrNoCapacity means the filter stage emptied: no usable, capability-
// matching worker exists right now. The caller's response is `overloaded`
// (M7) or a session-terminal error (M3), never a silent downgrade to a
// backend that cannot serve the traffic — implementation-plan.md
// "Capability-aware routing".
var ErrNoCapacity = errors.New("router: no usable worker for this request")

type Status int

const (
	Healthy Status = iota
	Degraded
	Ejected
)

func (s Status) String() string {
	switch s {
	case Healthy:
		return "healthy"
	case Degraded:
		return "degraded"
	case Ejected:
		return "ejected"
	default:
		return "unknown"
	}
}

const (
	// windowSize bounds both the error-rate and latency-p95 rolling
	// windows — enough samples for a meaningful signal without unbounded
	// growth over a long-lived worker. A tuning knob, not a derived
	// constant; reported rather than defended, same as the VAD thresholds
	// elsewhere in this project.
	windowSize = 50

	// ejectDuration is build-plan.md's own number: "after ~10s move to
	// Probing". maxEjectBackoff bounds the doubling on repeated probe
	// failure ("re-eject with a longer timer on failure") so a
	// persistently broken worker is retried occasionally, not never.
	ejectDuration   = 10 * time.Second
	maxEjectBackoff = 2 * time.Minute
)

// GrayFailureMultiple is how far above the cluster p95 a worker's own p95
// may sit before it is ejected as a gray failure — "alive, but far slower
// than its peers". Named and exported so the dashboard can show the rule
// it is applying rather than just its verdict.
const GrayFailureMultiple = 3.0

// minLatencySamples is how many calls a worker must have served before
// the gray-failure rule is allowed to judge it.
//
// Without this, the rule ejects healthy workers on their first slow call.
// p95 here is nearest-rank over a rolling window, so at n samples it is
// `sorted[int(0.95n)]` — and for any n below 20 that index IS n-1, the
// maximum. "p95 over 7 samples" is not a p95 at all; it is the slowest
// call the worker has ever served.
//
// Observed live: worker-c ejected at 4.7x with **7 samples** and a p95 of
// 39.8ms, while its steady-state latency was comparable to its peers.
// The 39.8ms was its first inference, which is slow on every real adapter
// — weights page in, the first decode warms caches. The rule then ejected
// it for being cold, which stopped it receiving traffic, which froze its
// window at that same 39.8ms so the ratio could never improve on its own.
//
// 20 is the smallest window where the nearest-rank p95 stops being the
// maximum, which is exactly the property that makes the comparison mean
// anything.
const minLatencySamples = 20

// Worker is the router's view of one backend: static identity (ID,
// Client, CompatibilityKey, Capabilities — set once at construction) plus
// live health/load state guarded by mu.
type Worker struct {
	ID               string
	Client           backend.Client
	CompatibilityKey session.CacheCompatibilityKey
	Capabilities     backend.Capabilities
	// Model is display-only — the human-readable name the worker
	// advertises (zipformer-en-20M, whisper-small...). Nothing in
	// selection reads it: two workers are same-model because they share a
	// compatibility key derived from their weights and runtime, never
	// because they share this string. Kept so the dashboard can say WHAT
	// is being served without a second lookup.
	Model string

	// KVTierURL is the shared KV tier this worker publishes session state
	// to, learned from its /health advertisement rather than configured
	// here. Workers sharing a compatibility key share a tier, so this is
	// effectively a property of the pool. Empty means the worker serves
	// only the pinned path. Nothing in selection reads it — like Model,
	// it is carried so the caller need not make a second lookup.
	KVTierURL string

	outstanding atomic.Int64 // sessions currently pinned here — BindSession/UnbindSession

	// Bucket is the rate-limit budget from build-plan.md's "Selection"
	// filter stage, which M3 explicitly deferred. Never nil for a worker
	// built by NewWorker, but nil-safe throughout so a hand-constructed
	// Worker in a test is simply unlimited rather than unusable.
	Bucket *Bucket

	mu        sync.Mutex
	outcomes  []bool
	latencies []time.Duration
	status    Status
	ejectAt   time.Time // Ejected until this passes, then eligible for exactly one probe
	probing   bool      // a probe selection is currently outstanding; don't hand out a second
	backoff   time.Duration
	draining  bool // operator-initiated: no NEW sessions, existing ones untouched
}

func NewWorker(id string, client backend.Client, key session.CacheCompatibilityKey, caps backend.Capabilities) *Worker {
	return &Worker{
		ID: id, Client: client, CompatibilityKey: key, Capabilities: caps,
		status: Healthy, backoff: ejectDuration,
		Bucket: NewBucket(defaultBucketLimit, defaultHeadroom),
	}
}

func (w *Worker) Status() Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status
}

// SetDraining is graceful drain: stop accepting NEW sessions, leave every
// in-flight one exactly as it is. This is deploy behaviour, and it is
// deliberately NOT a health state — a draining worker is not unhealthy, it
// has not failed, and it must not be ejected, probed, backed off, or
// counted as an error by any of the machinery in this file. It is simply
// not a candidate.
//
// Modeling it as a separate flag rather than a fourth Status value is the
// whole point: Status transitions are driven by observed outcomes and
// recover on their own timers, so representing an operator decision as one
// would mean a successful probe silently un-drains a worker somebody is
// trying to take out of service.
//
// Note what this does NOT do: it does not migrate the sessions already
// pinned here. Those keep streaming against this worker until they end
// naturally — which is what "graceful" means, and is also why a drain
// followed by a kill is a genuinely different experiment from a kill
// alone. The first shows a clean handover, the second shows recovery.
func (w *Worker) SetDraining(on bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.draining = on
}

func (w *Worker) Draining() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.draining
}

func (w *Worker) BindSession()   { w.outstanding.Add(1) }
func (w *Worker) UnbindSession() { w.outstanding.Add(-1) }
func (w *Worker) Outstanding() int64 {
	return w.outstanding.Load()
}

// eligible is a READ-ONLY check: is this worker even a candidate for
// Pick's scoring pass. Deliberately side-effect-free — Pick may evaluate
// several Ejected-but-timer-expired workers in one filter pass, and only
// ONE of them ends up chosen; committing "this worker is now the probe"
// here, for every candidate merely CONSIDERED, would strand the
// not-chosen ones in probing=true forever (no session is ever opened
// against them, so no Report() ever arrives to resolve it). See beginProbe.
func (w *Worker) eligible(now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.draining {
		return false // operator took it out of rotation; see SetDraining
	}
	switch w.status {
	case Healthy, Degraded:
		return true
	case Ejected:
		return !w.probing && now.After(w.ejectAt)
	default:
		return false
	}
}

// dueForProbe reports that this worker is ejected, its backoff has
// expired, and no trial is already outstanding — the half-open state of a
// circuit breaker.
//
// Separate from eligible() because the two answer different questions, and
// conflating them cost the fleet its ability to heal. eligible() asks "may
// this worker be considered?", and Pick then scored every candidate on
// (Outstanding+1) * latencyP95 and probed only the winner. But an ejected
// worker's p95 is FROZEN at whatever got it ejected — it receives no
// traffic, so no new sample can ever arrive — while a healthy peer's is
// small. The ejected worker therefore loses every comparison forever:
//
//	ejected ctc-1, 0 sessions   (0+1) * 0.664 = 0.664
//	healthy zip-2, 5 sessions   (5+1) * 0.019 = 0.114
//
// Observed live: a worker whose process had been restored and was
// answering /health in 5s sat ejected at a frozen 664ms while one peer
// carried the entire fleet's sessions. Recovery was gated on winning a
// competition that ejection made unwinnable.
func (w *Worker) dueForProbe(now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.draining {
		return false
	}
	return w.status == Ejected && !w.probing && now.After(w.ejectAt)
}

// beginProbe commits an Ejected-but-eligible worker to being THE probe,
// once Pick has actually chosen it (not merely considered it). A no-op
// for a Healthy/Degraded worker.
func (w *Worker) beginProbe() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == Ejected {
		w.probing = true
	}
}

func (w *Worker) errorRate() float64 {
	if len(w.outcomes) == 0 {
		return 0
	}
	fails := 0
	for _, ok := range w.outcomes {
		if !ok {
			fails++
		}
	}
	return float64(fails) / float64(len(w.outcomes))
}

func p95(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), d...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)) * 0.95)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (w *Worker) latencyP95() time.Duration {
	return p95(w.latencies)
}

// Stats exposes what the ejection rule actually compares, for the
// dashboard (internal/dash). "worker-c is ejected" is not an answer a
// reviewer can act on; "its p95 is 41ms against a cluster p95 of 10ms,
// and the threshold is 3x" is. Read-only, and computed from the same
// fields report() uses, so the explanation cannot drift from the decision.
func (w *Worker) Stats() (errorRate float64, latencyP95 time.Duration, samples int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.errorRate(), p95(w.latencies), len(w.latencies)
}

// ClusterP95 is the baseline every worker's latency is judged against.
//
// Worth stating plainly, because it is load-bearing and surprising: this
// pools raw SAMPLES across the fleet, not per-worker p95s. A worker
// serving four times the traffic contributes four times the samples and
// dominates the baseline — and since Pick sends traffic to whoever is
// fastest, the fastest worker both sets the standard and is measured
// against its own. On a fleet containing the mock adapter (~1ms, no real
// inference) alongside real ones (10-40ms), that skews the baseline low
// enough to eject healthy real workers. See docs/DASHBOARD.md.
func (r *Router) ClusterP95() time.Duration { return r.clusterP95() }

// eject must be called with mu held.
func (w *Worker) eject(now time.Time) {
	w.status = Ejected
	w.ejectAt = now.Add(w.backoff)
	w.probing = false
}

// report feeds one outcome into this worker's rolling windows and
// re-evaluates status against clusterP95 — build-plan.md "Health: errors
// and latency". If this worker was the outstanding probe, the outcome
// resolves it directly (success -> Healthy, backoff reset; failure ->
// re-ejected with the backoff doubled, capped) rather than going through
// the general Evaluate formula.
func (w *Worker) report(ok bool, latency time.Duration, clusterP95 time.Duration, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.status == Ejected && w.probing {
		w.probing = false
		if ok {
			w.status = Healthy
			w.backoff = ejectDuration
			w.outcomes = nil
			w.latencies = nil
		} else {
			w.backoff *= 2
			if w.backoff > maxEjectBackoff {
				w.backoff = maxEjectBackoff
			}
			w.ejectAt = now.Add(w.backoff)
			w.status = Ejected // stays ejected; ejectAt/backoff already advanced
		}
		return
	}
	if w.status == Ejected {
		return // not currently probing and timer hasn't expired: a stray report (shouldn't normally happen) changes nothing
	}

	w.outcomes = append(w.outcomes, ok)
	if len(w.outcomes) > windowSize {
		w.outcomes = w.outcomes[1:]
	}
	if latency > 0 {
		w.latencies = append(w.latencies, latency)
		if len(w.latencies) > windowSize {
			w.latencies = w.latencies[1:]
		}
	}

	errRate := w.errorRate()
	myP95 := p95(w.latencies)
	switch {
	case errRate > 0.5:
		w.eject(now)
	case len(w.latencies) >= minLatencySamples && clusterP95 > 0 &&
		float64(myP95) > GrayFailureMultiple*float64(clusterP95):
		w.eject(now) // gray failure: alive, but far slower than its peers
	case errRate > 0.1:
		w.status = Degraded
	default:
		w.status = Healthy
	}
}

// Router holds a fixed set of Workers, constructed once at gateway
// startup (build-plan.md: workers are addressed directly, never load-
// balanced — see "Hop 4"). Safe for concurrent use: every connection's
// sessionLoop calls Pick/Report on the same shared instance.
type Router struct {
	mu      sync.RWMutex
	workers []*Worker
}

func New(workers []*Worker) *Router {
	return &Router{workers: workers}
}

// Workers returns a snapshot, so callers can inspect the fleet while a
// control plane adds a newly verified worker. The Worker values themselves
// remain shared: their health and load fields have their own synchronization.
func (r *Router) Workers() []*Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]*Worker(nil), r.workers...)
}

// Add makes a health-verified worker eligible for new sessions. Identity is
// immutable for a worker lifetime, so replacing an existing ID is forbidden:
// a silent replacement could join a different cache-compatibility cohort.
func (r *Router) Add(worker *Worker) error {
	if worker == nil || worker.ID == "" {
		return errors.New("router: worker must have an ID")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.workers {
		if existing.ID == worker.ID {
			return fmt.Errorf("router: worker %q already exists", worker.ID)
		}
	}
	r.workers = append(r.workers, worker)
	return nil
}

// Find looks up a worker by ID — used by internal/coord to unbind a
// session from the worker it's failing away from.
// Pool returns the workers that share `id`'s compatibility key, `id`
// first and the rest in stable order. That set IS the homogeneous pool:
// every member can serve any other member's request, and — crucially —
// can import any other member's KV cache, because cache compatibility is
// exactly what the key encodes.
//
// Derived from the keys the workers advertised at startup, never from
// configuration. A pool written into a config file would be a second,
// weaker statement of the same fact, free to drift the moment a worker's
// dtype or cache schema changed; this one cannot, because it IS the fact.
//
// Used to build the Bifrost fallback chain for a stateless final
// (cmd/gateway/conn.go), so that chain can never cross a model family.
func (r *Router) Pool(id string) []*Worker {
	self, ok := r.Find(id)
	if !ok {
		return nil
	}
	pool := []*Worker{self}
	for _, w := range r.Workers() {
		if w.ID != id && w.CompatibilityKey == self.CompatibilityKey {
			pool = append(pool, w)
		}
	}
	return pool
}

func (r *Router) Find(id string) (*Worker, bool) {
	for _, w := range r.Workers() {
		if w.ID == id {
			return w, true
		}
	}
	return nil, false
}

// clusterP95 pools ALL workers' recent latency samples into one set —
// build-plan.md: "Compare against the cluster p95, not an absolute
// threshold — an absolute number goes stale the moment you change model
// or hardware."
// clusterP95 is the baseline a worker's own p95 is compared against: the
// MEDIAN OF PER-WORKER p95s, over workers that have served enough calls to
// have a meaningful one.
//
// It used to pool every worker's raw samples into one list and take the
// p95 of that, which is wrong in two directions at once, because sample
// count is proportional to traffic and traffic is assigned by speed:
//
//   - The fastest worker contributes the most samples, so it dominates the
//     baseline and effectively competes against itself. On this fleet the
//     mock adapter (~7ms, no real inference) set a baseline that ejected
//     real workers for being real.
//   - Symmetrically, a genuinely slow worker that has accumulated enough
//     samples drags the baseline UP to its own level and becomes
//     un-ejectable — the exact failure the rule exists to catch.
//
// A median over per-worker p95s asks the question the rule actually means:
// "is this worker far slower than a typical worker", where every worker
// counts once regardless of how much traffic it happens to be getting.
//
// Returns 0 — which disables gray-failure ejection entirely — when fewer
// than two workers qualify. You cannot call something an outlier without
// peers to compare it to, and a single-worker fleet has no peers.
// peerP95 is the baseline `subject` is judged against: the median p95 of
// the other workers RUNNING THE SAME MODEL, over those that have served
// enough calls for their p95 to mean anything.
//
// Four deliberate choices, each fixing a way this was wrong. The original
// pooled every worker's raw samples fleet-wide and took the p95 of that,
// which breaks in several directions at once, because sample count is
// proportional to traffic and traffic is assigned by speed:
//
//   - **Same-model peers only.** This is the big one. A fleet of
//     heterogeneous adapters has no single meaningful latency standard:
//     the mock adapter answers in ~8ms because it does no inference,
//     zipformer takes ~40ms because it does. Judging them against one
//     another ejects real workers for being real — observed live, with
//     worker-a at 41ms and worker-c at 34ms both ejected against an 8ms
//     mock-set baseline while reporting a 0.00 error rate. Two workers
//     are comparable exactly when they share a compatibility key, which
//     is the same notion of "same model" that decides whether a
//     checkpoint can be restored (internal/coord). Reusing it here keeps
//     one definition of sameness in the system instead of two.
//   - **Per-worker, not pooled.** The busiest worker contributed the most
//     samples and so dominated the baseline, effectively competing
//     against itself.
//   - **Excluding the subject.** Symmetrically, a genuinely slow worker
//     with enough samples dragged the baseline up to its own level and
//     became un-ejectable — the exact failure the rule exists to catch. A
//     worker must never be part of the standard it is held to.
//   - **Median, not mean.** One pathological peer should not move the bar.
//
// Returns 0 — which disables gray-failure ejection for this worker — when
// no same-model peer qualifies. That is the honest answer, not a
// limitation to work around: with one worker per model there is nothing
// to be an outlier *from*, and on this fleet it means the rule is live
// only for the worker-a/worker-b pair. Error-rate ejection is unaffected
// and still covers every worker.
func (r *Router) peerP95(subject *Worker) time.Duration {
	var each []time.Duration
	for _, w := range r.Workers() {
		if w.ID == subject.ID {
			continue
		}
		// Only workers running the SAME model are peers. See the comment
		// above for why heterogeneous comparison is meaningless here.
		if w.CompatibilityKey != subject.CompatibilityKey {
			continue
		}
		w.mu.Lock()
		if len(w.latencies) >= minLatencySamples {
			each = append(each, p95(w.latencies))
		}
		w.mu.Unlock()
	}
	if len(each) == 0 {
		return 0
	}
	sort.Slice(each, func(i, j int) bool { return each[i] < each[j] })
	return each[len(each)/2]
}

// clusterP95 is the fleet-wide version, for display only (the dashboard
// shows "the baseline" as one number). Selection never uses it — each
// worker is judged against peerP95 of the others.
func (r *Router) clusterP95() time.Duration {
	var each []time.Duration
	for _, w := range r.Workers() {
		w.mu.Lock()
		if len(w.latencies) >= minLatencySamples {
			each = append(each, p95(w.latencies))
		}
		w.mu.Unlock()
	}
	if len(each) == 0 {
		return 0
	}
	sort.Slice(each, func(i, j int) bool { return each[i] < each[j] })
	return each[len(each)/2]
}

func modeSupported(modes []string, mode session.Mode) bool {
	for _, m := range modes {
		if session.Mode(m) == mode {
			return true
		}
	}
	return false
}

// Pick selects a worker: FILTER (usable health, capability match for
// mode — implementation-plan.md's "Capability-aware routing"; rate-limit
// budget and memory headroom are explicitly out of scope until M7/M5)
// then SCORE (least-outstanding × latency, halved when the candidate
// shares prefer's compatibility key — "Cache-aware routing, expressed as
// one argument," build-plan.md). Called on session open and on failover
// only; the caller is responsible for pinning (BindSession) once it has
// actually committed to the result.
//
// Iterates candidates in a randomized order each call. Without this, a
// tie (every fresh worker starts at an identical score — the common case
// for the very first session, before Outstanding/latency differ at all)
// always resolved to whichever worker happened to be first in r.workers
// — fixed once, at construction, for the whole process's lifetime, since
// map iteration order (buildRouter builds from a map) only re-randomizes
// across separate program runs, not separate calls within one. Every
// session opened by a given gateway process would have piled onto the
// SAME single worker until load differentiated them — a real
// load-balancing bug, found by a chaos test's retry loop landing on the
// identical worker ten times in a row, not by inspection.
func (r *Router) Pick(mode session.Mode, exclude map[string]bool, prefer session.CacheCompatibilityKey) (*Worker, error) {
	now := time.Now()
	workers := r.Workers()
	var best *Worker
	// A worker whose ejection backoff has expired and which is owed one
	// trial request. Held aside from the scoring competition entirely —
	// see dueForProbe for why scoring it can only ever lose.
	var probe *Worker
	bestScore := math.MaxFloat64

	for _, idx := range rand.Perm(len(workers)) {
		w := workers[idx]
		if exclude != nil && exclude[w.ID] {
			continue
		}
		if !w.eligible(now) {
			continue
		}
		// Rate-limit budget: the filter-stage constraint build-plan.md
		// lists and M3 deferred. Read-only here — the winner is charged
		// below, so considering a worker never costs it anything.
		if !w.Bucket.Available() {
			continue
		}
		if mode == session.ModeOnline && !w.Capabilities.Streaming {
			continue
		}
		if !modeSupported(w.Capabilities.Modes, mode) {
			continue
		}

		// Held out of the scoring pass, not scored and rejected: its p95
		// is frozen at the value that ejected it, so any comparison is
		// decided by stale data the worker has no way to refresh.
		if w.dueForProbe(now) {
			if probe == nil {
				probe = w
			}
			continue
		}

		w.mu.Lock()
		latSec := w.latencyP95().Seconds()
		w.mu.Unlock()
		if latSec <= 0 {
			latSec = 0.001 // an unobserved-but-healthy worker must still be scorable, not multiply out to a literal 0 that would starve the Outstanding signal
		}

		// (Outstanding + 1), not Outstanding alone: build-plan.md's own
		// literal formula (`Outstanding * latencyP95Sec`) scores every
		// worker at exactly 0 whenever Outstanding is 0 for all of them —
		// the common case for the very first session, before any load
		// exists — which makes the `prefer` halving below multiply 0 by
		// 0.5 and stay 0, silently erasing the compatibility-key
		// preference in exactly the case a fresh session most needs it.
		// Caught by TestPickPrefersCompatibleKey, not by inspection.
		score := float64(w.Outstanding()+1) * latSec
		if prefer != "" && w.CompatibilityKey == prefer {
			score *= 0.5
		}
		if score < bestScore {
			best, bestScore = w, score
		}
	}
	// The half-open trial, taken in preference to the scored winner.
	//
	// Bounded, and that is what makes it safe to prefer: beginProbe sets
	// probing, which makes this worker ineligible until the outcome
	// resolves it, so exactly ONE session at a time is exposed to a
	// recovering worker. If the trial fails, coord fails that session over
	// and the backoff doubles (to a 2-minute cap); if it succeeds, report
	// clears the stale latency window so the worker rejoins scoring with
	// no memory of the sickness. Without this the ejection is permanent —
	// see dueForProbe.
	if probe != nil && probe.Bucket.Take() {
		probe.beginProbe()
		return probe, nil
	}

	if best == nil {
		return nil, ErrNoCapacity
	}
	// Charge the winner only. If its budget evaporated in the window
	// between the filter pass and here — another session picking the same
	// worker concurrently — report no capacity rather than handing out a
	// worker we have just been told to stop using. The caller's retry
	// will pick again against fresh state.
	if !best.Bucket.Take() {
		return nil, ErrNoCapacity
	}
	// No beginProbe here any more: an ejected worker can no longer reach
	// the scoring pass at all (it is diverted above), so `best` is always
	// Healthy or Degraded and the call could only ever be a no-op.
	return best, nil
}

// On429 applies a worker's own rate-limit refusal to its bucket. Called
// by whoever received the 429 — the point is that the gateway stops
// choosing this worker, immediately, rather than sleeping on the online
// path (build-plan.md: "Move to another backend").
func (r *Router) On429(id string, retryAfter time.Duration) {
	if w, ok := r.Find(id); ok {
		w.Bucket.On429(retryAfter)
		metrics.Backend429Total.Add(1)
	}
}

// Report feeds back the outcome of one backend interaction for worker id
// — the only way health state changes (Evaluate is reactive to observed
// traffic, not a background poller).
func (r *Router) Report(id string, ok bool, latency time.Duration) {
	// The baseline EXCLUDES this worker — see peerP95. A worker must not
	// be part of the standard it is judged against.
	now := time.Now()
	for _, w := range r.Workers() {
		if w.ID == id {
			w.report(ok, latency, r.peerP95(w), now)
			return
		}
	}
}
