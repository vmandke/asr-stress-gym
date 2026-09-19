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
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"asr-stress-gym/internal/backend"
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

// Worker is the router's view of one backend: static identity (ID,
// Client, CompatibilityKey, Capabilities — set once at construction) plus
// live health/load state guarded by mu.
type Worker struct {
	ID               string
	Client           backend.Client
	CompatibilityKey session.CacheCompatibilityKey
	Capabilities     backend.Capabilities

	outstanding atomic.Int64 // sessions currently pinned here — BindSession/UnbindSession

	mu        sync.Mutex
	outcomes  []bool
	latencies []time.Duration
	status    Status
	ejectAt   time.Time // Ejected until this passes, then eligible for exactly one probe
	probing   bool      // a probe selection is currently outstanding; don't hand out a second
	backoff   time.Duration
}

func NewWorker(id string, client backend.Client, key session.CacheCompatibilityKey, caps backend.Capabilities) *Worker {
	return &Worker{ID: id, Client: client, CompatibilityKey: key, Capabilities: caps, status: Healthy, backoff: ejectDuration}
}

func (w *Worker) Status() Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status
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
	switch w.status {
	case Healthy, Degraded:
		return true
	case Ejected:
		return !w.probing && now.After(w.ejectAt)
	default:
		return false
	}
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
	case clusterP95 > 0 && myP95 > 3*clusterP95:
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
	workers []*Worker
}

func New(workers []*Worker) *Router {
	return &Router{workers: workers}
}

func (r *Router) Workers() []*Worker { return r.workers }

// Find looks up a worker by ID — used by internal/coord to unbind a
// session from the worker it's failing away from.
func (r *Router) Find(id string) (*Worker, bool) {
	for _, w := range r.workers {
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
func (r *Router) clusterP95() time.Duration {
	var all []time.Duration
	for _, w := range r.workers {
		w.mu.Lock()
		all = append(all, w.latencies...)
		w.mu.Unlock()
	}
	return p95(all)
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
	var best *Worker
	bestScore := math.MaxFloat64

	for _, idx := range rand.Perm(len(r.workers)) {
		w := r.workers[idx]
		if exclude != nil && exclude[w.ID] {
			continue
		}
		if !w.eligible(now) {
			continue
		}
		if mode == session.ModeOnline && !w.Capabilities.Streaming {
			continue
		}
		if !modeSupported(w.Capabilities.Modes, mode) {
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
	if best == nil {
		return nil, ErrNoCapacity
	}
	best.beginProbe() // no-op unless best was actually Ejected-and-eligible; see its doc comment
	return best, nil
}

// Report feeds back the outcome of one backend interaction for worker id
// — the only way health state changes (Evaluate is reactive to observed
// traffic, not a background poller).
func (r *Router) Report(id string, ok bool, latency time.Duration) {
	cp95 := r.clusterP95()
	now := time.Now()
	for _, w := range r.workers {
		if w.ID == id {
			w.report(ok, latency, cp95, now)
			return
		}
	}
}
