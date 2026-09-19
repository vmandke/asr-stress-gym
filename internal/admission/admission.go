// Package admission implements steps 4 and 5 of docs/build-plan.md's
// degradation order:
//
//  1. Route new sessions to a less loaded worker
//  2. Increase chunk size (fewer, larger calls per session)
//  3. Drop to a cheaper, faster model
//  4. Reject NEW sessions with `overloaded` (retryable)
//  5. Never drop an already-admitted session
//
// Steps 1 and 3 already live in internal/router — scoring by
// least-outstanding is step 1, and the compatibility-key preference plus
// capability filter is where a "cheaper model" choice would be expressed.
// Step 2 is internal/audio's business, and is wired through ChunkPolicy
// below rather than implemented here, because nothing outside that
// package is allowed to reason about audio durations.
//
// **The asymmetry between 4 and 5 is the entire design.** Admission is
// checked exactly once, when a session starts. After that the session is
// admitted and nothing in this package can touch it. Refusing a new
// caller beats cutting someone off mid-sentence, and the only way to
// guarantee that is to have no mechanism capable of doing the latter.
//
// Queueing is deliberately absent. build-plan.md: "Queueing converts a
// fast failure into a slow one. A transcript delivered eight seconds late
// is worthless." A refused session is refused now, not parked.
package admission

import (
	"sync"
	"sync/atomic"
	"time"

	"asr-stress-gym/internal/metrics"
)

// Decision is what Admit returns. A zero Decision means admitted.
type Decision struct {
	Refused    bool
	Reason     string
	RetryAfter time.Duration
}

// Config holds the capacity envelope. Both numbers are tuning knobs,
// reported rather than defended (docs/build-plan.md's own convention for
// the VAD thresholds and the router's windows).
type Config struct {
	// MaxSessions is the hard ceiling on concurrently admitted sessions
	// across the whole gateway. Beyond it, new sessions are refused
	// regardless of what the fleet looks like — this is the backstop that
	// keeps the gateway's own memory bounded (invariant 16) when every
	// worker still claims to be healthy.
	MaxSessions int64

	// SoftSessions is where degradation BEGINS rather than where it ends:
	// above it the gateway widens chunks (step 2) to cut per-session call
	// volume, buying headroom before it has to refuse anyone. Below it,
	// nothing degrades.
	SoftSessions int64

	// RetryAfter is what a refused client is told to wait. Advisory.
	RetryAfter time.Duration
}

func DefaultConfig() Config {
	// 200 is docs/DECISIONS.md's stated concurrency target on the mock
	// adapter; the soft threshold sits at 75% of it so step 2 has room to
	// work before step 4 has to fire. Both are overridable from the
	// gateway's env (cmd/gateway) so a capacity run can move them without
	// a rebuild.
	return Config{MaxSessions: 200, SoftSessions: 150, RetryAfter: time.Second}
}

// Controller tracks admitted sessions. Safe for concurrent use: every
// connection's sessionLoop shares one instance.
type Controller struct {
	cfg      Config
	admitted atomic.Int64

	mu        sync.Mutex
	highWater int64
}

func New(cfg Config) *Controller {
	if cfg.MaxSessions <= 0 {
		cfg = DefaultConfig()
	}
	if cfg.SoftSessions <= 0 || cfg.SoftSessions > cfg.MaxSessions {
		cfg.SoftSessions = cfg.MaxSessions * 3 / 4
	}
	return &Controller{cfg: cfg}
}

// Admit decides whether a NEW session may start, and reserves a slot if
// so. Every Admit that returns an admitted Decision must be paired with
// exactly one Release.
//
// Uses compare-and-swap rather than "load, compare, add": under a ramp
// hundreds of connections call this at once, and a read-then-increment
// would let an unbounded number of them observe the same under-limit
// value and all proceed. The ceiling would then be advisory, which is the
// one thing a hard ceiling must not be.
func (c *Controller) Admit() Decision {
	for {
		cur := c.admitted.Load()
		if cur >= c.cfg.MaxSessions {
			metrics.AdmissionRejectedTotal.Add(1)
			return Decision{
				Refused:    true,
				Reason:     "gateway at session capacity",
				RetryAfter: c.cfg.RetryAfter,
			}
		}
		if c.admitted.CompareAndSwap(cur, cur+1) {
			c.noteHighWater(cur + 1)
			return Decision{}
		}
	}
}

// Release frees an admitted session's slot. Idempotent per session only
// by the caller's discipline — call it once, from the same place that
// owns the session's lifetime.
func (c *Controller) Release() {
	if c.admitted.Add(-1) < 0 {
		// Only reachable via a Release without a matching Admit, which is
		// a coordinator bug. Clamp rather than let it go negative, or the
		// ceiling silently stops applying.
		c.admitted.Store(0)
	}
}

func (c *Controller) Admitted() int64 { return c.admitted.Load() }

func (c *Controller) noteHighWater(n int64) {
	c.mu.Lock()
	if n > c.highWater {
		c.highWater = n
	}
	c.mu.Unlock()
}

func (c *Controller) HighWater() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.highWater
}

// Degraded reports whether the gateway is above its soft threshold and
// should start economising per session — step 2 of the degradation order.
func (c *Controller) Degraded() bool {
	return c.admitted.Load() > c.cfg.SoftSessions
}

// ChunkPolicy is step 2 expressed as the one number internal/audio needs.
// It returns the chunk duration a NEW session should use: the normal
// policy when healthy, a wider one when degraded.
//
// Wider chunks mean fewer backend calls for the same audio, trading
// partial-update latency for throughput — which is the correct trade at
// the point where the alternative is refusing the session outright.
// Applied at session start only: changing an established session's chunk
// size mid-utterance would change what the streaming encoder sees, and
// build-plan.md is explicit that chunk boundaries are part of the state a
// replay has to reproduce.
func (c *Controller) ChunkPolicy(normalMs, degradedMs float64) float64 {
	if c.Degraded() {
		return degradedMs
	}
	return normalMs
}
