package router

import (
	"sync"
	"time"
)

// Bucket is docs/build-plan.md's rate-limit bucket, one per worker.
//
// The headroom is the entire point of it: "discovering a limit by
// receiving a 429 means you already failed a request." Allow() stops
// handing out capacity at `limit*headroom` remaining rather than at zero,
// so the gateway backs off before the worker has to refuse anyone.
//
// What this is NOT: a client-side mirror of the worker's own limit, kept
// in sync. It is a local estimate that a real 429 corrects. On429 zeroes
// the bucket and blocks for exactly the Retry-After the worker asked for,
// because the worker's opinion of its own capacity always beats the
// gateway's guess.
//
// build-plan.md also flags the division problem — N gateways each sized
// to the full limit overshoot by N×. With one gateway that does not bite
// yet; the note lives in docs/DECISIONS.md so the HA profile (M9) does
// not quietly reintroduce it.
type Bucket struct {
	mu           sync.Mutex
	limit        float64 // tokens per second, and the ceiling the bucket refills to
	tokens       float64
	headroom     float64 // stop at this fraction of the limit still unspent
	blockedUntil time.Time
	lastRefill   time.Time
}

const (
	// defaultBucketLimit is generous relative to what one session costs:
	// a stream at 20ms frames and a 200ms chunk policy pushes ~5 times a
	// second, so 200/s is roughly 40 concurrent streams per worker — the
	// same order as build-plan.md's own capacity arithmetic ("one worker
	// sustains ~40 concurrent live streams"). It is a tuning knob,
	// reported rather than defended.
	defaultBucketLimit = 200.0

	// defaultHeadroom is build-plan.md's own 0.2: refuse locally once
	// only 20% of the limit remains.
	defaultHeadroom = 0.2
)

func NewBucket(limit, headroom float64) *Bucket {
	return &Bucket{limit: limit, tokens: limit, headroom: headroom, lastRefill: time.Now()}
}

// refill must be called with mu held.
func (b *Bucket) refill(now time.Time) {
	if b.lastRefill.IsZero() {
		b.lastRefill = now
		return
	}
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.lastRefill = now
	b.tokens += elapsed * b.limit
	if b.tokens > b.limit {
		b.tokens = b.limit
	}
}

// Available is a READ-ONLY check: does this worker have budget right now.
// Side-effect-free on purpose, for the same reason Worker.eligible is —
// Pick evaluates every candidate but commits to one, and charging a token
// to each worker merely CONSIDERED would drain the whole fleet's budget
// N times faster than the fleet is actually being used, at which point
// the gateway refuses traffic it never sent. Spend with Take, on the
// winner only.
//
// A nil Bucket always allows: a worker constructed without rate limiting
// is not rate limited, rather than being silently unusable.
func (b *Bucket) Available() bool {
	if b == nil {
		return true
	}
	return b.availableAt(time.Now())
}

func (b *Bucket) availableAt(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(now)
	return !now.Before(b.blockedUntil) && b.tokens > b.limit*b.headroom
}

// Take spends one token, committing the caller to this worker. Returns
// false if the budget went away between Available and here — a real race
// with other sessions picking concurrently, not a theoretical one.
func (b *Bucket) Take() bool {
	if b == nil {
		return true
	}
	return b.takeAt(time.Now())
}

func (b *Bucket) takeAt(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(now)
	if now.Before(b.blockedUntil) || b.tokens <= b.limit*b.headroom {
		return false
	}
	b.tokens--
	return true
}

// On429 applies a worker's own refusal: zero the bucket and stay out
// until Retry-After elapses. Deliberately harsher than decrementing —
// the worker has just told us our estimate was wrong, and the useful
// response to that is to stop guessing for a while.
func (b *Bucket) On429(retryAfter time.Duration) {
	if b == nil {
		return
	}
	b.on429At(time.Now(), retryAfter)
}

func (b *Bucket) on429At(now time.Time, retryAfter time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens = 0
	b.lastRefill = now
	b.blockedUntil = now.Add(retryAfter)
}

// Blocked reports whether the bucket is currently inside a Retry-After
// window. For diagnostics and tests; Allow already accounts for it.
func (b *Bucket) Blocked() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Now().Before(b.blockedUntil)
}
