package router

import (
	"sync"
	"testing"
	"time"

	"asr-stress-gym/internal/session"
)

func TestBucketRefusesBeforeReachingZero(t *testing.T) {
	// The headroom IS the feature: build-plan.md's "discovering a limit by
	// receiving a 429 means you already failed a request." A bucket that
	// only refused at zero would hand out every last token and let the
	// worker do the refusing, which is the behaviour this exists to avoid.
	b := NewBucket(100, 0.2)
	now := time.Now()

	taken := 0
	for b.takeAt(now) { // frozen clock: no refill, so this drains deterministically
		taken++
		if taken > 200 {
			t.Fatal("bucket never refused — headroom is not being applied")
		}
	}
	if taken != 80 {
		t.Errorf("took %d tokens before refusing, want 80 (100 limit, 20%% headroom)", taken)
	}
	b.mu.Lock()
	remaining := b.tokens
	b.mu.Unlock()
	if remaining < 100*0.2 {
		t.Errorf("bucket drained to %v, below the %v headroom it is supposed to protect", remaining, 100*0.2)
	}
}

func TestBucketRefillsOverTime(t *testing.T) {
	b := NewBucket(100, 0.2)
	start := time.Now()
	for b.takeAt(start) {
	}
	if b.takeAt(start) {
		t.Fatal("expected the bucket to be refusing at the headroom floor")
	}
	// One second at limit=100/s refills the whole bucket.
	if !b.takeAt(start.Add(time.Second)) {
		t.Error("bucket did not refill after a second")
	}
}

func TestBucketRefillIsCappedAtTheLimit(t *testing.T) {
	// Without the cap, an idle worker banks tokens indefinitely and the
	// first burst after a quiet period ignores the limit entirely.
	b := NewBucket(100, 0.2)
	start := time.Now()
	b.availableAt(start.Add(time.Hour)) // a long idle period, then refill

	taken := 0
	at := start.Add(time.Hour)
	for b.takeAt(at) {
		taken++
		if taken > 200 {
			t.Fatal("bucket banked unbounded tokens while idle")
		}
	}
	if taken != 80 {
		t.Errorf("took %d tokens after an hour idle, want 80 — refill must cap at the limit", taken)
	}
}

func TestOn429ZeroesTheBucketAndBlocksForRetryAfter(t *testing.T) {
	b := NewBucket(100, 0.2)
	now := time.Now()
	if !b.availableAt(now) {
		t.Fatal("fresh bucket should be available")
	}

	b.on429At(now, 2*time.Second)

	if b.availableAt(now) {
		t.Error("bucket is available immediately after a 429 — Retry-After is not being honoured")
	}
	// Still blocked before Retry-After elapses, even though refill alone
	// would have restored plenty of tokens by then.
	if b.availableAt(now.Add(1500 * time.Millisecond)) {
		t.Error("bucket unblocked early — refill must not override blockedUntil")
	}
	if !b.availableAt(now.Add(2100 * time.Millisecond)) {
		t.Error("bucket still blocked after Retry-After elapsed")
	}
}

func TestNilBucketIsUnlimited(t *testing.T) {
	// A Worker constructed by hand in a test has no bucket; it must be
	// unlimited rather than unusable, or every such test silently starts
	// exercising ErrNoCapacity instead of what it meant to.
	var b *Bucket
	if !b.Available() || !b.Take() {
		t.Error("nil bucket must allow everything")
	}
	b.On429(time.Second) // must not panic
	if b.Blocked() {
		t.Error("nil bucket must never report blocked")
	}
}

func TestAvailableDoesNotSpend(t *testing.T) {
	// Pick calls Available on every candidate and Take on exactly one. If
	// Available spent, a fleet of N workers would burn N tokens per
	// selection and the gateway would refuse traffic it never sent.
	b := NewBucket(100, 0.2)
	now := time.Now()
	for i := 0; i < 50; i++ {
		if !b.availableAt(now) {
			t.Fatalf("Available returned false on call %d — it is spending tokens", i)
		}
	}
	b.mu.Lock()
	tokens := b.tokens
	b.mu.Unlock()
	if tokens != 100 {
		t.Errorf("after 50 Available calls the bucket holds %v tokens, want 100 untouched", tokens)
	}
}

func TestBucketIsSafeUnderConcurrentTake(t *testing.T) {
	// Every connection's sessionLoop shares one Router and therefore one
	// bucket per worker. Run with -race.
	b := NewBucket(1000, 0.2)
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 40; j++ {
				if b.Take() {
					mu.Lock()
					granted++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if granted == 0 {
		t.Fatal("no request was granted at all")
	}
	// 2000 attempts against a 1000-token bucket with 20% headroom: the
	// exact number depends on how much wall time the goroutines took to
	// run (refill is real-time here), so assert the ceiling rather than an
	// exact count.
	if granted > 2000 {
		t.Errorf("granted %d, more than the attempts made — accounting is broken", granted)
	}
}

func TestPickSkipsRateLimitedWorkers(t *testing.T) {
	limited := newTestWorker("worker-limited", "K1", streamingCaps())
	healthy := newTestWorker("worker-healthy", "K1", streamingCaps())
	limited.Bucket.On429(time.Minute)

	r := New([]*Worker{limited, healthy})
	for i := 0; i < 20; i++ {
		got, err := r.Pick(session.ModeOnline, nil, "")
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		if got.ID == "worker-limited" {
			t.Fatal("Pick chose a worker inside its Retry-After window")
		}
	}
}

func TestPickReturnsNoCapacityWhenEveryWorkerIsRateLimited(t *testing.T) {
	// The whole fleet out of budget is backpressure, and it must surface
	// as ErrNoCapacity so cmd/gateway can answer `overloaded` (retryable)
	// rather than a terminal error.
	a := newTestWorker("worker-a", "K1", streamingCaps())
	b := newTestWorker("worker-b", "K1", streamingCaps())
	a.Bucket.On429(time.Minute)
	b.Bucket.On429(time.Minute)

	r := New([]*Worker{a, b})
	if _, err := r.Pick(session.ModeOnline, nil, ""); err != ErrNoCapacity {
		t.Fatalf("got %v, want ErrNoCapacity", err)
	}
}
