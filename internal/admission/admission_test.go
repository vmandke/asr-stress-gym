package admission

import (
	"sync"
	"testing"
	"time"

	"asr-stress-gym/internal/metrics"
)

func TestAdmitsUpToMaxSessionsThenRefuses(t *testing.T) {
	c := New(Config{MaxSessions: 3, SoftSessions: 2, RetryAfter: time.Second})

	for i := 0; i < 3; i++ {
		if d := c.Admit(); d.Refused {
			t.Fatalf("session %d refused below the ceiling: %+v", i, d)
		}
	}
	d := c.Admit()
	if !d.Refused {
		t.Fatal("4th session admitted past a ceiling of 3")
	}
	if d.RetryAfter != time.Second {
		t.Errorf("RetryAfter = %v, want 1s — a refusal must tell the client it is retryable", d.RetryAfter)
	}
	if d.Reason == "" {
		t.Error("a refusal with no reason is not actionable")
	}
}

func TestReleaseFreesCapacity(t *testing.T) {
	// Step 5 of the degradation order — "never drop an already-admitted
	// session" — only holds if admitted sessions reliably give their slot
	// back. A leak here makes the gateway refuse everyone, permanently,
	// after enough sessions have simply ended normally.
	c := New(Config{MaxSessions: 1, SoftSessions: 1})

	if c.Admit().Refused {
		t.Fatal("first session refused")
	}
	if !c.Admit().Refused {
		t.Fatal("second session admitted past a ceiling of 1")
	}
	c.Release()
	if c.Admit().Refused {
		t.Error("still refusing after the admitted session released its slot")
	}
}

func TestReleaseWithoutAdmitCannotDriveTheCountNegative(t *testing.T) {
	// A negative count would silently raise the effective ceiling, which
	// is the one failure mode a hard ceiling must not have.
	c := New(Config{MaxSessions: 2, SoftSessions: 1})
	c.Release()
	c.Release()
	if got := c.Admitted(); got != 0 {
		t.Fatalf("Admitted() = %d after unmatched Releases, want 0", got)
	}
	if c.Admit().Refused || c.Admit().Refused {
		t.Error("ceiling should still admit exactly 2")
	}
	if !c.Admit().Refused {
		t.Error("ceiling stopped applying after unmatched Releases")
	}
}

func TestConcurrentAdmitNeverExceedsTheCeiling(t *testing.T) {
	// The reason Admit uses compare-and-swap rather than load-then-add.
	// Under a ramp, hundreds of connections call this at the same instant;
	// a read-then-increment lets an unbounded number of them observe the
	// same under-limit value and all proceed. Run with -race.
	const ceiling = 50
	c := New(Config{MaxSessions: ceiling, SoftSessions: ceiling})

	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := 0
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !c.Admit().Refused {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if admitted != ceiling {
		t.Errorf("admitted %d concurrently, want exactly %d", admitted, ceiling)
	}
	if got := c.Admitted(); got != int64(ceiling) {
		t.Errorf("Admitted() = %d, want %d", got, ceiling)
	}
}

func TestRefusalIncrementsTheMetric(t *testing.T) {
	before := metrics.AdmissionRejectedTotal.Load()
	c := New(Config{MaxSessions: 1, SoftSessions: 1})
	c.Admit()
	c.Admit() // refused
	if got := metrics.AdmissionRejectedTotal.Load(); got != before+1 {
		t.Errorf("admission_rejected_total %d -> %d, want +1", before, got)
	}
}

func TestDegradedTracksTheSoftThreshold(t *testing.T) {
	c := New(Config{MaxSessions: 10, SoftSessions: 2})
	if c.Degraded() {
		t.Error("degraded with zero sessions admitted")
	}
	c.Admit()
	c.Admit()
	if c.Degraded() {
		t.Error("degraded AT the soft threshold; it should trigger above it")
	}
	c.Admit()
	if !c.Degraded() {
		t.Error("not degraded above the soft threshold")
	}
}

func TestChunkPolicyWidensOnlyWhenDegraded(t *testing.T) {
	// Step 2 of the degradation order: fewer, larger backend calls per
	// session buys headroom BEFORE step 4 has to refuse anyone.
	c := New(Config{MaxSessions: 10, SoftSessions: 1})
	if got := c.ChunkPolicy(160, 320); got != 160 {
		t.Errorf("healthy ChunkPolicy = %v, want the normal 160", got)
	}
	c.Admit()
	c.Admit() // now above the soft threshold
	if got := c.ChunkPolicy(160, 320); got != 320 {
		t.Errorf("degraded ChunkPolicy = %v, want the widened 320", got)
	}
}

func TestNewRepairsAnIncoherentConfig(t *testing.T) {
	// A soft threshold above the hard ceiling would mean step 2 never
	// fires and the first sign of trouble is a refusal — the opposite of
	// the intended order.
	c := New(Config{MaxSessions: 10, SoftSessions: 999})
	if c.cfg.SoftSessions >= c.cfg.MaxSessions {
		t.Errorf("SoftSessions = %d, not brought below MaxSessions = %d", c.cfg.SoftSessions, c.cfg.MaxSessions)
	}
	d := New(Config{})
	if d.cfg.MaxSessions != DefaultConfig().MaxSessions {
		t.Errorf("a zero Config did not fall back to defaults: %+v", d.cfg)
	}
}

func TestHighWaterRecordsThePeak(t *testing.T) {
	c := New(Config{MaxSessions: 10, SoftSessions: 8})
	c.Admit()
	c.Admit()
	c.Admit()
	c.Release()
	c.Release()
	if got := c.HighWater(); got != 3 {
		t.Errorf("HighWater() = %d, want 3 — the peak, not the current count", got)
	}
}
