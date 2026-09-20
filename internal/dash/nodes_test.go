package dash

import (
	"context"
	"testing"
	"time"
)

func i64(v int64) *int64 { return &v }

func TestNilSamplerIsSafe(t *testing.T) {
	var n *NodeSampler
	n.Run(context.Background())
	if n.Latest() != nil || n.Series() != nil || n.Order() != nil {
		t.Error("nil sampler returned non-nil readings")
	}
}

// A dead worker must record an explicit not-ok sample. If a failed probe
// simply skipped the node, its line would stop and look identical to a
// node that is merely idle — and watching a node go down is the point.
func TestFailedProbesAreRecordedNotSkipped(t *testing.T) {
	var ok bool
	s := NewNodeSampler(func(ctx context.Context) []NodeSample {
		ok = !ok
		return []NodeSample{{Node: "worker-a", AtMs: nowMs(), OK: ok, Detail: "connection refused"}}
	})
	s.sample(context.Background())
	s.sample(context.Background())

	series := s.Series()["worker-a"]
	if len(series) != 2 {
		t.Fatalf("recorded %d samples, want 2", len(series))
	}
	if series[0].OK == series[1].OK {
		t.Error("both samples have the same OK value; the failing probe was not distinguished")
	}
}

func TestGatewayIsSampledAlongsideTheWorkers(t *testing.T) {
	s := NewNodeSampler(func(ctx context.Context) []NodeSample {
		return []NodeSample{{Node: "worker-a", OK: true}}
	})
	s.sample(context.Background())

	order := s.Order()
	if len(order) != 2 || order[0] != "gateway" {
		t.Fatalf("order = %v, want gateway first then worker-a", order)
	}
	var gw *NodeSample
	for i, n := range s.Latest() {
		if n.Node == "gateway" {
			gw = &s.Latest()[i]
		}
	}
	if gw == nil {
		t.Fatal("no gateway sample")
	}
	if gw.Goroutines == 0 || gw.HeapBytes == nil {
		t.Errorf("gateway sample missing runtime stats: %+v", gw)
	}
}

// The percentage is computed server-side, once, so every consumer of the
// API gets the same null handling rather than re-implementing it.
func TestMemoryPercentOfLimitIsDerived(t *testing.T) {
	s := NewNodeSampler(func(ctx context.Context) []NodeSample {
		return []NodeSample{
			{Node: "with-limit", OK: true, MemBytes: i64(768 << 20), MemLimit: i64(1536 << 20)},
			{Node: "no-limit", OK: true, MemBytes: i64(768 << 20)},
		}
	})
	s.sample(context.Background())

	got := map[string]*float64{}
	for _, n := range s.Latest() {
		got[n.Node] = n.MemPctLimit
	}
	if got["with-limit"] == nil || *got["with-limit"] != 50 {
		t.Errorf("with-limit pct = %v, want 50", got["with-limit"])
	}
	// No limit means no percentage — not 0%, and not a bar drawn against
	// an invented ceiling.
	if got["no-limit"] != nil {
		t.Errorf("no-limit pct = %v, want nil", got["no-limit"])
	}
}

func TestSeriesIsBounded(t *testing.T) {
	s := NewNodeSampler(func(ctx context.Context) []NodeSample {
		return []NodeSample{{Node: "worker-a", OK: true}}
	})
	for i := 0; i < nodeSeriesCapacity+50; i++ {
		s.sample(context.Background())
	}
	if got := len(s.Series()["worker-a"]); got != nodeSeriesCapacity {
		t.Errorf("series length = %d, want %d", got, nodeSeriesCapacity)
	}
}

// The poller must not be able to hang on a blackholed worker: the fault
// suite injects exactly "accept the connection, never answer".
func TestProbeIsBoundedByATimeout(t *testing.T) {
	s := NewNodeSampler(func(ctx context.Context) []NodeSample {
		<-ctx.Done() // a worker that never answers
		return []NodeSample{{Node: "worker-a", OK: false, Detail: "timeout"}}
	})
	done := make(chan struct{})
	go func() { defer close(done); s.sample(context.Background()) }()
	select {
	case <-done:
	case <-time.After(nodeProbeTimeout + 2*time.Second):
		t.Fatal("sample() hung on an unresponsive probe")
	}
}
