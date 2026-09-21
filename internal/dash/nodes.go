package dash

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Per-node resource telemetry: memory, CPU, queue depth, RTF, sessions —
// one time series per node, for every node in the system including the
// gateway itself.
//
// Why the GATEWAY polls the workers rather than the browser polling them
// directly:
//
//   - The workers' ports are published to the host for the chaos scripts,
//     but that is a convenience of this compose file, not a property of
//     the design. A dashboard that fans out to six origins from the
//     browser stops working the moment the fleet is not port-mapped.
//   - Polling once per node per second in one place gives every viewer the
//     same samples. Two browser tabs polling independently would produce
//     two different histories of the same fleet and no way to say which is
//     right.
//   - It survives a dead worker gracefully: a failed probe records an
//     explicit not-ok sample, so the graph shows a node that stopped
//     answering rather than a line that simply stops — which is the whole
//     point of watching these during a kill.
//
// The poller is strictly an observer. It never feeds the router: what a
// worker reports about its own memory has no influence on whether it is
// picked (see backend.WorkerAdvert's comment on these fields).

const (
	// One sample per node per second. Fast enough to see a kill and the
	// memory drop that follows within a chart width; slow enough that six
	// workers cost six HTTP requests a second against endpoints that do
	// no work.
	nodeSampleInterval = time.Second

	// 300 samples = 5 minutes of history at that cadence, which comfortably
	// spans a chaos scenario from injection to recovery.
	nodeSeriesCapacity = 300

	// A probe that has not answered in this long is a failed probe. Kept
	// well under the sample interval's multiple so a hung worker (the
	// blackhole fault injects exactly this) shows up as not-ok promptly
	// instead of backing the poller up.
	nodeProbeTimeout = 750 * time.Millisecond
)

// NodeSample is one reading of one node at one instant. Pointer fields are
// "not measured" versus "measured as zero" — resources.py returns null off
// Linux, and a dashboard that renders a missing reading as 0 invents a
// healthy-looking flat line out of an absent one.
type NodeSample struct {
	Node   string `json:"node"`
	AtMs   int64  `json:"at_ms"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`

	ActiveSessions int `json:"active_sessions"`
	QueueDepth     int `json:"queue_depth"`
	Inflight       int `json:"inflight"`
	Running        int `json:"running"`

	RTFP50      *float64 `json:"rtf_p50"`
	CPUPercent  *float64 `json:"cpu_percent"`
	RSSBytes    *int64   `json:"rss_bytes"`
	MemBytes    *int64   `json:"mem_bytes"`
	MemLimit    *int64   `json:"mem_limit_bytes"`
	UptimeS     float64  `json:"uptime_s"`
	Goroutines  int      `json:"goroutines,omitempty"`
	HeapBytes   *int64   `json:"heap_bytes,omitempty"`
	IsGateway   bool     `json:"is_gateway,omitempty"`
	MemPctLimit *float64 `json:"mem_pct_limit,omitempty"`

	// Shared-KV locality counters come from the worker's health response.
	// They are cumulative process counters; the browser differences them.
	KVEnabled   bool   `json:"kv_enabled,omitempty"`
	KVTier      string `json:"kv_tier,omitempty"`
	KVLocalHits int64  `json:"kv_local_hits,omitempty"`
	KVTierHits  int64  `json:"kv_tier_hits,omitempty"`
	KVMisses    int64  `json:"kv_misses,omitempty"`
}

// NodeProbe returns one sample per worker. cmd/gateway supplies it,
// closing over the router, so this package needs no dependency on
// internal/router or internal/backend.
type NodeProbe func(ctx context.Context) []NodeSample

// NodeSampler holds a bounded history per node.
type NodeSampler struct {
	mu     sync.Mutex
	series map[string][]NodeSample
	order  []string // stable display order: first-seen wins
	probe  NodeProbe
}

func NewNodeSampler(probe NodeProbe) *NodeSampler {
	return &NodeSampler{series: map[string][]NodeSample{}, probe: probe}
}

// Run samples until ctx is done. Started once from cmd/gateway.
func (n *NodeSampler) Run(ctx context.Context) {
	if n == nil {
		return
	}
	t := time.NewTicker(nodeSampleInterval)
	defer t.Stop()
	n.sample(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.sample(ctx)
		}
	}
}

func (n *NodeSampler) sample(ctx context.Context) {
	samples := []NodeSample{gatewaySample()}
	if n.probe != nil {
		pctx, cancel := context.WithTimeout(ctx, nodeProbeTimeout)
		samples = append(samples, n.probe(pctx)...)
		cancel()
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	for _, s := range samples {
		if s.MemBytes != nil && s.MemLimit != nil && *s.MemLimit > 0 {
			// Computed here, once, rather than in the browser: the ratio
			// is the number a reviewer actually reads ("worker-c is at
			// 78% of its limit"), and deriving it in JS means every
			// consumer of this API re-implements the null handling.
			pct := 100 * float64(*s.MemBytes) / float64(*s.MemLimit)
			s.MemPctLimit = &pct
		}
		if _, seen := n.series[s.Node]; !seen {
			n.order = append(n.order, s.Node)
		}
		ring := append(n.series[s.Node], s)
		if len(ring) > nodeSeriesCapacity {
			ring = ring[len(ring)-nodeSeriesCapacity:]
		}
		n.series[s.Node] = ring
	}
}

// Latest is one sample per node, newest first-seen order — what the
// topology pane renders into each node box.
func (n *NodeSampler) Latest() []NodeSample {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]NodeSample, 0, len(n.order))
	for _, id := range n.order {
		if ring := n.series[id]; len(ring) > 0 {
			out = append(out, ring[len(ring)-1])
		}
	}
	return out
}

// Series is the full retained history per node — what the graphs draw.
// Served from GET /api/nodes rather than pushed in the 250ms snapshot: at
// seven nodes × 300 samples it is far too large to re-send four times a
// second, and it changes only once a second anyway.
func (n *NodeSampler) Series() map[string][]NodeSample {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string][]NodeSample, len(n.series))
	for k, v := range n.series {
		out[k] = append([]NodeSample(nil), v...)
	}
	return out
}

// Order is the stable node ordering, so the UI does not reshuffle its
// columns every render because Go randomised a map iteration.
func (n *NodeSampler) Order() []string {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.order...)
}

// gatewaySample puts the gateway on the same axes as the workers. Its
// interesting numbers are different — goroutines and heap, not RTF — but
// its memory is measured the same way, against the same cgroup limit, so
// "which node is closest to its ceiling" is one comparable question across
// the whole system rather than two incomparable ones.
func gatewaySample() NodeSample {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heap := int64(ms.HeapAlloc)
	rss := int64(ms.Sys)

	s := NodeSample{
		Node: "gateway", AtMs: nowMs(), OK: true, IsGateway: true,
		Goroutines: runtime.NumGoroutine(),
		HeapBytes:  &heap,
		RSSBytes:   &rss,
		UptimeS:    time.Since(processStart).Seconds(),
	}
	if cur, lim := cgroupMemory(); cur != nil {
		s.MemBytes = cur
		s.MemLimit = lim
	} else {
		// No cgroup (running outside a container): Sys is the closest
		// honest stand-in for footprint, and there is no limit to show.
		s.MemBytes = &rss
	}
	return s
}

var processStart = time.Now()

// cgroupMemory mirrors worker/resources.py, for the same reason and with
// the same fallbacks: cgroup v2, then v1, then nothing. Kept as a small
// duplicate rather than a shared service because the alternative is the
// gateway asking a worker how much memory the GATEWAY is using.
func cgroupMemory() (current, limit *int64) {
	readInt := func(path string) *int64 {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		v, err := strconv.ParseInt(strings.Fields(strings.TrimSpace(string(b)))[0], 10, 64)
		if err != nil {
			return nil
		}
		return &v
	}
	current = readInt("/sys/fs/cgroup/memory.current")
	limit = readInt("/sys/fs/cgroup/memory.max") // absent, or "max", when unlimited
	if current == nil {
		current = readInt("/sys/fs/cgroup/memory/memory.usage_in_bytes")
		limit = readInt("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	}
	// cgroup v1 reports "no limit" as a sentinel near 2^63.
	if limit != nil && *limit >= (1<<40) {
		limit = nil
	}
	return current, limit
}
