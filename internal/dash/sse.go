package dash

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"asr-stress-gym/internal/metrics"
)

// snapshotInterval is docs/build-plan.md's own figure. It is a display
// cadence, not a sampling cadence: latency percentiles are computed over
// every sample the gateway took since the process started (bounded by the
// ring), not over the 250ms bucket, so a slower cadence would cost
// smoothness and not accuracy.
const snapshotInterval = 250 * time.Millisecond

// maxStreamsInSnapshot bounds the recurring payload. At the configured
// ceiling of 200 sessions the full list is ~40KB, and sending it four
// times a second is 160KB/s of mostly-unchanged rows.
//
// build-plan.md says "send everything, filter in the browser", which is
// right for the event feed and wrong here — it was written before the
// admission ceiling was 200. The compromise: the snapshot carries the
// newest rows plus a total count, and GET /api/streams serves the full
// list on demand for a reviewer who wants to scroll. Nothing is hidden;
// it is just not re-sent four times a second.
const maxStreamsInSnapshot = 50

// Latency is one series' percentiles, in milliseconds. Count is carried
// because a p99 over eleven samples is not a p99, and a chart that cannot
// say so invites reading noise as signal.
type Latency struct {
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Count int     `json:"count"`
}

// WorkerView and FleetView are the router's live view of the fleet. They
// live here, rather than in cmd/gateway, so the dashboard snapshot and the
// existing GET /api/debug/workers endpoint are the SAME struct — the chaos
// scripts parse that endpoint (cmd/chaostest/faults.go) and a second
// near-identical definition is how the two would quietly disagree about
// what "ejected" means.
type WorkerView struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Outstanding int64  `json:"outstanding"`
	RateLimited bool   `json:"rate_limited"`
	CompatKey   string `json:"compatibility_key_hash"`
	// What this worker is actually serving, and in which modes. Display
	// only — selection never reads Model (see router.Worker.Model).
	Model        string   `json:"model"`
	Modes        []string `json:"modes"`
	Streaming    bool     `json:"streaming"`
	Serializable bool     `json:"serializable"`
	// Additive since M9. Draining is an operator decision, not a health
	// state, and is reported separately for exactly that reason — see
	// router.Worker.SetDraining.
	Draining bool `json:"draining"`

	// What the ejection rule actually compared. "ejected" on its own is
	// not something a reviewer can act on; these three make it explicable
	// on the node itself — see router.Worker.Stats.
	ErrorRate    float64 `json:"error_rate"`
	LatencyP95Ms float64 `json:"latency_p95_ms"`
	LatencySamps int     `json:"latency_samples"`
}

type FleetView struct {
	Workers           []WorkerView `json:"workers"`
	AdmittedSessions  int64        `json:"admitted_sessions"`
	HighWaterSessions int64        `json:"high_water_sessions"`
	// The baseline each worker's p95 is judged against, and the multiple
	// at which a worker is ejected for gray failure.
	ClusterP95Ms  float64 `json:"cluster_p95_ms"`
	EjectAtMultip float64 `json:"eject_at_multiple"`
}

// FleetFunc is how the dashboard reads the router without importing it:
// cmd/gateway owns the Router and the admission Controller and supplies a
// closure. Keeps this package free of a dependency on the routing policy
// it only ever displays.
type FleetFunc func() FleetView

// Snapshot is the periodic state-of-the-world frame.
type Snapshot struct {
	AtMs         int64            `json:"at_ms"`
	Fleet        FleetView        `json:"fleet"`
	Metrics      metrics.Snap     `json:"metrics"`
	Streams      []Stream         `json:"streams"`
	StreamsTotal int              `json:"streams_total"`
	Push         Latency          `json:"push_latency"`
	Final        Latency          `json:"final_latency"`
	WorkerPushes map[string]int64 `json:"worker_pushes"`
	// Bifrost's own view, when it is configured. Diagnostic only — see
	// bifrost.go for why fallback_index is the number that matters.
	Bifrost BifrostStats `json:"bifrost"`
}

// Snapshot builds one frame. Cheap enough to call four times a second with
// a few hundred live streams: one mutex acquisition, one map copy, and two
// sorts over bounded sample rings.
func (h *Hub) Snapshot(fleet FleetFunc) Snapshot {
	return h.SnapshotWith(fleet, nil)
}

// SnapshotWith folds in Bifrost's scrape when one is configured.
func (h *Hub) SnapshotWith(fleet FleetFunc, bf *BifrostScraper) Snapshot {
	snap := Snapshot{AtMs: nowMs(), Metrics: metrics.Snapshot()}
	if fleet != nil {
		snap.Fleet = fleet()
	}
	snap.Bifrost = bf.Stats()
	if h == nil {
		return snap
	}

	all := h.Streams()
	snap.StreamsTotal = len(all)
	if len(all) > maxStreamsInSnapshot {
		all = all[:maxStreamsInSnapshot]
	}
	snap.Streams = all

	h.mu.Lock()
	p50, p95, p99, n := h.pushLat.percentiles()
	snap.Push = Latency{P50: p50, P95: p95, P99: p99, Count: n}
	p50, p95, p99, n = h.finalLat.percentiles()
	snap.Final = Latency{P50: p50, P95: p95, P99: p99, Count: n}
	snap.WorkerPushes = make(map[string]int64, len(h.workerPushes))
	for k, v := range h.workerPushes {
		snap.WorkerPushes[k] = v
	}
	h.mu.Unlock()

	return snap
}

// EventsHandler serves GET /api/events as Server-Sent Events.
//
// Why SSE and not the WebSocket this project already speaks:
//
//   - **Direction.** Telemetry is server→browser only. Control
//     (POST /api/chaos/...) is request/response, which plain HTTP already
//     does. Nothing here needs a bidirectional stream, and choosing one
//     would mean writing framing and a message-type dispatcher for traffic
//     that has neither.
//
//   - **Reconnection is the feature.** EventSource reconnects on its own,
//     with backoff, resending Last-Event-ID — so the server replays the
//     notable ring and the browser closes its own gap. That matters
//     *precisely when the dashboard is doing its job*: the reviewer clicks
//     Kill, and the dashboard must come back by itself rather than showing
//     a frozen chart. Over WebSocket that is hand-written JS reconnect and
//     resume logic, in a milestone with a one-day budget.
//
//   - **It shares nothing with the audio socket.** That socket (:7070)
//     is binary, sequence-validated, and has a session lifecycle; a
//     browser attaching to it is a connection that code assumes is a
//     client session. SSE lives on the control port (:7000) with the
//     health and metrics endpoints, where it belongs.
//
//   - **No dependency, no build step, debuggable with curl.** It is
//     text/event-stream over the http.ResponseWriter already in hand, and
//     `curl -N localhost:7000/api/events` prints the feed.
//
// The costs, stated rather than discovered later: it is text-only (fine —
// these are JSON frames), it is capped at six connections per origin on
// HTTP/1.1 (irrelevant for one dashboard tab), and it requires any proxy
// in front of it to disable response buffering, which is why the HA
// profile fronts the control port at L4 rather than L7 (see ha/nginx.conf).
func (h *Hub) EventsHandler(fleet FleetFunc) http.HandlerFunc {
	return h.EventsHandlerWith(fleet, nil)
}

func (h *Hub) EventsHandlerWith(fleet FleetFunc, bf *BifrostScraper) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		// Belt and braces for any proxy that honours it; the HA profile
		// avoids the problem structurally instead.
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)

		// Tell the browser how fast to come back. The default is 3s, which
		// is a long time to stare at a frozen chart after a kill.
		fmt.Fprintf(w, "retry: 1000\n\n")
		flusher.Flush()

		// Replay whatever the client missed. An EventSource sets this
		// header itself on every automatic reconnect; a curl user can set
		// it by hand.
		if last := r.Header.Get("Last-Event-ID"); last != "" {
			if id, err := strconv.ParseUint(last, 10, 64); err == nil {
				for _, ev := range h.Since(id) {
					writeEvent(w, ev)
				}
				flusher.Flush()
			}
		}

		id, events := h.subscribe()
		defer h.unsubscribe(id)

		ticker := time.NewTicker(snapshotInterval)
		defer ticker.Stop()

		// First snapshot immediately, so the page has state before the
		// first tick rather than rendering empty for a quarter second.
		writeSnapshot(w, h.SnapshotWith(fleet, bf))
		flusher.Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case ev := <-events:
				writeEvent(w, ev)
				flusher.Flush()
			case <-ticker.C:
				writeSnapshot(w, h.SnapshotWith(fleet, bf))
				flusher.Flush()
			}
		}
	}
}

// writeEvent emits a discrete event WITH an id, so a reconnect can resume
// from it. Snapshots deliberately carry no id: they are state, not
// history, and replaying a stale one on reconnect would repaint the page
// with the past.
func writeEvent(w http.ResponseWriter, ev Event) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "id: %d\nevent: event\ndata: %s\n\n", ev.ID, b)
}

func writeSnapshot(w http.ResponseWriter, s Snapshot) {
	b, err := json.Marshal(s)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", b)
}
