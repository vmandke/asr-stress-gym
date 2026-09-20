package dash

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"
)

//go:embed static
var staticFS embed.FS

// Target is where one worker's two fault-injection surfaces live.
//
// The split is not incidental. Request-level faults (slow, blackhole, 429,
// corrupt) are module state inside the worker process and are served on
// its own port. Process-level faults (die, restore) cannot be: a process
// that has called os._exit cannot resurrect itself, so those are served by
// the supervisor parent on a second port. Routing both through one port
// would mean a killed worker could never be told to come back. See
// docs/PROTOCOL.md "Fault injection".
type Target struct {
	WorkerURL string // :9000 — the worker itself
	AdminURL  string // :9001 — the supervisor
}

// Control is the dashboard's server side: the hub, the fleet view, the
// node sampler, and the chaos targets.
type Control struct {
	Hub     *Hub
	Fleet   FleetFunc
	Nodes   *NodeSampler
	Targets map[string]Target

	// Drain is supplied by cmd/gateway, closing over the router. Graceful
	// drain is the one node action with no worker-side endpoint behind it:
	// "stop accepting new sessions, finish what you have" is a statement
	// about SELECTION, and selection lives in the gateway. Asking the
	// worker to refuse opens would be the same thing done worse — it would
	// surface as errors the router has to learn from, rather than as a
	// worker that is simply not picked.
	Drain func(worker string, on bool) error

	// LoadgenURL is the load generator's control API (cmd/loadgen
	// --control-addr), empty when there is none. The gateway only
	// forwards the button press: generating load from inside the gateway
	// would put the generator and the thing it measures in one process,
	// one scheduler and one memory limit — see cmd/loadgen/control.go.
	LoadgenURL string

	// Bifrost's own metrics, when it is configured. Nil otherwise, and
	// nil-safe throughout.
	BifrostStats *BifrostScraper

	// KVPools groups the fleet by compatibility key and names the shared
	// tier each cohort publishes to. Supplied by cmd/gateway closing over
	// the router, because the mapping is learned from the workers' own
	// /health and must not get a second home here. Nil when nothing
	// advertises a tier. See kv.go.
	KVPools KVPoolsFunc

	hc *http.Client
}

// Mount registers every dashboard route on the gateway's control-plane
// mux (:7000), alongside /health and the debug endpoints.
func (c *Control) Mount(mux *http.ServeMux) {
	if c.hc == nil {
		// Short, bounded, and its own client: a blackholed worker accepts
		// the connection and never answers, and the dashboard must not
		// inherit that hang. Longer than nodeProbeTimeout because these
		// are operator actions, not polls — a restore genuinely takes a
		// moment to spawn a child.
		c.hc = &http.Client{Timeout: 5 * time.Second}
	}

	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic("dash: embedded static assets missing: " + err.Error())
	}
	files := http.FileServer(http.FS(sub))

	mux.Handle("GET /dashboard", http.RedirectHandler("/dashboard/", http.StatusMovedPermanently))
	mux.Handle("GET /dashboard/", http.StripPrefix("/dashboard/", files))
	// The bare root too: a reviewer told to "open localhost:7000" should
	// land on the dashboard, not a 404 that looks like a broken build.
	mux.Handle("GET /{$}", http.RedirectHandler("/dashboard/", http.StatusFound))

	mux.HandleFunc("GET /api/events", c.Hub.EventsHandlerWith(c.Fleet, c.BifrostStats))
	mux.HandleFunc("GET /api/streams", c.streamsHandler)
	mux.HandleFunc("GET /api/streams/{id}/log", c.streamLogHandler)
	mux.HandleFunc("GET /api/nodes", c.nodesHandler)
	mux.HandleFunc("POST /api/chaos/{worker}/{action}", c.chaosHandler)
	mux.HandleFunc("POST /api/load/{n}", c.loadHandler)
	mux.HandleFunc("POST /api/load/stop", c.loadStopHandler)
	mux.HandleFunc("GET /api/load", c.loadStatusHandler)
	mux.HandleFunc("GET /api/kv", c.kvHandler)
}

// loadHandler is build-plan.md's POST /api/load/{n}: ramp the load
// generator to n streams. A thin forward, deliberately — the gateway does
// not know how to make a stream and should not learn.
func (c *Control) loadHandler(w http.ResponseWriter, r *http.Request) {
	if c.LoadgenURL == "" {
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error": "no load generator configured (set LOADGEN_URL, or run cmd/loadgen yourself)",
		})
		return
	}
	n, err := strconv.Atoi(r.PathValue("n"))
	if err != nil || n < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "n must be a non-negative integer"})
		return
	}
	// Zero streams is how a slider expresses "off"; forwarding it as a
	// load of 0 would be rejected by the generator, so it means stop.
	if n == 0 {
		c.forwardLoad(w, "/stop", "")
		return
	}
	q := r.URL.Query()
	query := fmt.Sprintf("streams=%d", n)
	for _, k := range []string{"duration", "mode", "speech-ratio", "ramp"} {
		if v := q.Get(k); v != "" {
			query += "&" + k + "=" + url.QueryEscape(v)
		}
	}
	c.forwardLoad(w, "/load", query)
}

func (c *Control) loadStopHandler(w http.ResponseWriter, r *http.Request) {
	if c.LoadgenURL == "" {
		writeJSON(w, http.StatusNotImplemented, map[string]any{"error": "no load generator configured"})
		return
	}
	c.forwardLoad(w, "/stop", "")
}

func (c *Control) loadStatusHandler(w http.ResponseWriter, r *http.Request) {
	if c.LoadgenURL == "" {
		writeJSON(w, http.StatusOK, map[string]any{"available": false})
		return
	}
	resp, err := c.hc.Get(c.LoadgenURL + "/status")
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"available": false, "error": err.Error()})
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	w.Header().Set("Content-Type", "application/json")
	// Passed through verbatim with an availability flag spliced in, so the
	// dashboard reads the generator's own answer rather than this
	// handler's paraphrase of it.
	w.Write([]byte(`{"available":true,"status":`))
	w.Write(body)
	w.Write([]byte(`}`))
}

func (c *Control) forwardLoad(w http.ResponseWriter, path, query string) {
	target := c.LoadgenURL + path
	if query != "" {
		target += "?" + query
	}
	body, err := c.post(target, nil)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "target": target})
		return
	}
	c.Hub.Chaos("loadgen", path[1:], query)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(body))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// streamsHandler serves the FULL stream list, unlike the periodic snapshot
// which carries only the newest maxStreamsInSnapshot rows. See sse.go.
func (c *Control) streamsHandler(w http.ResponseWriter, r *http.Request) {
	streams := c.Hub.Streams()
	writeJSON(w, http.StatusOK, map[string]any{"streams": streams, "count": len(streams)})
}

func (c *Control) streamLogHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	log, ok := c.Hub.Log(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown stream", "id": id})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "events": log})
}

func (c *Control) nodesHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"order":  c.Nodes.Order(),
		"series": c.Nodes.Series(),
		"latest": c.Nodes.Latest(),
	})
}

// chaosHandler is the control plane. It adds no server capability: every
// action here is one the headless chaos scripts already drive
// (cmd/chaostest/faults.go). That equivalence is the point — anything a
// reviewer can demonstrate by clicking is also assertable in CI, and if
// the two ever diverge the scripts are the truth.
func (c *Control) chaosHandler(w http.ResponseWriter, r *http.Request) {
	worker := r.PathValue("worker")
	action := r.PathValue("action")

	target, ok := c.Targets[worker]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown worker", "worker": worker})
		return
	}

	q := r.URL.Query()
	var (
		url    string
		body   any
		detail string
	)

	switch action {
	case "kill":
		url, body, detail = target.AdminURL+"/admin/die", nil, "SIGKILL"
	case "restore":
		// Returns as soon as the child is SPAWNED, not when it is serving:
		// a real adapter then loads weights, which for whisper_ct2 is
		// seconds. The dashboard shows the node as down until the node
		// sampler sees it answer, which is the honest signal — the same
		// trap chaos.sh hit and now polls /health for.
		url, body, detail = target.AdminURL+"/admin/restore", nil, "respawn"
	case "slow":
		ms := intParam(q.Get("ms"), 500)
		url, body, detail = target.WorkerURL+"/admin/slow", map[string]any{"ms": ms}, fmt.Sprintf("+%dms", ms)
	case "blackhole":
		on := boolParam(q.Get("on"), true)
		url, body, detail = target.WorkerURL+"/admin/blackhole", map[string]any{"on": on}, fmt.Sprintf("on=%v", on)
	case "429":
		rate := floatParam(q.Get("rate"), 0.5)
		url, body, detail = target.WorkerURL+"/admin/429", map[string]any{"rate": rate}, fmt.Sprintf("rate=%.2f", rate)
	case "corrupt":
		on := boolParam(q.Get("on"), true)
		url, body, detail = target.WorkerURL+"/admin/corrupt", map[string]any{"on": on}, fmt.Sprintf("on=%v", on)
	case "reset":
		url, body, detail = target.WorkerURL+"/admin/reset", nil, "clear faults"
	case "drain":
		on := boolParam(q.Get("on"), true)
		if c.Drain == nil {
			writeJSON(w, http.StatusNotImplemented, map[string]any{"error": "drain not wired"})
			return
		}
		if err := c.Drain(worker, on); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		c.Hub.Chaos(worker, action, fmt.Sprintf("on=%v", on))
		writeJSON(w, http.StatusOK, map[string]any{"worker": worker, "action": action, "draining": on})
		return
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unknown action", "action": action})
		return
	}

	resp, err := c.post(url, body)
	if err != nil {
		// A kill is expected to sometimes look like a failure from here:
		// the supervisor answers, but if the request races the child's
		// death the connection can drop. Reported honestly rather than
		// swallowed — the node sampler is what confirms the outcome.
		c.Hub.Chaos(worker, action, detail+" (request error: "+err.Error()+")")
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "worker": worker, "action": action})
		return
	}

	c.Hub.Chaos(worker, action, detail)
	writeJSON(w, http.StatusOK, map[string]any{
		"worker": worker, "action": action, "detail": detail, "worker_response": resp,
	})
}

func (c *Control) post(url string, body any) (string, error) {
	payload := []byte("{}")
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return "", err
		}
		payload = b
	}
	resp, err := c.hc.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode != http.StatusOK {
		return string(out), fmt.Errorf("%s: status %d", url, resp.StatusCode)
	}
	return string(out), nil
}

func intParam(s string, fallback int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return fallback
}

func floatParam(s string, fallback float64) float64 {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return v
	}
	return fallback
}

func boolParam(s string, fallback bool) bool {
	if v, err := strconv.ParseBool(s); err == nil {
		return v
	}
	return fallback
}

// SortedTargets is the stable worker ordering for the topology pane.
func SortedTargets(t map[string]Target) []string {
	out := make([]string, 0, len(t))
	for k := range t {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
