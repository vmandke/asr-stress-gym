// Command gateway is the ASR Stress Gym gateway process: owns the
// WebSocket socket, sequence validation, the audio pipeline, the router
// (fleet-wide, shared across every connection), and the checkpoint store.
// See docs/implementation-plan.md for the milestone breakdown and
// cmd/gateway/conn.go for the per-connection wiring.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"asr-stress-gym/internal/admission"
	"asr-stress-gym/internal/audio"
	"asr-stress-gym/internal/backend"
	"asr-stress-gym/internal/bifrost"
	"asr-stress-gym/internal/coord"
	"asr-stress-gym/internal/dash"
	"asr-stress-gym/internal/metrics"
	"asr-stress-gym/internal/router"
	"asr-stress-gym/internal/session"
)

var startedAt = time.Now()

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":     "READY",
		"component":  "gateway",
		"uptime_sec": int(time.Since(startedAt).Seconds()),
	})
}

func wsHandler(cfg connConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// InsecureSkipVerify: this project has no auth/TLS in scope
		// (build-plan.md's own non-goals list) and its clients — loadgen,
		// the smoke tester — are plain Go processes that never send a
		// browser-style Origin header. coder/websocket's default
		// same-origin check would reject them outright; relaxing it here
		// is a deliberate scope choice, not an oversight.
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			log.Printf("gateway: ws accept: %v", err)
			return
		}
		handleConnection(r.Context(), ws, cfg)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envInt reads an integer env var, falling back on absence OR on a value
// that does not parse. A capacity knob that silently became 0 because
// someone typed "200 " would turn the admission ceiling into "refuse
// everyone", which is a worse failure than ignoring the override.
func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n <= 0 {
		log.Printf("gateway: ignoring %s=%q (want a positive integer); using %d", key, v, fallback)
		return fallback
	}
	return n
}

// envEnabled accepts the conventional empty/0/false/off spellings. It is
// intentionally only used for benchmark isolation: a cold-replay run must
// be able to turn off checkpoint creation without maintaining a second
// gateway binary. Normal deployments keep the default true.
func envEnabled(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	switch v {
	case "0", "false", "off", "no":
		return false
	case "1", "true", "on", "yes":
		return true
	default:
		log.Printf("gateway: ignoring %s=%q (want boolean); using %t", key, v, fallback)
		return fallback
	}
}

// parseWorkerURLs reads "id=url,id=url,..." — e.g.
// "worker-a=http://worker-a:9000,worker-b=http://worker-b:9000".
func parseWorkerURLs(spec string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		id, url, ok := strings.Cut(pair, "=")
		if !ok {
			log.Printf("gateway: skipping malformed WORKER_URLS entry %q (want id=url)", pair)
			continue
		}
		out[id] = url
	}
	return out
}

// buildRouter queries each configured worker's /health once at startup to
// learn its CompatibilityKey and Capabilities (both needed by every Pick
// call — see internal/backend.WorkerAdvert's doc comment). Retries
// briefly per worker: compose's depends_on/service_healthy should mean
// they're already up, but this is defensive against a startup race
// rather than assuming perfect ordering. A worker that never answers is
// logged and left OUT of the fleet rather than crashing the gateway —
// build-plan.md's "Startup ordering": "The gateway must also tolerate a
// worker being absent at boot."
func buildRouter(urls map[string]string) *router.Router {
	const (
		healthRetries = 10
		healthBackoff = 500 * time.Millisecond
		healthTimeout = 2 * time.Second
	)

	var workers []*router.Worker
	for id, url := range urls {
		client := backend.NewHTTPClient(url)
		var advert backend.WorkerAdvert
		var err error
		for attempt := 0; attempt < healthRetries; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), healthTimeout)
			advert, err = client.Health(ctx)
			cancel()
			if err == nil {
				break
			}
			time.Sleep(healthBackoff)
		}
		if err != nil {
			log.Printf("gateway: worker %s (%s) never answered /health, excluding from the fleet: %v", id, url, err)
			continue
		}
		w := router.NewWorker(id, client, session.CacheCompatibilityKey(advert.CompatibilityKeyHash), advert.Capabilities)
		w.Model = advert.Model
		if advert.KVTier != nil && advert.KVTier.Enabled {
			w.KVTierURL = advert.KVTier.URL
		}
		workers = append(workers, w)
		log.Printf("gateway: worker %s ready — model=%s key=%s streaming=%v serializable=%v kv_tier=%s",
			id, advert.Model, advert.CompatibilityKeyHash, advert.Capabilities.Streaming,
			advert.Capabilities.Serializable, orNone(w.KVTierURL))
	}
	return router.New(workers)
}

// registerFleetWorker is the second half of an add-worker operation. The
// fleet manager has already created the container and registered its Bifrost
// provider, but neither fact is permission to join a cache cohort. The
// gateway re-reads /health and compares the advertised compatibility key to
// the boot worker for that family before selection can ever see it.
func registerFleetWorker(rt *router.Router, family, id, url string) error {
	if !strings.HasPrefix(id, "worker-"+family+"-") {
		return fmt.Errorf("worker %q does not have the requested %s family identity", id, family)
	}
	var cohort *router.Worker
	prefix := "worker-" + family + "-"
	for _, existing := range rt.Workers() {
		if strings.HasPrefix(existing.ID, prefix) {
			cohort = existing
			break
		}
	}
	if cohort == nil {
		return fmt.Errorf("no boot worker exists for family %q", family)
	}
	client := backend.NewHTTPClient(url)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	advert, err := client.Health(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("health check %s: %w", id, err)
	}
	if advert.CompatibilityKeyHash != string(cohort.CompatibilityKey) {
		return fmt.Errorf("%s compatibility key %q does not match %s cohort %q", id,
			advert.CompatibilityKeyHash, family, cohort.CompatibilityKey)
	}
	if !advert.Capabilities.Serializable || advert.KVTier == nil || !advert.KVTier.Enabled || advert.KVTier.URL != cohort.KVTierURL {
		return fmt.Errorf("%s does not advertise the %s cohort's serializable KV tier", id, family)
	}
	worker := router.NewWorker(id, client, session.CacheCompatibilityKey(advert.CompatibilityKeyHash), advert.Capabilities)
	worker.Model = advert.Model
	worker.KVTierURL = advert.KVTier.URL
	if err := rt.Add(worker); err != nil {
		return err
	}
	log.Printf("gateway: dynamically admitted %s into %s cohort (key=%s tier=%s)", id, family, worker.CompatibilityKey, worker.KVTierURL)
	return nil
}

// orNone keeps the startup log honest about an absent tier rather than
// printing an empty field that reads as a truncated line.
func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics.Snapshot())
}

// workersHandler exposes the router's live view of the fleet: each
// worker's health status, load and rate-limit state.
//
// Chaos scenario 6 (gray failure) is the reason it exists. That scenario
// asserts a worker is "ejected within 15s on latency, not errors" —
// a statement purely about the ROUTER's internal state, which no other
// endpoint reveals. Without this, the only way to test it is to infer
// ejection from where traffic stopped going, which cannot distinguish
// "ejected for latency" from "ejected for errors" and so cannot test the
// thing the scenario is actually about.
//
// Read-only, and derived from the router rather than mirrored, so it
// cannot drift from what selection really sees.
// The view itself moved to internal/dash at M9 so the dashboard snapshot
// and this endpoint are literally the same struct. Two near-identical
// definitions is how they would drift, and cmd/chaostest parses this one.
func fleetView(rt *router.Router, adm *admission.Controller) dash.FleetFunc {
	return func() dash.FleetView {
		out := dash.FleetView{
			AdmittedSessions:  adm.Admitted(),
			HighWaterSessions: adm.HighWater(),
			ClusterP95Ms:      float64(rt.ClusterP95().Microseconds()) / 1000,
			EjectAtMultip:     router.GrayFailureMultiple,
		}
		for _, wk := range rt.Workers() {
			errRate, p95, samples := wk.Stats()
			out.Workers = append(out.Workers, dash.WorkerView{
				ErrorRate:    errRate,
				LatencyP95Ms: float64(p95.Microseconds()) / 1000,
				LatencySamps: samples,
				ID:           wk.ID,
				Status:       wk.Status().String(),
				Outstanding:  wk.Outstanding(),
				RateLimited:  wk.Bucket.Blocked(),
				CompatKey:    string(wk.CompatibilityKey),
				Model:        wk.Model,
				Modes:        wk.Capabilities.Modes,
				Streaming:    wk.Capabilities.Streaming,
				Serializable: wk.Capabilities.Serializable,
				Draining:     wk.Draining(),
			})
		}
		return out
	}
}

func workersHandler(fleet dash.FleetFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fleet())
	}
}

// nodeProbe reads every worker's /health once per sampling tick, for the
// dashboard's per-node resource graphs.
//
// It asks the WORKER, not the router: the router's view is about
// selection (is this worker a candidate) and is derived from the gateway's
// own call outcomes, while this is about the worker's own resource
// reality. They can legitimately disagree — a healthy worker at 95% of its
// memory limit, or an ejected worker that is perfectly idle — and seeing
// both is the point. A failed probe is recorded as an explicit not-ok
// sample so a dead node draws as a gap, not as zeros.
func nodeProbe(rt *router.Router) dash.NodeProbe {
	return func(ctx context.Context) []dash.NodeSample {
		workers := rt.Workers()
		out := make([]dash.NodeSample, len(workers))
		var wg sync.WaitGroup
		for i, wk := range workers {
			wg.Add(1)
			go func(i int, wk *router.Worker) {
				defer wg.Done()
				s := dash.NodeSample{Node: wk.ID, AtMs: time.Now().UnixMilli()}
				adv, err := wk.Client.Health(ctx)
				if err != nil {
					rt.MarkUnhealthy(wk.ID)
					s.Detail = err.Error()
					out[i] = s
					return
				}
				s.OK = true
				s.ActiveSessions = adv.ActiveSessions
				s.QueueDepth = adv.QueueDepth
				s.Inflight = adv.Inflight
				s.Running = adv.Running
				s.RTFP50 = adv.RTFP50
				s.CPUPercent = adv.CPUPercent
				s.RSSBytes = adv.RSSBytes
				s.MemBytes = adv.CgroupMemoryBytes
				s.MemLimit = adv.CgroupMemoryLimitBytes
				s.UptimeS = adv.UptimeS
				if adv.KVTier != nil && adv.KVTier.Enabled {
					s.KVEnabled = true
					s.KVTier = adv.KVTier.URL
					s.KVLocalHits = adv.KVTier.LocalHits
					s.KVTierHits = adv.KVTier.TierHits
					s.KVMisses = adv.KVTier.Misses
				}
				if s.MemBytes == nil {
					s.MemBytes = adv.RSSBytes // no cgroup: RSS is the honest stand-in
				}
				out[i] = s
			}(i, wk)
		}
		wg.Wait()
		return out
	}
}

// chaosTargets maps each worker to its two fault-injection surfaces. The
// supervisor's admin port is derived from the worker URL by replacing the
// port, because compose fixes that relationship (9000/9001 in every worker
// service) and a second env var listing the same six hosts again is a
// second thing to keep in sync. WORKER_ADMIN_URLS overrides it for any
// deployment where that does not hold.
func chaosTargets(urls map[string]string, override string) map[string]dash.Target {
	admin := parseWorkerURLs(override)
	out := map[string]dash.Target{}
	for id, u := range urls {
		a, ok := admin[id]
		if !ok {
			a = strings.Replace(u, ":9000", ":9001", 1)
		}
		out[id] = dash.Target{WorkerURL: u, AdminURL: a}
	}
	return out
}

// corruptCheckpointHandler is chaos scenario 5's gateway-side fault
// injection hook (build-plan.md demo 5: "corrupt checkpoint... validation
// fails, falls back to audio replay, session still succeeds"). Checkpoints
// live gateway-side (internal/coord), so this is the natural place to
// corrupt one — see internal/backend.CheckpointResp's doc comment.
func corruptCheckpointHandler(checkpoints *coord.CheckpointStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionID := r.PathValue("session_id")
		ok := checkpoints != nil && checkpoints.Corrupt(sessionID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"corrupted": ok, "session_id": sessionID})
	}
}

func main() {
	dashboardAddr := envOr("DASHBOARD_ADDR", ":7000")
	wsAddr := envOr("WS_ADDR", ":7070")
	// Defaults to one real worker so a bare `go run ./cmd/gateway` still
	// resolves something; the fleet sets this explicitly to every worker
	// service. The mock worker was removed from the deployed fleet at M11
	// — the `mock` ADAPTER remains, for unit tests that must pass on a
	// clone with no model weights.
	workerURLs := envOr("WORKER_URLS", "worker-a=http://worker-a:9000")

	rt := buildRouter(parseWorkerURLs(workerURLs))
	if len(rt.Workers()) == 0 {
		log.Printf("gateway: WARNING — no workers answered at startup; every session will fail until one becomes reachable")
	}
	var checkpoints *coord.CheckpointStore
	if envEnabled("CHECKPOINTS_ENABLED", true) {
		checkpoints = coord.NewCheckpointStore()
	} else {
		log.Printf("gateway: checkpoints disabled (benchmark cold-replay mode)")
	}
	adm := admission.New(admission.Config{
		MaxSessions:  int64(envInt("MAX_SESSIONS", 200)),
		SoftSessions: int64(envInt("SOFT_SESSIONS", 150)),
		RetryAfter:   admissionRetryAfter,
	})
	// Nil unless BIFROST_URL is set — see internal/bifrost. A nil client
	// means finals take the direct path, which is also the fallback on any
	// Bifrost error, so this is never load-bearing for correctness.
	var fallbacks []string
	if raw := envOr("BIFROST_FALLBACKS", ""); raw != "" {
		for _, f := range strings.Split(raw, ",") {
			if f = strings.TrimSpace(f); f != "" {
				fallbacks = append(fallbacks, f)
			}
		}
	}
	// Bifrost is the default streaming control plane. Compose supplies this
	// service name; a local developer can still override BIFROST_URL.
	bf := bifrost.New(envOr("BIFROST_URL", "http://bifrost:8080"), envOr("BIFROST_MODEL", ""), fallbacks, 15*time.Second)
	if bf.Enabled() {
		// The pool is chosen PER SESSION from the router's own view
		// (conn.go bifrostPool), so there is no single chain to print
		// here any more — printing one would describe behaviour the
		// gateway no longer has.
		log.Printf("gateway: Bifrost enabled at %s — routing stateless streaming; the provider pool is derived per session "+
			"from the router's compatibility-key cohort, so a fallback chain can never cross a model family. "+
			"KV bytes travel directly between workers and their family tier.", envOr("BIFROST_URL", "http://bifrost:8080"))
	}

	// Bifrost's own metrics endpoint lives beside its API. Scraped for the
	// dashboard only; nothing here influences routing.
	bifrostStats := dash.NewBifrostScraper(envOr("BIFROST_URL", "http://bifrost:8080"))
	go bifrostStats.Run(context.Background())

	hub := dash.NewHub()
	nodes := dash.NewNodeSampler(nodeProbe(rt))
	go nodes.Run(context.Background())

	cfg := connConfig{
		Router:            rt,
		Checkpoints:       checkpoints,
		Admission:         adm,
		Bifrost:           bf,
		Hub:               hub,
		BifrostFinals:     envOr("BIFROST_FINALS", "") != "",
		BifrostModelAlias: envOr("BIFROST_MODEL_ALIAS", "asr-1"),
		StatelessStream:   envEnabled("STATELESS_STREAM", true),
		NewAudioPipeline: func(sampleRateHz uint32, chunkMs float64) (audio.Pipeline, error) {
			return audio.NewVADPipeline(sampleRateHz, chunkMs, audio.DefaultVADConfig)
		},
	}

	fleet := fleetView(rt, adm)
	control := &dash.Control{
		Hub:             hub,
		Fleet:           fleet,
		Nodes:           nodes,
		Targets:         chaosTargets(parseWorkerURLs(workerURLs), envOr("WORKER_ADMIN_URLS", "")),
		LoadgenURL:      envOr("LOADGEN_URL", ""),
		FleetManagerURL: envOr("FLEET_MANAGER_URL", ""),
		RegisterWorker: func(family, id, url string) error {
			return registerFleetWorker(rt, family, id, url)
		},
		BifrostStats: bifrostStats,
		// Cohorts, derived live from the router rather than configured:
		// workers are grouped by the compatibility key they advertise, and
		// each cohort's tier is whatever its members advertise at /health.
		// The dashboard therefore cannot show a family->tier mapping the
		// fleet does not actually have.
		KVPools: func() []dash.KVPool {
			byKey := map[string]*dash.KVPool{}
			for _, w := range rt.Workers() {
				key := string(w.CompatibilityKey)
				p, ok := byKey[key]
				if !ok {
					p = &dash.KVPool{CompatKey: key}
					byKey[key] = p
				}
				p.Workers = append(p.Workers, w.ID)
				if p.TierURL == "" {
					p.TierURL = w.KVTierURL
				}
			}
			out := make([]dash.KVPool, 0, len(byKey))
			for _, p := range byKey {
				out = append(out, *p)
			}
			sort.Slice(out, func(i, j int) bool { return out[i].Workers[0] < out[j].Workers[0] })
			return out
		},
		// Graceful drain is a routing decision, so it is implemented here
		// rather than by asking the worker to start refusing — see
		// dash.Control.Drain and router.Worker.SetDraining.
		Drain: func(id string, on bool) error {
			w, ok := rt.Find(id)
			if !ok {
				return fmt.Errorf("unknown worker %q", id)
			}
			w.SetDraining(on)
			return nil
		},
	}

	dashMux := http.NewServeMux()
	dashMux.HandleFunc("GET /health", healthHandler)
	dashMux.HandleFunc("GET /api/debug/metrics", metricsHandler)
	dashMux.HandleFunc("GET /api/debug/workers", workersHandler(fleet))
	dashMux.HandleFunc("POST /api/debug/corrupt-checkpoint/{session_id}", corruptCheckpointHandler(checkpoints))
	control.Mount(dashMux)

	wsMux := http.NewServeMux()
	wsMux.HandleFunc("/ws", wsHandler(cfg))

	go func() {
		log.Printf("gateway: dashboard/health listening on %s", dashboardAddr)
		if err := http.ListenAndServe(dashboardAddr, dashMux); err != nil {
			log.Fatalf("gateway: dashboard listener: %v", err)
		}
	}()

	log.Printf("gateway: ws listening on %s (%d worker(s) in the fleet)", wsAddr, len(rt.Workers()))
	if err := http.ListenAndServe(wsAddr, wsMux); err != nil {
		log.Fatalf("gateway: ws listener: %v", err)
	}
}
