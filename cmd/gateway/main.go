// Command gateway is the ASR Stress Gym gateway process: owns the
// WebSocket socket, sequence validation, the audio pipeline, the router
// (fleet-wide, shared across every connection), and the checkpoint store.
// See docs/implementation-plan.md for the milestone breakdown and
// cmd/gateway/conn.go for the per-connection wiring.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	"asr-stress-gym/internal/admission"
	"asr-stress-gym/internal/audio"
	"asr-stress-gym/internal/backend"
	"asr-stress-gym/internal/bifrost"
	"asr-stress-gym/internal/coord"
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
		workers = append(workers, w)
		log.Printf("gateway: worker %s ready — model=%s key=%s streaming=%v serializable=%v",
			id, advert.Model, advert.CompatibilityKeyHash, advert.Capabilities.Streaming, advert.Capabilities.Serializable)
	}
	return router.New(workers)
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
func workersHandler(rt *router.Router, adm *admission.Controller) http.HandlerFunc {
	type workerView struct {
		ID           string `json:"id"`
		Status       string `json:"status"`
		Outstanding  int64  `json:"outstanding"`
		RateLimited  bool   `json:"rate_limited"`
		CompatKey    string `json:"compatibility_key_hash"`
		Streaming    bool   `json:"streaming"`
		Serializable bool   `json:"serializable"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		out := struct {
			Workers           []workerView `json:"workers"`
			AdmittedSessions  int64        `json:"admitted_sessions"`
			HighWaterSessions int64        `json:"high_water_sessions"`
		}{AdmittedSessions: adm.Admitted(), HighWaterSessions: adm.HighWater()}

		for _, wk := range rt.Workers() {
			out.Workers = append(out.Workers, workerView{
				ID:           wk.ID,
				Status:       wk.Status().String(),
				Outstanding:  wk.Outstanding(),
				RateLimited:  wk.Bucket.Blocked(),
				CompatKey:    string(wk.CompatibilityKey),
				Streaming:    wk.Capabilities.Streaming,
				Serializable: wk.Capabilities.Serializable,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	}
}

// corruptCheckpointHandler is chaos scenario 5's gateway-side fault
// injection hook (build-plan.md demo 5: "corrupt checkpoint... validation
// fails, falls back to audio replay, session still succeeds"). Checkpoints
// live gateway-side (internal/coord), so this is the natural place to
// corrupt one — see internal/backend.CheckpointResp's doc comment.
func corruptCheckpointHandler(checkpoints *coord.CheckpointStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionID := r.PathValue("session_id")
		ok := checkpoints.Corrupt(sessionID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"corrupted": ok, "session_id": sessionID})
	}
}

func main() {
	dashboardAddr := envOr("DASHBOARD_ADDR", ":7000")
	wsAddr := envOr("WS_ADDR", ":7070")
	// Defaults to the single mock worker so a bare `go run ./cmd/gateway`
	// still works without the full compose fleet up; the fleet (M3+)
	// sets this explicitly to every worker service.
	workerURLs := envOr("WORKER_URLS", "worker-mock=http://worker-mock:9000")

	rt := buildRouter(parseWorkerURLs(workerURLs))
	if len(rt.Workers()) == 0 {
		log.Printf("gateway: WARNING — no workers answered at startup; every session will fail until one becomes reachable")
	}
	checkpoints := coord.NewCheckpointStore()
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
	bf := bifrost.New(envOr("BIFROST_URL", ""), envOr("BIFROST_MODEL", ""), fallbacks, 15*time.Second)
	if bf.Enabled() {
		scope := "offline sessions only"
		if envOr("BIFROST_FINALS", "") != "" {
			scope = "offline sessions AND online finals"
		}
		log.Printf("gateway: Bifrost enabled at %s — routing %s via %s; partials always direct",
			envOr("BIFROST_URL", ""), scope, bf.Describe())
	}

	cfg := connConfig{
		Router:        rt,
		Checkpoints:   checkpoints,
		Admission:     adm,
		Bifrost:       bf,
		BifrostFinals: envOr("BIFROST_FINALS", "") != "",
		NewAudioPipeline: func(sampleRateHz uint32, chunkMs float64) (audio.Pipeline, error) {
			return audio.NewVADPipeline(sampleRateHz, chunkMs, audio.DefaultVADConfig)
		},
	}

	dashMux := http.NewServeMux()
	dashMux.HandleFunc("GET /health", healthHandler)
	dashMux.HandleFunc("GET /api/debug/metrics", metricsHandler)
	dashMux.HandleFunc("GET /api/debug/workers", workersHandler(rt, adm))
	dashMux.HandleFunc("POST /api/debug/corrupt-checkpoint/{session_id}", corruptCheckpointHandler(checkpoints))

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
