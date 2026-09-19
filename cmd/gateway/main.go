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
	"strings"
	"time"

	"github.com/coder/websocket"

	"asr-stress-gym/internal/audio"
	"asr-stress-gym/internal/backend"
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
	cfg := connConfig{
		Router:      rt,
		Checkpoints: checkpoints,
		NewAudioPipeline: func(sampleRateHz uint32, chunkMs float64) (audio.Pipeline, error) {
			return audio.NewVADPipeline(sampleRateHz, chunkMs, audio.DefaultVADConfig)
		},
	}

	dashMux := http.NewServeMux()
	dashMux.HandleFunc("GET /health", healthHandler)
	dashMux.HandleFunc("GET /api/debug/metrics", metricsHandler)
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
