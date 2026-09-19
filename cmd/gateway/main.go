// Command gateway is the ASR Stress Gym gateway process: owns the
// WebSocket socket, sequence validation, the (M1 pass-through) audio
// pipeline, and — for now, with no router yet (M3) — a direct connection
// to one statically configured worker. See docs/implementation-plan.md
// for the milestone breakdown and cmd/gateway/conn.go for the per-
// connection wiring.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/coder/websocket"
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

func main() {
	dashboardAddr := envOr("DASHBOARD_ADDR", ":7000")
	wsAddr := envOr("WS_ADDR", ":7070")
	// M1: no router, no pool — every session opens against one
	// statically configured worker (build-plan.md Phase 1's own scope).
	// Defaults to the compose service that ships with a serializable
	// adapter, matching docs/DECISIONS.md.
	workerURL := envOr("WORKER_URL", "http://worker-mock:9000")
	cfg := connConfig{workerBaseURL: workerURL}

	dashMux := http.NewServeMux()
	dashMux.HandleFunc("/health", healthHandler)

	wsMux := http.NewServeMux()
	wsMux.HandleFunc("/ws", wsHandler(cfg))

	go func() {
		log.Printf("gateway: dashboard/health listening on %s", dashboardAddr)
		if err := http.ListenAndServe(dashboardAddr, dashMux); err != nil {
			log.Fatalf("gateway: dashboard listener: %v", err)
		}
	}()

	log.Printf("gateway: ws listening on %s (worker=%s)", wsAddr, workerURL)
	if err := http.ListenAndServe(wsAddr, wsMux); err != nil {
		log.Fatalf("gateway: ws listener: %v", err)
	}
}
