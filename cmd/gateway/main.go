// Command gateway is the ASR Stress Gym gateway process.
//
// At this milestone (M0) it exposes only a health endpoint so the compose
// stack can come up with green healthchecks. The WebSocket listener,
// session coordinator, router and dashboard land in M1-M9; see
// docs/implementation-plan.md for the milestone breakdown.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
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

func main() {
	addr := os.Getenv("DASHBOARD_ADDR")
	if addr == "" {
		addr = ":7000"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)

	log.Printf("gateway: health endpoint listening on %s (M0 skeleton — no WS/session/router yet)", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("gateway: %v", err)
	}
}
