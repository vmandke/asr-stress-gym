package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// The load-control API (M9). With --control-addr, loadgen starts idle and
// waits to be told what to do, so `docker compose up` brings the stack up
// QUIET and the reviewer decides when to apply load — docs/build-plan.md,
// "loadgen starts idle and is driven from the dashboard".
//
// This lives in loadgen rather than in the gateway on purpose. The
// gateway could open WebSocket sessions against itself, but then the
// thing being measured and the thing generating the load would share a
// process, a scheduler and a memory limit — and the first symptom of
// that is a latency chart that bends under load for reasons that have
// nothing to do with the gateway. Keeping the generator a separate
// container with its own CPU budget is what makes the numbers mean
// anything; the gateway only proxies the button press.
//
//	POST /load?streams=N[&duration=..&mode=..&speech-ratio=..]  start (replaces any current run)
//	POST /stop                                                   stop the current run
//	GET  /status                                                 what is running now
type controller struct {
	base config // the flags loadgen was started with; each request overlays on this

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
	current config
	started time.Time
	// gen identifies the current run. A finishing run only clears the
	// state if it is still the current one — otherwise a run that was
	// replaced, and takes a moment to unwind its streams, would report
	// "idle" over the top of its own replacement.
	gen uint64
}

func serveControl(base config) error {
	c := &controller{base: base}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /load", c.handleLoad)
	mux.HandleFunc("POST /stop", c.handleStop)
	mux.HandleFunc("GET /status", c.handleStatus)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "READY", "component": "loadgen"})
	})

	log.Printf("loadgen: idle, control API on %s (POST /load?streams=N to begin)", base.controlAddr)
	return http.ListenAndServe(base.controlAddr, mux)
}

func (c *controller) handleLoad(w http.ResponseWriter, r *http.Request) {
	cfg := c.base
	cfg.controlAddr = ""
	cfg.out = "" // a controlled run reports to the dashboard, not to a CSV; see docs/BENCH.md

	q := r.URL.Query()
	if v := q.Get("streams"); v != "" {
		n, err := parseInt(v)
		if err != nil || n < 1 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "streams must be a positive integer"})
			return
		}
		cfg.streams = n
	}
	if v := q.Get("duration"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad duration: " + err.Error()})
			return
		}
		cfg.duration = d
	}
	if v := q.Get("mode"); v != "" {
		cfg.mode = v
	}
	if v := q.Get("speech-ratio"); v != "" {
		f, err := parseFloat(v)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad speech-ratio"})
			return
		}
		cfg.speechRatio = f
	}
	if v := q.Get("ramp"); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			cfg.ramp = d
		}
	} else if cfg.ramp == 0 && cfg.streams > 1 {
		// A default ramp for dashboard-driven runs, which the CLI does not
		// have. Starting N streams in the same instant makes every one of
		// them see an idle fleet and pick the same top-scored worker, so
		// the topology shows a single hot node for reasons that are an
		// artefact of the launch rather than of the routing policy.
		cfg.ramp = time.Duration(cfg.streams) * 400 * time.Millisecond
		if cfg.ramp > 15*time.Second {
			cfg.ramp = 15 * time.Second
		}
	}

	c.mu.Lock()
	// Replace, not reject: the dashboard's slider is a "make it be N
	// streams" control, and refusing because a previous run is still up
	// would make the button feel broken.
	if c.cancel != nil {
		c.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.gen++
	gen := c.gen
	c.cancel, c.running, c.current, c.started = cancel, true, cfg, time.Now()
	c.mu.Unlock()

	go func() {
		if err := runCtx(ctx, cfg); err != nil {
			log.Printf("loadgen: run: %v", err)
		}
		c.mu.Lock()
		if c.gen == gen {
			c.running = false
			c.cancel = nil
		}
		c.mu.Unlock()
	}()

	writeJSON(w, http.StatusOK, map[string]any{
		"started": true, "streams": cfg.streams,
		"duration": cfg.duration.String(), "ramp": cfg.ramp.String(), "mode": cfg.mode,
	})
}

func (c *controller) handleStop(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	was := c.running
	c.gen++ // invalidate the running batch's completion bookkeeping
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.running = false
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"stopped": was})
}

func (c *controller) handleStatus(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]any{"running": c.running}
	if c.running {
		out["streams"] = c.current.streams
		out["mode"] = c.current.mode
		out["duration"] = c.current.duration.String()
		out["elapsed_s"] = int(time.Since(c.started).Seconds())
	}
	writeJSON(w, http.StatusOK, out)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

func parseFloat(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%g", &f)
	return f, err
}
