package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Request-level fault injection. These hit the WORKER's own port (9000),
// not the supervisor's admin port (9001) — the split is deliberate and
// documented in docs/PROTOCOL.md "Fault injection": killing a process
// needs a surviving parent, but making a live process misbehave does not,
// and routing the two through one port would mean a killed worker could
// no longer be told to stop being killed.
func postAdmin(baseURL, path string, body any) error {
	var payload *strings.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = strings.NewReader(string(b))
	} else {
		payload = strings.NewReader("{}")
	}
	resp, err := http.Post(baseURL+path, "application/json", payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s%s: unexpected status %d", baseURL, path, resp.StatusCode)
	}
	return nil
}

func injectSlow(w workerRef, d time.Duration) error {
	return postAdmin(w.healthURL, "/admin/slow", map[string]any{"ms": d.Milliseconds()})
}

func injectBlackhole(w workerRef, on bool) error {
	return postAdmin(w.healthURL, "/admin/blackhole", map[string]any{"on": on})
}

func inject429(w workerRef, rate float64) error {
	return postAdmin(w.healthURL, "/admin/429", map[string]any{"rate": rate})
}

// resetFaults clears every request-level fault on a worker. Best-effort
// and error-tolerant: it runs in deferred cleanup, where a worker that
// has since been killed outright is an expected state, not a failure of
// the reset.
func resetFaults(w workerRef) {
	_ = postAdmin(w.healthURL, "/admin/reset", nil)
}

func resetAllFaults(fleet map[string]workerRef) {
	for _, w := range fleet {
		resetFaults(w)
	}
}

// --- router introspection (GET /api/debug/workers) ---

type workerView struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Outstanding int64  `json:"outstanding"`
	RateLimited bool   `json:"rate_limited"`
	Streaming   bool   `json:"streaming"`
}

type fleetView struct {
	Workers           []workerView `json:"workers"`
	AdmittedSessions  int64        `json:"admitted_sessions"`
	HighWaterSessions int64        `json:"high_water_sessions"`
}

func fetchFleet(gatewayURL string) (fleetView, error) {
	resp, err := http.Get(gatewayURL + "/api/debug/workers")
	if err != nil {
		return fleetView{}, err
	}
	defer resp.Body.Close()
	var out fleetView
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fleetView{}, err
	}
	return out, nil
}

func (f fleetView) statusOf(id string) string {
	for _, w := range f.Workers {
		if w.ID == id {
			return w.Status
		}
	}
	return "unknown"
}

// awaitWorkerStatus polls until a worker reaches one of the wanted
// statuses, or the deadline passes. Returns the last status seen, so a
// failure message can say what actually happened rather than only that
// something did not.
func awaitWorkerStatus(gatewayURL, id string, within time.Duration, wanted ...string) (string, bool) {
	deadline := time.Now().Add(within)
	last := "unknown"
	for time.Now().Before(deadline) {
		f, err := fetchFleet(gatewayURL)
		if err == nil {
			last = f.statusOf(id)
			for _, w := range wanted {
				if last == w {
					return last, true
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return last, false
}
