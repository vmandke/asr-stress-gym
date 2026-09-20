package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// cmdFleet prints the router's own live view of the fleet.
//
// Deliberately sourced from the gateway's /api/debug/workers rather than
// by polling each worker's /health directly. Those are different
// questions: a worker's /health says what the WORKER thinks, and the
// router's view says what SELECTION actually sees — health status,
// ejection, rate-limit budget, pinned session count. A worker can be
// perfectly healthy and still be ejected, and only this view shows that.
func cmdFleet() error {
	debugURL := envOr("GATEWAY_DEBUG_URL", "http://localhost:7000")

	resp, err := http.Get(debugURL + "/api/debug/workers")
	if err != nil {
		return fmt.Errorf("%s: %w (is the stack up? `make up`)", debugURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s/api/debug/workers: status %d", debugURL, resp.StatusCode)
	}

	var view struct {
		Workers []struct {
			ID           string `json:"id"`
			Status       string `json:"status"`
			Outstanding  int64  `json:"outstanding"`
			RateLimited  bool   `json:"rate_limited"`
			CompatKey    string `json:"compatibility_key_hash"`
			Streaming    bool   `json:"streaming"`
			Serializable bool   `json:"serializable"`
		} `json:"workers"`
		AdmittedSessions  int64 `json:"admitted_sessions"`
		HighWaterSessions int64 `json:"high_water_sessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		return err
	}

	// Group by compatibility key: two workers sharing one is what makes a
	// same-model failover possible at all, and it is invisible in a flat
	// list of 64-character hashes.
	short := map[string]string{}
	label := 1
	for _, w := range view.Workers {
		if _, seen := short[w.CompatKey]; !seen {
			short[w.CompatKey] = fmt.Sprintf("K%d", label)
			label++
		}
	}

	fmt.Printf("%-14s %-5s %-10s %-9s %-7s %-12s %s\n",
		"worker", "key", "status", "streaming", "cache", "pinned", "rate limited")
	fmt.Printf("%-14s %-5s %-10s %-9s %-7s %-12s %s\n",
		"--------------", "-----", "----------", "---------", "-------", "------------", "------------")
	for _, w := range view.Workers {
		cache := "replay"
		if w.Serializable {
			cache = "RESTORE"
		}
		streaming := "no"
		if w.Streaming {
			streaming = "yes"
		}
		limited := ""
		if w.RateLimited {
			limited = "YES"
		}
		fmt.Printf("%-14s %-5s %-10s %-9s %-7s %-12d %s\n",
			w.ID, short[w.CompatKey], w.Status, streaming, cache, w.Outstanding, limited)
	}

	fmt.Printf("\nadmitted sessions %d (high water %d)\n", view.AdmittedSessions, view.HighWaterSessions)

	fmt.Println("\ncompatibility keys — workers sharing one can restore each other's checkpoints")
	for key, name := range short {
		members := []string{}
		for _, w := range view.Workers {
			if w.CompatKey == key {
				members = append(members, w.ID)
			}
		}
		fmt.Printf("  %-4s %v\n", name, members)
	}
	fmt.Printf("\n  'cache: RESTORE' is the only place the warm-checkpoint tier is real.\n")
	fmt.Printf("  Everywhere else a same-model failover still happens — it just recovers\n")
	fmt.Printf("  by replaying audio instead, because those runtimes cannot serialize state.\n")
	return nil
}
