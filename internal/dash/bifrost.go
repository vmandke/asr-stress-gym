package dash

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Bifrost's own view of the fleet, scraped from its Prometheus endpoint.
//
// Worth having for one reason above the others: **`fallback_index`**. It
// is the only direct measure of whether Bifrost is earning its place here.
// `fallback_index=0` means the primary served and Bifrost contributed
// nothing but a hop; `fallback_index>=1` means a provider failed and the
// chain actually saved the request. Without it, "Bifrost provides
// failover" is a claim about configuration rather than an observation.
//
// It is also a SECOND, INDEPENDENT health opinion. Bifrost tracks provider
// liveness itself, on its own traffic, with no knowledge of the gateway's
// router. When the two disagree — the router calls a worker healthy while
// Bifrost cannot reach it, or the reverse — one of them is wrong, and that
// disagreement is the signature of a gray failure. A single view cannot
// produce that signal at all.
//
// Read-only and strictly diagnostic: nothing here feeds a routing
// decision. Within-pool placement is the one thing genuinely delegated to
// Bifrost, and the gateway second-guessing it would take that back.

const bifrostScrapeInterval = 5 * time.Second

// BifrostStats is one scrape, flattened for the dashboard.
type BifrostStats struct {
	Available bool   `json:"available"`
	Error     string `json:"error,omitempty"`
	AtMs      int64  `json:"at_ms"`

	// Requests served at each depth of the fallback chain. Key is the
	// index as a string ("0" = primary). Sparse: absent means never.
	ByFallbackIndex map[string]int64 `json:"by_fallback_index"`
	Errors          map[string]int64 `json:"errors_by_fallback_index"`

	// Per-provider, as Bifrost sees them — not as the router does.
	Providers []BifrostProvider `json:"providers"`

	SavedByFallback int64 `json:"saved_by_fallback"` // successes at index >= 1
	TotalSuccess    int64 `json:"total_success"`
	TotalError      int64 `json:"total_error"`
	ActiveRequests  int64 `json:"active_requests"`
}

type BifrostProvider struct {
	Name      string  `json:"name"`
	Success   int64   `json:"success"`
	Errors    int64   `json:"errors"`
	MeanMs    float64 `json:"mean_ms"`
	Up        bool    `json:"up"`
	HasUpInfo bool    `json:"has_up_info"`
}

// BifrostScraper polls Bifrost's /metrics. Nil-safe throughout: with no
// Bifrost configured the dashboard simply reports it unavailable.
type BifrostScraper struct {
	url string
	hc  *http.Client

	mu   sync.Mutex
	last BifrostStats
}

func NewBifrostScraper(baseURL string) *BifrostScraper {
	if baseURL == "" {
		return nil
	}
	return &BifrostScraper{
		url: strings.TrimSuffix(baseURL, "/") + "/metrics",
		// Short: this is telemetry on a dashboard tick. A hanging scrape
		// must never delay the loop, and a missed sample is invisible.
		hc: &http.Client{Timeout: 3 * time.Second},
	}
}

func (b *BifrostScraper) Run(ctx context.Context) {
	if b == nil {
		return
	}
	t := time.NewTicker(bifrostScrapeInterval)
	defer t.Stop()
	b.scrape(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.scrape(ctx)
		}
	}
}

func (b *BifrostScraper) Stats() BifrostStats {
	if b == nil {
		return BifrostStats{Available: false}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.last
}

// Prometheus exposition is `name{label="v",...} value`. A full parser is
// not warranted for the five series we read, but the label extraction is
// regex-based rather than split-on-comma because label VALUES may contain
// commas — Bifrost emits a dozen empty tenancy labels per sample.
var (
	labelRe  = regexp.MustCompile(`(\w+)="([^"]*)"`)
	metricRe = regexp.MustCompile(`^(\w+)\{([^}]*)\}\s+(.+)$`)
)

func (b *BifrostScraper) scrape(ctx context.Context) {
	stats := BifrostStats{
		AtMs:            nowMs(),
		ByFallbackIndex: map[string]int64{},
		Errors:          map[string]int64{},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.url, nil)
	if err != nil {
		b.store(BifrostStats{AtMs: nowMs(), Error: err.Error()})
		return
	}
	resp, err := b.hc.Do(req)
	if err != nil {
		b.store(BifrostStats{AtMs: nowMs(), Error: err.Error()})
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		b.store(BifrostStats{AtMs: nowMs(), Error: err.Error()})
		return
	}
	stats.Available = true

	type acc struct {
		success, errors  int64
		latSum, latCount float64
		up               bool
		hasUp            bool
	}
	byProvider := map[string]*acc{}
	get := func(name string) *acc {
		if byProvider[name] == nil {
			byProvider[name] = &acc{}
		}
		return byProvider[name]
	}

	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		m := metricRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		name, rawLabels, rawVal := m[1], m[2], strings.TrimSpace(m[3])
		val, err := strconv.ParseFloat(rawVal, 64)
		if err != nil {
			continue
		}
		labels := map[string]string{}
		for _, l := range labelRe.FindAllStringSubmatch(rawLabels, -1) {
			labels[l[1]] = l[2]
		}
		provider, idx := labels["provider"], labels["fallback_index"]

		switch name {
		case "bifrost_success_requests_total":
			n := int64(val)
			stats.TotalSuccess += n
			if idx != "" {
				stats.ByFallbackIndex[idx] += n
				if idx != "0" {
					stats.SavedByFallback += n
				}
			}
			if provider != "" {
				get(provider).success += n
			}
		case "bifrost_error_requests_total":
			n := int64(val)
			stats.TotalError += n
			if idx != "" {
				stats.Errors[idx] += n
			}
			if provider != "" {
				get(provider).errors += n
			}
		case "bifrost_upstream_latency_seconds_sum":
			if provider != "" {
				get(provider).latSum += val
			}
		case "bifrost_upstream_latency_seconds_count":
			if provider != "" {
				get(provider).latCount += val
			}
		case "bifrost_provider_key_up":
			if provider != "" {
				a := get(provider)
				a.up, a.hasUp = val > 0, true
			}
		case "bifrost_active_requests":
			stats.ActiveRequests += int64(val)
		}
	}

	names := make([]string, 0, len(byProvider))
	for n := range byProvider {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		a := byProvider[n]
		p := BifrostProvider{Name: n, Success: a.success, Errors: a.errors, Up: a.up, HasUpInfo: a.hasUp}
		if a.latCount > 0 {
			p.MeanMs = a.latSum / a.latCount * 1000
		}
		stats.Providers = append(stats.Providers, p)
	}
	b.store(stats)
}

func (b *BifrostScraper) store(s BifrostStats) {
	b.mu.Lock()
	b.last = s
	b.mu.Unlock()
}
