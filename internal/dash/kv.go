package dash

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
)

// The KV inspection endpoint: what is actually in the cache, and what the
// shared tier is currently holding.
//
// Everything here is READ BACK from the fleet rather than described by
// this package. The tensor list comes from the worker's /v1/kv/layout,
// which derives it from the ONNX graph; the resident versions come from
// the tier's own /keys. A dashboard that described the cache from its own
// idea of it would be exactly the sort of plausible fiction this project
// is built to avoid — it would keep saying "35 tensors, 1.09 MB" after
// someone swapped the model.
//
// Grouped by compatibility-key cohort because that is the real unit: a
// cohort shares a tier, and a blob is loadable by any of its members and
// none outside it.

// KVPool is one compatibility-key cohort and the tier its members share.
// Supplied by cmd/gateway, which closes over the router — dash does not
// import router, and the family->tier mapping has exactly one home
// (the workers' own /health advertisements).
type KVPool struct {
	CompatKey string   `json:"compat_key"`
	Workers   []string `json:"workers"`
	TierURL   string   `json:"-"`
}

// KVPoolsFunc returns the current cohorts.
type KVPoolsFunc func() []KVPool

type kvPoolView struct {
	KVPool
	Layout   json.RawMessage `json:"layout,omitempty"`
	Tier     json.RawMessage `json:"tier_stats,omitempty"`
	Keys     json.RawMessage `json:"keys,omitempty"`
	TierName string          `json:"tier"`
	Error    string          `json:"error,omitempty"`
}

// kvHandler answers GET /api/kv[?session=<id>].
//
// With `session`, each pool additionally reports the versions that session
// currently has resident in that pool's tier — which is how a client can
// watch its own state being published, retired and (on a failover) read
// back by a different worker.
func (c *Control) kvHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if c.KVPools == nil {
		json.NewEncoder(w).Encode(map[string]any{"pools": []any{}, "available": false})
		return
	}
	session := r.URL.Query().Get("session")
	pools := c.KVPools()

	views := make([]kvPoolView, len(pools))
	var wg sync.WaitGroup
	for i, p := range pools {
		wg.Add(1)
		go func(i int, p KVPool) {
			defer wg.Done()
			v := kvPoolView{KVPool: p, TierName: tierName(p.TierURL)}

			// The layout is a property of the model, so any member of the
			// cohort answers identically; ask the first that responds.
			for _, id := range p.Workers {
				t, ok := c.Targets[id]
				if !ok || t.WorkerURL == "" {
					continue
				}
				if raw, err := c.getJSON(t.WorkerURL + "/v1/kv/layout"); err == nil {
					v.Layout = raw
					break
				}
			}
			if p.TierURL != "" {
				if raw, err := c.getJSON(p.TierURL + "/stats"); err == nil {
					v.Tier = raw
				} else {
					v.Error = err.Error()
				}
				if session != "" {
					// "kv:<session>:" is the prefix StatelessClient names a
					// session's versions under (backend.stateRef).
					q := "?limit=40&prefix=" + url.QueryEscape("kv:"+session+":")
					if raw, err := c.getJSON(p.TierURL + "/keys" + q); err == nil {
						v.Keys = raw
					}
				}
			}
			views[i] = v
		}(i, p)
	}
	wg.Wait()

	json.NewEncoder(w).Encode(map[string]any{"available": true, "pools": views})
}

func (c *Control) getJSON(u string) (json.RawMessage, error) {
	resp, err := c.hc.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// The status check is not a formality. Go's default mux answers an
	// unknown route with the plain text "404 page not found", and a JSON
	// decoder reads the leading `404` as a perfectly valid number and
	// stops — so a worker or tier running an older image would silently
	// put the integer 404 where the UI expected an object. Observed
	// exactly that way. Fail loudly instead.
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
		return nil, fmt.Errorf("%s: HTTP %d", u, resp.StatusCode)
	}
	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// tierName is the host part of a tier URL ("kvtier-zip"), which is what a
// reader recognises; the full URL is an implementation detail.
func tierName(u string) string {
	if u == "" {
		return ""
	}
	p, err := url.Parse(u)
	if err != nil {
		return u
	}
	return p.Hostname()
}
