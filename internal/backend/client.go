// Package backend is the Client interface the coordinator calls across
// the network: open/push/flush/restore/close/health, matching
// docs/PROTOCOL.md exactly. It is the only place HTTP to a worker
// happens, and the only place a raw PCM byte slice is treated as an
// opaque payload rather than something to decode — it never inspects
// audio content, only carries it.
package backend

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrStaleGeneration is returned when a Push's expected_generation no
	// longer matches the worker's state — compare-and-commit rejected the
	// write rather than silently applying it over newer state.
	ErrStaleGeneration = errors.New("backend: stale generation")
	// ErrNotSupported is returned by Restore when the adapter's
	// Capabilities.Serializable is false (docs/DECISIONS.md: true only on
	// the mock adapter).
	ErrNotSupported = errors.New("backend: checkpoint restore not supported by this adapter")
	// ErrRateLimited is returned when a worker answers 429. Callers on the
	// online path must NOT sleep on it — see RateLimitError and
	// docs/build-plan.md: "A 500ms backoff is catastrophic against a 200ms
	// budget. Move to another backend."
	ErrRateLimited = errors.New("backend: rate limited")
)

// RateLimitError carries the worker's own Retry-After alongside
// ErrRateLimited, so the router can zero that worker's bucket for exactly
// as long as the worker askedrather than guessing a backoff.
//
// It wraps ErrRateLimited so `errors.Is(err, ErrRateLimited)` works for
// callers that only need to know "not this backend, right now" — which is
// every caller on the online path.
type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("backend: rate limited, retry after %s", e.RetryAfter)
}

func (e *RateLimitError) Unwrap() error { return ErrRateLimited }

// parseRetryAfter reads the header in its delta-seconds form, which is
// what a worker under load realistically sends. The HTTP-date form is
// deliberately not handled: it would need clock-skew reasoning between
// two containers to gain nothing, and an unparseable value falls back to
// a short default rather than to zero — zero would mean "retry
// immediately", turning the one response designed to stop a storm into
// the thing that causes one.
func parseRetryAfter(h string) time.Duration {
	const fallback = time.Second
	if h == "" {
		return fallback
	}
	secs, err := strconv.Atoi(strings.TrimSpace(h))
	if err != nil || secs < 0 {
		return fallback
	}
	return time.Duration(secs) * time.Second
}

// Capabilities mirrors worker/adapters/base.py's Capabilities dataclass.
// A router (M3+) filters candidates on this before it ever scores one —
// see implementation-plan.md "Capability-aware routing".
type Capabilities struct {
	Streaming    bool     `json:"streaming"`
	Serializable bool     `json:"serializable"`
	Endpointing  bool     `json:"endpointing"`
	Modes        []string `json:"modes"`
	MinChunkMs   int      `json:"min_chunk_ms"`
	MaxChunkMs   int      `json:"max_chunk_ms"`
}

type OpenReq struct {
	SessionID    string `json:"session_id"`
	SampleRateHz int    `json:"sample_rate_hz"`
	Mode         string `json:"mode"`
}

type OpenResp struct {
	Handle               string       `json:"handle"`
	CompatibilityKeyHash string       `json:"compatibility_key_hash"`
	Capabilities         Capabilities `json:"capabilities"`
	Generation           uint64       `json:"generation"`
}

// PushReq's Audio is an opaque byte slice — this package carries it, it
// never interprets it. See internal/audio for where chunk bytes actually
// come from.
type PushReq struct {
	Handle             string
	SeqStart           uint64
	SeqEnd             uint64
	ExpectedGeneration uint64
	Audio              []byte
}

type PushResp struct {
	Text           string `json:"text"`
	LastSeqApplied uint64 `json:"last_seq_applied"`
	Generation     uint64 `json:"generation"`
}

type FlushResp struct {
	Text  string `json:"text"`
	Final bool   `json:"final"`
}

type RestoreReq struct {
	CheckpointBlob []byte
	// LastSeqApplied is read back from the CheckpointResp that produced
	// CheckpointBlob (see Checkpoint) and threaded through so the fresh
	// handle this creates correctly reflects how much audio the restored
	// state already accounts for — without it, tail replay would start
	// from 0 and redundantly (if harmlessly, thanks to idempotent
	// replay) resend audio the checkpoint already covers.
	LastSeqApplied uint64
}

type RestoreResp struct {
	Handle         string `json:"handle"`
	Generation     uint64 `json:"generation"`
	LastSeqApplied uint64 `json:"last_seq_applied"`
}

// KVTierAdvert is the worker's half of the shared-KV-tier contract: where
// it reads and publishes session state. See cmd/kvtier and
// docs/KVCACHE.md.
type KVTierAdvert struct {
	Enabled   bool   `json:"enabled"`
	URL       string `json:"url"`
	LocalHits int64  `json:"local_hits"`
	TierHits  int64  `json:"tier_hits"`
	Misses    int64  `json:"misses"`
}

// WorkerAdvert matches build-plan.md's "Worker advertisement" JSON,
// already served at M0's GET /health — plus Capabilities (M3), an
// additive extension: the router (internal/router) must filter and score
// candidates from a single startup Health() call, before it ever Opens a
// session against one, so everything Pick needs to decide has to be here.
type WorkerAdvert struct {
	WorkerID             string       `json:"worker_id"`
	Status               string       `json:"status"`
	Model                string       `json:"model"`
	CompatibilityKeyHash string       `json:"compatibility_key_hash"`
	Capabilities         Capabilities `json:"capabilities"`
	ActiveSessions       int          `json:"active_sessions"`
	StateBytes           int64        `json:"state_bytes"`
	QueueDepth           int          `json:"queue_depth"`
	RTFP50               *float64     `json:"rtf_p50"`
	LastHeartbeatMs      int64        `json:"last_heartbeat_ms"`

	// M9 resource telemetry, for the dashboard's per-node graphs
	// (internal/dash). Pointers because "not measured on this platform"
	// and "measured as zero" are different facts and the dashboard draws
	// them differently — a gap in the line versus a line at the floor.
	// resources.py returns None off Linux, where /proc does not exist.
	//
	// Nothing in the ROUTER reads these. Selection policy stays a
	// function of health, latency, rate budget and capability
	// (internal/router); adding memory pressure to it is a real design
	// question with its own failure modes (a worker near its limit being
	// starved of the traffic that would let it finish and free state),
	// not a free upgrade because the number happens to be available now.
	// Which shared KV tier this worker publishes its state to, advertised
	// rather than configured gateway-side for the same reason the
	// compatibility key is: there must be no second copy of the
	// family->tier mapping that could drift from the fleet's own. Each
	// model family has its own tier (docker-compose.yml, x-kvtier).
	// Absent or disabled means this worker serves only the pinned path.
	KVTier *KVTierAdvert `json:"kv_tier"`

	Inflight               int      `json:"inflight"`
	Running                int      `json:"running"`
	InflightHighWater      int      `json:"inflight_high_water"`
	UptimeS                float64  `json:"uptime_s"`
	RSSBytes               *int64   `json:"rss_bytes"`
	CgroupMemoryBytes      *int64   `json:"cgroup_memory_bytes"`
	CgroupMemoryLimitBytes *int64   `json:"cgroup_memory_limit_bytes"`
	CPUPercent             *float64 `json:"cpu_percent"`
}

// CheckpointResp is the worker's answer to "give me your current state to
// checkpoint". Note the asymmetry with Restore: the CALLER here is the
// gateway asking the CURRENTLY-PINNED worker for a blob it will hold onto
// itself — checkpoints live gateway-side (internal/coord), the same place
// the audio journal does, and for the same reason (docs/build-plan.md's
// topology section: a worker's state is rebuildable from the journal, but
// the journal — and now the checkpoint — has to survive that worker's
// death, so neither can live only in that worker's own memory).
type CheckpointResp struct {
	CheckpointBlob []byte
	Generation     uint64
	LastSeqApplied uint64
}

// Client is the coordinator's entire view of a backend. There is no
// Replay (replay is Push over a span the worker may already have
// consumed — last_seq_applied makes that safe) and no CreateState (that
// is Open) — see implementation-plan.md defect #1.
type Client interface {
	Open(ctx context.Context, r OpenReq) (OpenResp, error)
	Push(ctx context.Context, r PushReq) (PushResp, error)
	Flush(ctx context.Context, handle string) (FlushResp, error)
	Restore(ctx context.Context, r RestoreReq) (RestoreResp, error)
	Checkpoint(ctx context.Context, handle string) (CheckpointResp, error)
	Close(ctx context.Context, handle string) error
	Health(ctx context.Context) (WorkerAdvert, error)
}

type HTTPClient struct {
	baseURL string
	hc      *http.Client
}

func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{baseURL: baseURL, hc: &http.Client{Timeout: 5 * time.Second}}
}

func (c *HTTPClient) postJSON(ctx context.Context, path string, reqBody, respBody any) (*http.Response, error) {
	var buf bytes.Buffer
	if reqBody != nil {
		if err := json.NewEncoder(&buf).Encode(reqBody); err != nil {
			return nil, fmt.Errorf("backend: encode request: %w", err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("backend: %s: %w", path, err)
	}
	if respBody != nil && resp.StatusCode < 300 {
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
			return resp, fmt.Errorf("backend: %s: decode response: %w", path, err)
		}
	}
	return resp, nil
}

func (c *HTTPClient) Open(ctx context.Context, r OpenReq) (OpenResp, error) {
	var out OpenResp
	resp, err := c.postJSON(ctx, "/v1/stream/open", r, &out)
	if err != nil {
		return OpenResp{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		// A new session refused for rate. openWithRetry treats this like
		// any other Open failure — try a different worker — which is
		// exactly right: there is no state to preserve yet, so moving is
		// free.
		return OpenResp{}, &RateLimitError{RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
	}
	if resp.StatusCode != http.StatusOK {
		return OpenResp{}, fmt.Errorf("backend: open: unexpected status %d", resp.StatusCode)
	}
	return out, nil
}

// Push sends a binary body, not base64 — see docs/PROTOCOL.md and
// implementation-plan.md defect #5. Metadata travels in headers because
// the body is pure opaque audio bytes.
func (c *HTTPClient) Push(ctx context.Context, r PushReq) (PushResp, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/stream/push", bytes.NewReader(r.Audio))
	if err != nil {
		return PushResp{}, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Handle", r.Handle)
	req.Header.Set("X-Seq-Start", fmt.Sprintf("%d", r.SeqStart))
	req.Header.Set("X-Seq-End", fmt.Sprintf("%d", r.SeqEnd))
	req.Header.Set("X-Expected-Generation", fmt.Sprintf("%d", r.ExpectedGeneration))

	resp, err := c.hc.Do(req)
	if err != nil {
		return PushResp{}, fmt.Errorf("backend: push: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return PushResp{}, ErrStaleGeneration
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return PushResp{}, &RateLimitError{RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return PushResp{}, fmt.Errorf("backend: push: unexpected status %d: %s", resp.StatusCode, body)
	}
	var out PushResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return PushResp{}, fmt.Errorf("backend: push: decode response: %w", err)
	}
	return out, nil
}

func (c *HTTPClient) Flush(ctx context.Context, handle string) (FlushResp, error) {
	var out FlushResp
	resp, err := c.postJSON(ctx, "/v1/stream/flush", map[string]string{"handle": handle}, &out)
	if err != nil {
		return FlushResp{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return FlushResp{}, fmt.Errorf("backend: flush: unexpected status %d", resp.StatusCode)
	}
	return out, nil
}

func (c *HTTPClient) Restore(ctx context.Context, r RestoreReq) (RestoreResp, error) {
	var out RestoreResp
	// Base64, unlike Push: this is a binary blob riding inside a JSON
	// string field, so it must be text-safe. Infrequent (M3+ failover
	// only), never the hot path — unlike Push's raw binary body, where
	// the same argument would cost 33% on every one of ~50 msg/sec.
	body := map[string]any{
		"checkpoint_blob":  base64.StdEncoding.EncodeToString(r.CheckpointBlob),
		"last_seq_applied": r.LastSeqApplied,
	}
	resp, err := c.postJSON(ctx, "/v1/stream/restore", body, &out)
	if err != nil {
		return RestoreResp{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotImplemented {
		return RestoreResp{}, ErrNotSupported
	}
	if resp.StatusCode != http.StatusOK {
		return RestoreResp{}, fmt.Errorf("backend: restore: unexpected status %d", resp.StatusCode)
	}
	return out, nil
}

// Checkpoint asks the currently-pinned worker to serialize its own state
// for handle. 501/ErrNotSupported when Capabilities.Serializable is
// false — the caller (internal/coord's async checkpoint loop) treats that
// as "nothing to store", not an error worth logging on every push
// (invariant 13: checkpoint failure must never take down healthy
// inference, and "not supported" isn't even a failure, just a fact about
// this adapter).
func (c *HTTPClient) Checkpoint(ctx context.Context, handle string) (CheckpointResp, error) {
	var out struct {
		CheckpointBlob string `json:"checkpoint_blob"`
		Generation     uint64 `json:"generation"`
		LastSeqApplied uint64 `json:"last_seq_applied"`
	}
	resp, err := c.postJSON(ctx, "/v1/stream/checkpoint", map[string]string{"handle": handle}, &out)
	if err != nil {
		return CheckpointResp{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotImplemented {
		return CheckpointResp{}, ErrNotSupported
	}
	if resp.StatusCode != http.StatusOK {
		return CheckpointResp{}, fmt.Errorf("backend: checkpoint: unexpected status %d", resp.StatusCode)
	}
	blob, err := base64.StdEncoding.DecodeString(out.CheckpointBlob)
	if err != nil {
		return CheckpointResp{}, fmt.Errorf("backend: checkpoint: decode blob: %w", err)
	}
	return CheckpointResp{CheckpointBlob: blob, Generation: out.Generation, LastSeqApplied: out.LastSeqApplied}, nil
}

func (c *HTTPClient) Close(ctx context.Context, handle string) error {
	resp, err := c.postJSON(ctx, "/v1/stream/close", map[string]string{"handle": handle}, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("backend: close: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) Health(ctx context.Context) (WorkerAdvert, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return WorkerAdvert{}, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return WorkerAdvert{}, fmt.Errorf("backend: health: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return WorkerAdvert{}, fmt.Errorf("backend: health: unexpected status %d", resp.StatusCode)
	}
	var out WorkerAdvert
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return WorkerAdvert{}, fmt.Errorf("backend: health: decode response: %w", err)
	}
	return out, nil
}

var _ Client = (*HTTPClient)(nil)
