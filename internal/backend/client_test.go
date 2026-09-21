package backend

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Deliberately invalid UTF-8 (0xFF, 0xFE are never valid UTF-8 lead
// bytes): if Restore ever regressed to casting bytes->string straight
// into JSON instead of base64-encoding, encoding/json would silently
// mangle this via lossy replacement, and the fakeWorker handler below
// would catch it.
var wantCheckpointBlob = []byte{0xFF, 0xFE, 0x00, 0x01, 0x80, 'h', 'i'}

// fakeWorker implements just enough of docs/PROTOCOL.md's gateway->backend
// surface to exercise HTTPClient without a real Python worker.
func fakeWorker(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/stream/open", func(w http.ResponseWriter, r *http.Request) {
		var req OpenReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("open: bad request body: %v", err)
		}
		if req.SessionID == "" {
			t.Fatal("open: session_id missing")
		}
		json.NewEncoder(w).Encode(OpenResp{
			Handle:               "handle-1",
			CompatibilityKeyHash: "sha256:mock",
			Capabilities:         Capabilities{Streaming: true, Serializable: true, Modes: []string{"online", "offline"}},
			Generation:           0,
		})
	})

	mux.HandleFunc("/v1/stream/push", func(w http.ResponseWriter, r *http.Request) {
		// Binary body, metadata in headers — implementation-plan.md defect #5.
		if ct := r.Header.Get("Content-Type"); ct != "application/octet-stream" {
			t.Fatalf("push: Content-Type = %q, want application/octet-stream", ct)
		}
		if h := r.Header.Get("X-Handle"); h != "handle-1" {
			t.Fatalf("push: X-Handle = %q, want handle-1", h)
		}
		if s := r.Header.Get("X-Seq-Start"); s != "0" {
			t.Fatalf("push: X-Seq-Start = %q, want 0", s)
		}
		if s := r.Header.Get("X-Seq-End"); s != "7" {
			t.Fatalf("push: X-Seq-End = %q, want 7", s)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("push: read body: %v", err)
		}
		if len(body) != 4 {
			t.Fatalf("push: body length = %d, want 4 (raw bytes, not base64)", len(body))
		}
		if r.Header.Get("X-Expected-Generation") == "999" {
			w.WriteHeader(http.StatusConflict)
			return
		}
		json.NewEncoder(w).Encode(PushResp{Text: "mock transcript", LastSeqApplied: 7, Generation: 1})
	})

	mux.HandleFunc("/v1/stream/flush", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(FlushResp{Text: "final text", Final: true})
	})

	mux.HandleFunc("/v1/stream/restore", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			CheckpointBlob string `json:"checkpoint_blob"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("restore: bad request body: %v", err)
		}
		decoded, err := base64.StdEncoding.DecodeString(body.CheckpointBlob)
		if err != nil {
			t.Fatalf("restore: checkpoint_blob is not valid base64: %v", err)
		}
		if string(decoded) != string(wantCheckpointBlob) {
			t.Fatalf("restore: decoded blob = %q, want %q", decoded, wantCheckpointBlob)
		}
		w.WriteHeader(http.StatusNotImplemented) // mock adapter aside, default: not supported
	})

	mux.HandleFunc("/v1/stream/close", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/v1/stream/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Handle string `json:"handle"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if body.Handle != "handle-1" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"checkpoint_blob":  base64.StdEncoding.EncodeToString(wantCheckpointBlob),
			"generation":       3,
			"last_seq_applied": 42,
		})
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(WorkerAdvert{
			WorkerID:             "worker-mock",
			Status:               "READY",
			CompatibilityKeyHash: "sha256:mock",
			Capabilities:         Capabilities{Streaming: true, Serializable: true, Modes: []string{"online", "offline"}},
			KVTier:               &KVTierAdvert{Enabled: true, URL: "http://tier:9500", LocalHits: 8, TierHits: 2, Misses: 1},
		})
	})

	return httptest.NewServer(mux)
}

func TestHTTPClientOpen(t *testing.T) {
	srv := fakeWorker(t)
	defer srv.Close()
	c := NewHTTPClient(srv.URL)

	resp, err := c.Open(context.Background(), OpenReq{SessionID: "s1", SampleRateHz: 16000, Mode: "online"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if resp.Handle != "handle-1" {
		t.Fatalf("got handle %q, want handle-1", resp.Handle)
	}
	if !resp.Capabilities.Streaming {
		t.Fatal("expected Capabilities.Streaming = true")
	}
}

func TestHTTPClientPushSendsBinaryBodyAndHeaders(t *testing.T) {
	srv := fakeWorker(t)
	defer srv.Close()
	c := NewHTTPClient(srv.URL)

	resp, err := c.Push(context.Background(), PushReq{
		Handle: "handle-1", SeqStart: 0, SeqEnd: 7, ExpectedGeneration: 0,
		Audio: []byte{0x01, 0x02, 0x03, 0x04},
	})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if resp.LastSeqApplied != 7 || resp.Generation != 1 {
		t.Fatalf("got %+v", resp)
	}
}

func TestHTTPClientPushStaleGeneration(t *testing.T) {
	srv := fakeWorker(t)
	defer srv.Close()
	c := NewHTTPClient(srv.URL)

	_, err := c.Push(context.Background(), PushReq{
		Handle: "handle-1", SeqStart: 0, SeqEnd: 7, ExpectedGeneration: 999,
		Audio: []byte{0x01, 0x02, 0x03, 0x04},
	})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("got %v, want ErrStaleGeneration", err)
	}
}

func TestHTTPClientFlush(t *testing.T) {
	srv := fakeWorker(t)
	defer srv.Close()
	c := NewHTTPClient(srv.URL)

	resp, err := c.Flush(context.Background(), "handle-1")
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !resp.Final || resp.Text != "final text" {
		t.Fatalf("got %+v", resp)
	}
}

func TestHTTPClientRestoreNotSupported(t *testing.T) {
	srv := fakeWorker(t)
	defer srv.Close()
	c := NewHTTPClient(srv.URL)

	_, err := c.Restore(context.Background(), RestoreReq{CheckpointBlob: wantCheckpointBlob})
	if !errors.Is(err, ErrNotSupported) {
		t.Fatalf("got %v, want ErrNotSupported", err)
	}
}

func TestHTTPClientCloseAndHealth(t *testing.T) {
	srv := fakeWorker(t)
	defer srv.Close()
	c := NewHTTPClient(srv.URL)

	if err := c.Close(context.Background(), "handle-1"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	adv, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if adv.WorkerID != "worker-mock" || adv.Status != "READY" {
		t.Fatalf("got %+v", adv)
	}
	// M3: /health must carry Capabilities too — the router filters and
	// scores from a single startup Health() call, before it ever Opens a
	// session against a candidate.
	if !adv.Capabilities.Streaming || !adv.Capabilities.Serializable {
		t.Fatalf("got Capabilities %+v, want streaming+serializable", adv.Capabilities)
	}
	if adv.CompatibilityKeyHash != "sha256:mock" {
		t.Fatalf("got CompatibilityKeyHash %q, want sha256:mock", adv.CompatibilityKeyHash)
	}
	if adv.KVTier == nil || adv.KVTier.LocalHits != 8 || adv.KVTier.TierHits != 2 || adv.KVTier.Misses != 1 {
		t.Fatalf("got KVTier %+v, want locality counters", adv.KVTier)
	}
}

func TestHTTPClientCheckpoint(t *testing.T) {
	srv := fakeWorker(t)
	defer srv.Close()
	c := NewHTTPClient(srv.URL)

	resp, err := c.Checkpoint(context.Background(), "handle-1")
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if string(resp.CheckpointBlob) != string(wantCheckpointBlob) {
		t.Fatalf("got blob %q, want %q", resp.CheckpointBlob, wantCheckpointBlob)
	}
	if resp.Generation != 3 || resp.LastSeqApplied != 42 {
		t.Fatalf("got %+v", resp)
	}
}

func TestHTTPClientCheckpointUnknownHandle(t *testing.T) {
	srv := fakeWorker(t)
	defer srv.Close()
	c := NewHTTPClient(srv.URL)

	_, err := c.Checkpoint(context.Background(), "no-such-handle")
	if err == nil {
		t.Fatal("expected an error for an unknown handle")
	}
}

// --- M7: rate limiting ---

func TestParseRetryAfter(t *testing.T) {
	cases := []struct {
		header string
		want   time.Duration
	}{
		{"5", 5 * time.Second},
		{" 2 ", 2 * time.Second}, // whitespace from a hand-rolled header
		{"0", 0},
		// Every unparseable form falls back to a second rather than to
		// zero. Zero would mean "retry immediately", turning the one
		// response designed to STOP a storm into the thing that causes
		// one — so this is a correctness fallback, not tidiness.
		{"", time.Second},
		{"Wed, 21 Oct 2015 07:28:00 GMT", time.Second}, // the HTTP-date form, deliberately unhandled
		{"-3", time.Second},
		{"banana", time.Second},
	}
	for _, c := range cases {
		if got := parseRetryAfter(c.header); got != c.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", c.header, got, c.want)
		}
	}
}

func TestPushReturnsRateLimitErrorOn429(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate_limited"}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	_, err := c.Push(context.Background(), PushReq{Handle: "h1", SeqStart: 1, SeqEnd: 2, Audio: []byte{0, 0}})

	// errors.Is is what the online path actually uses — it only needs to
	// know "not this backend, right now".
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("got %v, want it to match ErrRateLimited", err)
	}
	// errors.As is what the router needs, to honour the worker's own
	// Retry-After rather than guessing a backoff.
	var rl *RateLimitError
	if !errors.As(err, &rl) {
		t.Fatalf("got %v, want a *RateLimitError", err)
	}
	if rl.RetryAfter != 3*time.Second {
		t.Errorf("RetryAfter = %v, want 3s from the header", rl.RetryAfter)
	}
}

func TestOpenReturnsRateLimitErrorOn429(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	_, err := c.Open(context.Background(), OpenReq{SessionID: "s1", SampleRateHz: 16000, Mode: "online"})
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("got %v, want ErrRateLimited — a new session refused for rate must be distinguishable from a broken worker", err)
	}
}

func TestRateLimitErrorIsNotConfusedWithStaleGeneration(t *testing.T) {
	// Both are non-fatal, both come back from Push, and the gateway does
	// very different things with them: 409 is a coordinator bug worth an
	// error event, 429 is backpressure worth a failover.
	rl := &RateLimitError{RetryAfter: time.Second}
	if errors.Is(rl, ErrStaleGeneration) {
		t.Error("a rate-limit error must not match ErrStaleGeneration")
	}
}
