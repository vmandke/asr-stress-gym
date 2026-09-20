package backend

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// capture records what a fake Bifrost actually received, so the assertions
// are about the bytes on the wire rather than about the client's intent.
type capture struct {
	calls  atomic.Int64
	fields map[string][]string
	body   string
}

func fakeBifrost(t *testing.T, status int, respBody string, cap *capture) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.calls.Add(1)
		if err := r.ParseMultipartForm(8 << 20); err != nil {
			t.Errorf("not parseable as multipart: %v", err)
		}
		cap.fields = r.MultipartForm.Value
		w.WriteHeader(status)
		w.Write([]byte(respBody))
	}))
}

func newTestClient(url string) *StatelessClient {
	return NewStatelessClient(url, "", "worker-zip-1/alias",
		[]string{"worker-zip-2/alias"}, WorkerAdvert{}, 5*time.Second)
}

func TestFirstPushCarriesASinkButNoRef(t *testing.T) {
	cap := &capture{}
	srv := fakeBifrost(t, 200, `{"text":"hello"}`, cap)
	defer srv.Close()
	c := newTestClient(srv.URL)

	resp, err := c.Push(context.Background(), PushReq{Handle: "kv:s1", SeqEnd: 100, Audio: []byte("pcm")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "hello" || resp.LastSeqApplied != 100 {
		t.Errorf("resp = %+v", resp)
	}
	if got := cap.fields["state_ref"]; len(got) != 0 {
		t.Errorf("state_ref = %v, want absent — there is no predecessor to read", got)
	}
	if got := cap.fields["state_sink"]; len(got) != 1 || got[0] != "kv:s1:100" {
		t.Errorf("state_sink = %v, want [kv:s1:100]", got)
	}
	if got := cap.fields["kv_mode"]; len(got) != 1 || got[0] != "stream" {
		t.Errorf("kv_mode = %v, want [stream]", got)
	}
}

// Versions must be immutable: chunk N reads N-1 and writes N, never
// overwriting its own input. That is what makes a retry on a peer safe.
func TestSecondPushReadsThePredecessorAndWritesANewVersion(t *testing.T) {
	cap := &capture{}
	srv := fakeBifrost(t, 200, `{"text":"hello there"}`, cap)
	defer srv.Close()
	c := newTestClient(srv.URL)

	ctx := context.Background()
	if _, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 100, Audio: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 200, Audio: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if got := cap.fields["state_ref"]; len(got) != 1 || got[0] != "kv:s1:100" {
		t.Errorf("state_ref = %v, want [kv:s1:100]", got)
	}
	if got := cap.fields["state_sink"]; len(got) != 1 || got[0] != "kv:s1:200" {
		t.Errorf("state_sink = %v, want [kv:s1:200] — a new version, not the one just read", got)
	}
}

// Recovery replays freely, so a push at or below what is already applied
// has to be a cheap no-op — not a re-infer, and not a second tier write.
func TestReplayAtOrBelowAppliedMakesNoCall(t *testing.T) {
	cap := &capture{}
	srv := fakeBifrost(t, 200, `{"text":"first"}`, cap)
	defer srv.Close()
	c := newTestClient(srv.URL)

	ctx := context.Background()
	if _, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 100, Audio: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 100, Audio: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	if got := cap.calls.Load(); got != 1 {
		t.Errorf("made %d calls, want 1 — the replay should not have reached the network", got)
	}
	if resp.Text != "first" || resp.LastSeqApplied != 100 {
		t.Errorf("replay resp = %+v, want the remembered answer", resp)
	}
}

func TestTheWholeFallbackChainRidesWithEveryRequest(t *testing.T) {
	cap := &capture{}
	srv := fakeBifrost(t, 200, `{"text":"x"}`, cap)
	defer srv.Close()
	c := NewStatelessClient(srv.URL, "", "w1/a", []string{"w2/a", "w3/a"}, WorkerAdvert{}, time.Second)

	if _, err := c.Push(context.Background(), PushReq{Handle: "h", SeqEnd: 1, Audio: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if got := cap.fields["model"]; len(got) != 1 || got[0] != "w1/a" {
		t.Errorf("model = %v, want [w1/a] — the primary is led with every time, which is what\n"+
			"keeps the previous chunk's state local in the common case", got)
	}
	if got := cap.fields["fallbacks"]; len(got) != 2 || got[0] != "w2/a" || got[1] != "w3/a" {
		t.Errorf("fallbacks = %v, want [w2/a w3/a] in order", got)
	}
}

// A 424 means the state reference resolved nowhere on any member of the
// chain. It must surface as an error so coord replays audio, NOT be
// swallowed into a transcript produced against fresh state.
func TestMissingStateReferenceIsAnError(t *testing.T) {
	cap := &capture{}
	srv := fakeBifrost(t, http.StatusFailedDependency, `{"error":"state_ref_miss"}`, cap)
	defer srv.Close()
	c := newTestClient(srv.URL)

	_, err := c.Push(context.Background(), PushReq{Handle: "h", SeqEnd: 1, Audio: []byte("a")})
	if err == nil {
		t.Fatal("expected an error, got a transcript built on state that was never loaded")
	}
	if !strings.Contains(err.Error(), "state reference missing") {
		t.Errorf("err = %v, want it to name the cause", err)
	}
}

func TestRateLimitIsTypedSoTheRouterCanZeroTheBucket(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.calls.Add(1)
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c := newTestClient(srv.URL)

	_, err := c.Push(context.Background(), PushReq{Handle: "h", SeqEnd: 1, Audio: []byte("a")})
	var rl *RateLimitError
	if !errors.As(err, &rl) {
		t.Fatalf("err = %v, want a *RateLimitError so dispatchChunk can call On429", err)
	}
	if rl.RetryAfter != 2*time.Second {
		t.Errorf("RetryAfter = %v, want 2s", rl.RetryAfter)
	}
}

// Not a gap: state already lives outside every worker, so a gateway-side
// checkpoint would be a third copy of something already durable. coord
// reads ErrNotSupported as "no checkpoint" and degrades to replay.
func TestCheckpointAndRestoreAreUnsupportedByDesign(t *testing.T) {
	c := newTestClient("http://unused")
	if _, err := c.Checkpoint(context.Background(), "h"); !errors.Is(err, ErrNotSupported) {
		t.Errorf("Checkpoint err = %v, want ErrNotSupported", err)
	}
	if _, err := c.Restore(context.Background(), RestoreReq{}); !errors.Is(err, ErrNotSupported) {
		t.Errorf("Restore err = %v, want ErrNotSupported", err)
	}
}

func TestFlushFinalizesAgainstTheLatestVersionWithoutAppendingAudio(t *testing.T) {
	cap := &capture{}
	srv := fakeBifrost(t, 200, `{"text":"the final"}`, cap)
	defer srv.Close()
	c := newTestClient(srv.URL)

	ctx := context.Background()
	if _, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 300, Audio: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Flush(ctx, "kv:s1")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "the final" || !resp.Final {
		t.Errorf("resp = %+v", resp)
	}
	if got := cap.fields["kv_mode"]; len(got) != 1 || got[0] != "final" {
		t.Errorf("kv_mode = %v, want [final]", got)
	}
	if got := cap.fields["state_ref"]; len(got) != 1 || got[0] != "kv:s1:300" {
		t.Errorf("state_ref = %v, want [kv:s1:300]", got)
	}
	if got := cap.fields["state_sink"]; len(got) != 0 {
		t.Errorf("state_sink = %v, want absent — a final publishes no successor", got)
	}
}

// Immutable versions must not mean versions kept forever. A session writes
// one per chunk, so at a 160ms cadence a 60s session produces ~375 of them.
// Measured before this existed: 30 streams for 60s wrote 6,528 versions
// into a 256 MB tier and evicted 5,682 (87%), costing 12 sessions their
// state. Per-session residency has to be bounded by policy, not by the LRU
// discovering the problem.
func TestASuccessfulPushRetiresItsPredecessor(t *testing.T) {
	cap := &capture{}
	bf := fakeBifrost(t, 200, `{"text":"x"}`, cap)
	defer bf.Close()

	deleted := make(chan string, 4)
	tier := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted <- r.URL.Path
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer tier.Close()

	c := NewStatelessClient(bf.URL, tier.URL, "w/a", nil, WorkerAdvert{}, 2*time.Second)
	ctx := context.Background()
	if _, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 100, Audio: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	// Nothing to retire yet: version 100 is the only one that exists.
	select {
	case got := <-deleted:
		t.Fatalf("retired %q after the FIRST push; it has no predecessor", got)
	case <-time.After(150 * time.Millisecond):
	}

	if _, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 200, Audio: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-deleted:
		if !strings.Contains(got, "kv:s1:100") {
			t.Errorf("retired %q, want the predecessor kv:s1:100", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the predecessor was never retired — versions accumulate until the LRU evicts them")
	}
}

// Retirement is best-effort bookkeeping: a tier that cannot be reached
// costs memory the TTL reclaims, and must never fail a chunk that has
// already been served.
func TestRetirementFailureDoesNotFailThePush(t *testing.T) {
	cap := &capture{}
	bf := fakeBifrost(t, 200, `{"text":"x"}`, cap)
	defer bf.Close()

	c := NewStatelessClient(bf.URL, "http://127.0.0.1:1", "w/a", nil, WorkerAdvert{}, time.Second)
	ctx := context.Background()
	if _, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 100, Audio: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 200, Audio: []byte("b")})
	if err != nil {
		t.Fatalf("push failed because its GC could not reach the tier: %v", err)
	}
	if resp.LastSeqApplied != 200 {
		t.Errorf("LastSeqApplied = %d, want 200", resp.LastSeqApplied)
	}
}

// Open is called again at every VAD endpoint — finalizeEndpoint does
// Close, then Open, so a new utterance cannot inherit the previous one's
// state. Close deletes every version in the tier, so if Open did not also
// reset the version counter the next utterance's first push would
// reference a version that no longer exists and take a 424. That is a
// failure on every multi-utterance session, and it is invisible in a
// single-utterance test.
func TestOpenResetsTheVersionCounterSoANewUtteranceStartsClean(t *testing.T) {
	cap := &capture{}
	bf := fakeBifrost(t, 200, `{"text":"x"}`, cap)
	defer bf.Close()
	c := NewStatelessClient(bf.URL, "", "w/a", nil, WorkerAdvert{}, time.Second)
	ctx := context.Background()

	if _, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 500, Audio: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	// The endpoint boundary: Close wipes the tier, Open begins again.
	if err := c.Close(ctx, "kv:s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(ctx, OpenReq{SessionID: "s1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Push(ctx, PushReq{Handle: "kv:s1", SeqEnd: 600, Audio: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if got := cap.fields["state_ref"]; len(got) != 0 {
		t.Errorf("state_ref = %v, want absent — it points at a version Close deleted", got)
	}
	if got := cap.fields["state_sink"]; len(got) != 1 || got[0] != "kv:s1:600" {
		t.Errorf("state_sink = %v, want [kv:s1:600]", got)
	}
}

func TestCloseReclaimsEveryVersionOfTheSession(t *testing.T) {
	var gotMethod, gotQuery string
	tier := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotQuery = r.Method, r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	}))
	defer tier.Close()
	c := NewStatelessClient("http://unused", tier.URL, "w/a", nil, WorkerAdvert{}, time.Second)

	if err := c.Close(context.Background(), "kv:s1"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("method = %s, want DELETE", gotMethod)
	}
	if !strings.Contains(gotQuery, "prefix=kv%3As1%3A") && !strings.Contains(gotQuery, "prefix=kv:s1:") {
		t.Errorf("query = %q, want a prefix delete scoped to this session", gotQuery)
	}
}

// A tier that cannot be reached at teardown costs memory the TTL reclaims.
// It must never fail a session that has already produced its final.
func TestCloseIsBestEffort(t *testing.T) {
	c := NewStatelessClient("http://unused", "http://127.0.0.1:1", "w/a", nil, WorkerAdvert{}, 200*time.Millisecond)
	if err := c.Close(context.Background(), "kv:s1"); err != nil {
		t.Errorf("Close err = %v, want nil — teardown must not fail a completed session", err)
	}
}
