package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// StatelessClient speaks the shared-KV-tier streaming path: it implements
// the same Client interface as HTTPClient, but addresses a MODEL FAMILY
// through Bifrost rather than one worker directly.
//
// **Why this is a Client and not a new code path.** Everything the session
// loop does around a push — failover, the journal, rate-limit handling,
// latency reporting, the dashboard — is written against this interface.
// Implementing it means the stateless path inherits all of that unchanged,
// and the two paths stay honestly comparable because only the transport
// differs. cmd/gateway swaps one for the other at session start; nothing
// downstream knows which it has.
//
// **What changes underneath.** HTTPClient holds a base URL for one worker
// and relies on that worker holding the session's inference state. This
// holds a Bifrost provider chain for a whole compatibility-key cohort, and
// relies on no worker holding anything: each push names the state it reads
// (state_ref) and the state it writes (state_sink), both living in the
// shared tier (cmd/kvtier). Any member of the cohort can serve any chunk.
//
// **Primary stickiness is deliberate.** The chain always leads with the
// same member, so in the ordinary case the serving worker still has the
// previous chunk's state live in its own hot cache and pays nothing to
// resume — measured at 1.88x faster than fanning out, and zero bytes
// fetched (docs/STATELESS-KVTIER.md). Bifrost moves off the primary only
// when it fails, and the replacement then pays one fetch. That is exactly
// the production posture: affinity as an OPTIMIZATION, not a correctness
// requirement.
//
// **Checkpoint and Restore are deliberately unsupported.** They exist so
// the gateway can hold a copy of state that would otherwise die with its
// worker. Here the state already lives outside every worker, so a
// gateway-side checkpoint would be a third copy of something already
// durable. Returning ErrNotSupported is not a gap: coord's recovery path
// reads it as "no checkpoint available" and degrades to audio replay,
// which is the correct answer when a whole family is gone.
type StatelessClient struct {
	bifrostURL string
	tierURL    string
	primary    string   // "worker-zip-1/alias" — led with on every request
	fallbacks  []string // the rest of the cohort, in order
	advert     WorkerAdvert
	hc         *http.Client

	mu          sync.Mutex
	lastApplied uint64 // the version currently published in the tier
	lastText    string // answer to replay a push at or below lastApplied
}

// NewStatelessClient builds a client over one compatibility-key cohort.
// primary and fallbacks are Bifrost model names ("<provider>/<alias>"),
// which cmd/gateway derives from router.Pool — so the chain cannot cross a
// model family, because membership is key equality.
func NewStatelessClient(bifrostURL, tierURL, primary string, fallbacks []string, advert WorkerAdvert, timeout time.Duration) *StatelessClient {
	return &StatelessClient{
		bifrostURL: bifrostURL,
		tierURL:    tierURL,
		primary:    primary,
		fallbacks:  fallbacks,
		advert:     advert,
		hc:         &http.Client{Timeout: timeout},
	}
}

// stateRef names one immutable version of a session's state. Versions are
// never overwritten — a push reads N and writes M>N — which is what makes
// a retry safe: if a worker dies mid-chunk, the retry finds its input
// intact. See cmd/kvtier/main.go.
func stateRef(handle string, seq uint64) string {
	return fmt.Sprintf("%s:%d", handle, seq)
}

func (c *StatelessClient) Open(ctx context.Context, r OpenReq) (OpenResp, error) {
	// No remote call: there is no worker-side session to create. The
	// handle is just the prefix every one of this session's state versions
	// will be named under.
	//
	// Resetting the version counter is the load-bearing part. Open is
	// called again at every VAD endpoint (finalizeEndpoint: Close, then
	// Open), where it means "begin a new inference context" — the same
	// reason a stateful worker gets a fresh handle there, so a new
	// utterance cannot inherit the previous one's model state or
	// transcript. Without the reset the handle is unchanged, so the next
	// utterance's first push would reference a version that Close had just
	// deleted and take a 424 on every multi-utterance session.
	c.mu.Lock()
	c.lastApplied, c.lastText = 0, ""
	c.mu.Unlock()

	return OpenResp{
		Handle:               "kv:" + r.SessionID,
		CompatibilityKeyHash: c.advert.CompatibilityKeyHash,
		Capabilities:         c.advert.Capabilities,
		Generation:           0,
	}, nil
}

func (c *StatelessClient) Push(ctx context.Context, r PushReq) (PushResp, error) {
	c.mu.Lock()
	applied, text := c.lastApplied, c.lastText
	c.mu.Unlock()

	// Idempotent replay, matching the stateful worker's own rule
	// (docs/PROTOCOL.md "last_seq_applied"): audio at or below what is
	// already applied is a no-op, not a re-infer. Recovery replays freely,
	// so this has to be cheap and side-effect free.
	if r.SeqEnd <= applied {
		return PushResp{Text: text, LastSeqApplied: applied, Generation: r.ExpectedGeneration}, nil
	}

	fields := map[string]string{
		"model":           c.primary,
		"response_format": "json",
		"kv_mode":         "stream",
		"state_sink":      stateRef(r.Handle, r.SeqEnd),
	}
	if applied > 0 {
		fields["state_ref"] = stateRef(r.Handle, applied)
	}

	out, err := c.call(ctx, fields, r.Audio)
	if err != nil {
		return PushResp{}, err
	}

	c.mu.Lock()
	c.lastApplied, c.lastText = r.SeqEnd, out
	c.mu.Unlock()

	// Retire the predecessor now that its successor is durable.
	//
	// Versions are immutable so a retry can always find its input intact,
	// but "immutable" must not mean "kept forever": a session writes one
	// version per chunk, so at a 160ms cadence a single 60s session
	// produces ~375 of them. Measured before this existed — 30 streams for
	// 60s wrote 6,528 versions into a 256 MB tier and evicted 5,682 of
	// them (87%), which cost 12 sessions their state and sent them through
	// audio replay. Per-session residency has to be bounded by the policy,
	// not by the LRU discovering the problem.
	//
	// Retiring `applied` is safe precisely here and not earlier: this push
	// has returned, so no retry of it is outstanding, and every later push
	// reads r.SeqEnd. Best-effort and off the critical path — a delete
	// that fails costs memory the TTL reclaims, and must never fail a
	// chunk that has already been served.
	if applied > 0 {
		go c.retire(stateRef(r.Handle, applied))
	}
	return PushResp{Text: out, LastSeqApplied: r.SeqEnd, Generation: r.ExpectedGeneration}, nil
}

func (c *StatelessClient) retire(ref string) {
	if c.tierURL == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.tierURL+"/kv/"+url.PathEscape(ref), nil)
	if err != nil {
		return
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func (c *StatelessClient) Flush(ctx context.Context, handle string) (FlushResp, error) {
	c.mu.Lock()
	applied := c.lastApplied
	c.mu.Unlock()

	fields := map[string]string{
		"model":           c.primary,
		"response_format": "json",
		"kv_mode":         "final",
	}
	if applied > 0 {
		fields["state_ref"] = stateRef(handle, applied)
	}
	// An empty payload: finalize consumes the state built by previous
	// pushes and must not append audio of its own. emptyWAV is a valid
	// zero-frame container rather than an absent part, so the worker's
	// OpenAI-shaped endpoint parses it by its normal path.
	text, err := c.call(ctx, fields, emptyWAV())
	if err != nil {
		return FlushResp{}, err
	}
	return FlushResp{Text: text, Final: true}, nil
}

// Restore and Checkpoint are unsupported by design — see the type comment.
func (c *StatelessClient) Restore(ctx context.Context, r RestoreReq) (RestoreResp, error) {
	return RestoreResp{}, ErrNotSupported
}

func (c *StatelessClient) Checkpoint(ctx context.Context, handle string) (CheckpointResp, error) {
	return CheckpointResp{}, ErrNotSupported
}

// Close reclaims every version this session published. The tier's TTL is
// the backstop for sessions that end by dying rather than by closing.
func (c *StatelessClient) Close(ctx context.Context, handle string) error {
	if c.tierURL == "" {
		return nil
	}
	url := fmt.Sprintf("%s/kv?prefix=%s:", c.tierURL, handle)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		// Best-effort: a tier that cannot be reached at teardown costs
		// memory that TTL will reclaim, and must never fail a session
		// that has already produced its final.
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *StatelessClient) Health(ctx context.Context) (WorkerAdvert, error) {
	// The router polls real workers directly; this client is not a worker
	// and has no independent health of its own to report.
	return c.advert, nil
}

// call posts one OpenAI-shaped transcription through Bifrost and returns
// the text.
//
// The fallback chain rides as repeated `fallbacks` form fields, and the
// KV references as `state_ref`/`state_sink`. Both work because Bifrost
// forwards unknown multipart fields to the provider verbatim — measured,
// along with the fact that it STRIPS unknown response fields, which is why
// nothing comes back this way and a miss is signalled as a status code.
// See docs/STATELESS-KVTIER.md.
func (c *StatelessClient) call(ctx context.Context, fields map[string]string, audio []byte) (string, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			return "", err
		}
	}
	for _, f := range c.fallbacks {
		if err := mw.WriteField("fallbacks", f); err != nil {
			return "", err
		}
	}
	part, err := mw.CreateFormFile("file", "chunk.wav")
	if err != nil {
		return "", err
	}
	if _, err := part.Write(audio); err != nil {
		return "", err
	}
	if err := mw.Close(); err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.bifrostURL+"/v1/audio/transcriptions", &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := c.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("stateless push: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusTooManyRequests:
		return "", &RateLimitError{RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
	case http.StatusFailedDependency:
		// 424: the state reference resolved nowhere on any member of the
		// chain — evicted, expired, or the tier lost it. Serving the chunk
		// against fresh state would silently drop the session's context,
		// so the worker refuses. Surfacing it as an ordinary error sends
		// the session through coord's recovery, which (finding no
		// checkpoint, because this client has none) replays audio from the
		// journal. That is the right answer: rebuild by recomputation.
		return "", fmt.Errorf("stateless push: state reference missing: %s", truncate(raw))
	default:
		return "", fmt.Errorf("stateless push: status %d: %s", resp.StatusCode, truncate(raw))
	}

	var out struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("stateless push: malformed body: %w", err)
	}
	return out.Text, nil
}

func truncate(b []byte) string {
	if len(b) > 200 {
		return string(b[:200])
	}
	return string(b)
}

// emptyWAV is a valid mono 16kHz s16le container with zero frames.
func emptyWAV() []byte {
	var b bytes.Buffer
	b.WriteString("RIFF")
	binary.Write(&b, binary.LittleEndian, uint32(36))
	b.WriteString("WAVEfmt ")
	binary.Write(&b, binary.LittleEndian, uint32(16))    // fmt chunk size
	binary.Write(&b, binary.LittleEndian, uint16(1))     // PCM
	binary.Write(&b, binary.LittleEndian, uint16(1))     // mono
	binary.Write(&b, binary.LittleEndian, uint32(16000)) // sample rate
	binary.Write(&b, binary.LittleEndian, uint32(32000)) // byte rate
	binary.Write(&b, binary.LittleEndian, uint16(2))     // block align
	binary.Write(&b, binary.LittleEndian, uint16(16))    // bits per sample
	b.WriteString("data")
	binary.Write(&b, binary.LittleEndian, uint32(0))
	return b.Bytes()
}
