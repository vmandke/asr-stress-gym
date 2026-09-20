// Package bifrost routes DISCRETE transcription requests through a Bifrost
// gateway — docs/build-plan.md, "The Bifrost boundary".
//
// What goes through here and what does not:
//
//	partials during speech  ->  DIRECT to the pinned worker
//	finals per utterance    ->  here
//	offline jobs            ->  here
//
// The split is structural, not a preference. Bifrost routes discrete HTTP
// requests and load-balances between providers. A `push` carries a handle
// and an expected generation, and the worker holds inference state keyed by
// that handle — so a streaming call routed through Bifrost could land chunk
// N on one worker and chunk N+1 on another, the second holding no state,
// and the gateway never learning the model changed underneath it. No
// partial.reset, no replay, a silently corrupt transcript. That is exactly
// the failure this project exists to prevent, so the streaming path is not
// routable through here and there is no code path that would allow it.
//
// A complete utterance, by contrast, IS a discrete stateless request, which
// is what Bifrost is for and what it is good at: provider fallback, retry,
// weighted load balancing and health tracking as configuration rather than
// code.
//
// Disabled by default. Every call falls back to the direct path on any
// error, so enabling this can degrade latency but can never lose a final.
package bifrost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Client is nil-safe throughout: a gateway built without Bifrost holds a
// nil *Client and every method reports "not enabled" rather than panicking.
// That keeps the call sites free of enabled/disabled branching.
type Client struct {
	baseURL   string
	model     string
	fallbacks []string
	hc        *http.Client

	// The provider that served the most recent successful request.
	// Diagnostic only: read by the gateway's log line and by the routing
	// trace, never by any decision.
	lastProvider atomic.Value
}

// LastProvider reports which provider served the last successful call, or
// "" if none has. Nil-safe like the rest of this type.
func (c *Client) LastProvider() string {
	if c == nil {
		return ""
	}
	v, _ := c.lastProvider.Load().(string)
	return v
}

func pick(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// New returns nil when baseURL is empty — i.e. when BIFROST_URL is unset.
// Callers treat a nil Client as "use the direct path", which is the default
// and the safe state.
//
// fallbacks is an ordered chain of
// "provider/model" strings Bifrost tries when the primary fails — this is
// the one Bifrost feature that does real, non-duplicated work here, because
// the gateway implements failover for the STREAMING path only
// (internal/coord) and has nothing equivalent for finals.
//
// Verified to work on /v1/audio/transcriptions, which the published docs
// only describe for chat completions: with the primary worker SIGKILLed,
// a request naming it still returned a transcript, served by the next
// provider in the chain.
func New(baseURL, model string, fallbacks []string, timeout time.Duration) *Client {
	if baseURL == "" {
		return nil
	}
	if model == "" {
		model = "whisper-1" // the name OpenAI clients send; providers map it
	}
	return &Client{
		baseURL:   baseURL,
		model:     model,
		fallbacks: fallbacks,
		// Bounded, and deliberately longer than the direct backend client's
		// 5s: a final routed through Bifrost pays a proxy hop plus whatever
		// retry policy Bifrost is configured with. Still bounded, because an
		// unbounded wait here would stall the utterance that is trying to
		// finish.
		hc: &http.Client{Timeout: timeout},
	}
}

func (c *Client) Enabled() bool { return c != nil }

// BaseURL is for callers that must build their own request against the
// same Bifrost instance rather than go through Transcribe — currently only
// backend.StatelessClient, whose requests carry KV state references this
// package has no business knowing about.
func (c *Client) BaseURL() string {
	if c == nil {
		return ""
	}
	return c.baseURL
}

// Transcribe posts one complete utterance as an OpenAI-shaped multipart
// request and returns the text.
//
// `wav` is opaque here — it was built by internal/audio, the only package
// allowed to know what a sample is.
func (c *Client) Transcribe(ctx context.Context, wav []byte) (string, error) {
	return c.TranscribeVia(ctx, wav, c.model, c.fallbacks)
}

// TranscribeVia routes one utterance through a CALLER-SUPPLIED pool: a
// primary provider and an ordered fallback chain.
//
// This is what makes Bifrost load-bearing rather than decorative here.
// With a fixed `model` from an env var, Bifrost answered a question the
// gateway had already answered better — and answered it differently, so
// an offline session pinned to one worker had its final computed on
// another, discarding the state it had just built. Worse, the static
// fallback chain crossed model families, which is harmless for stateless
// audio and would be silent corruption for anything carrying state.
//
// Supplying the pool per request fixes both. The caller passes the worker
// its own router chose, followed by that worker's cache-compatible peers
// (router.Pool), so:
//
//   - the primary is the gateway's decision, not a config constant;
//   - the chain is homogeneous BY CONSTRUCTION and cannot cross a family;
//   - Bifrost does the part it is actually good at — retry, failover and
//     health across a set of interchangeable endpoints — while every
//     decision needing runtime knowledge stays in the router.
//
// That is the two-layer shape production uses (docs/PRODUCTION-SHAPE.md):
// a gateway that resolves names and fails over, in front of a pool whose
// members are interchangeable by construction.
func (c *Client) TranscribeVia(ctx context.Context, wav []byte, model string, fallbacks []string) (string, error) {
	if c == nil {
		return "", fmt.Errorf("bifrost: not enabled")
	}
	if model == "" {
		model = c.model
	}

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	part, err := mw.CreateFormFile("file", "utterance.wav")
	if err != nil {
		return "", err
	}
	if _, err := part.Write(wav); err != nil {
		return "", err
	}
	if err := mw.WriteField("model", model); err != nil {
		return "", err
	}
	// Repeated form fields, one per fallback, in order. Bifrost gives each
	// provider its own full retry budget before moving to the next.
	for _, f := range fallbacks {
		if err := mw.WriteField("fallbacks", f); err != nil {
			return "", err
		}
	}
	if err := mw.Close(); err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/audio/transcriptions", &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := c.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("bifrost: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return "", fmt.Errorf("bifrost: status %d: %s", resp.StatusCode, snippet)
	}

	var out struct {
		Text        string `json:"text"`
		WorkerID    string `json:"worker_id"` // the worker's own answer — see worker/server.py
		ExtraFields struct {
			Provider string `json:"provider"`
		} `json:"extra_fields"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("bifrost: decode: %w", err)
	}
	// Which provider actually served is recorded, not acted on. The
	// gateway must not start second-guessing Bifrost's choice within a
	// pool — that is the one decision genuinely delegated to it — but
	// "which of the pool answered" is exactly what a trace needs to show
	// that a fallback happened, so it is observable.
	c.lastProvider.Store(pick(out.ExtraFields.Provider, out.WorkerID))
	return out.Text, nil
}

// Describe reports the configured routing chain, for the gateway's startup
// log. Which backend actually served a request is Bifrost's to decide and
// is visible in its own logs; the gateway deliberately does not branch on
// it, because the whole point of delegating this class of traffic is not
// having a second opinion about backend health.
func (c *Client) Describe() string {
	if c == nil {
		return "disabled"
	}
	if len(c.fallbacks) == 0 {
		return c.model + " (no fallbacks)"
	}
	return c.model + " -> " + strings.Join(c.fallbacks, " -> ")
}
