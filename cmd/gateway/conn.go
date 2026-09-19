// Per-connection orchestration: readLoop / sessionLoop / writeLoop
// (build-plan.md "Goroutine structure"), wiring internal/wire,
// internal/session, internal/audio, internal/journal, internal/router,
// internal/coord and internal/backend together.
//
// M3 note: this file absorbed internal/coord's failover calls directly
// rather than growing a separate "Coordinator" type that duplicates the
// state sessionLoop already owns — coord exports the recovery algorithms
// as functions over an explicit RecoveryDeps bundle precisely so this
// file (the actual owner of a connection's state) can call them, not so
// a second stateful object needs to shadow sessionLoop's own state.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/coder/websocket"

	"asr-stress-gym/internal/admission"
	"asr-stress-gym/internal/audio"
	"asr-stress-gym/internal/backend"
	"asr-stress-gym/internal/bifrost"
	"asr-stress-gym/internal/coord"
	"asr-stress-gym/internal/journal"
	"asr-stress-gym/internal/metrics"
	"asr-stress-gym/internal/router"
	"asr-stress-gym/internal/session"
	"asr-stress-gym/internal/wire"
)

const (
	frameChanSize = 64 // bounded — build-plan.md "Bound both channels"
	eventChanSize = 64

	onlineChunkMs  = 160  // build-plan.md's online chunking constant
	offlineChunkMs = 2000 // ...and offline

	// ~30s retention at the nominal 20ms cadence (build-plan.md's own
	// retention target). Frame SIZE is client-controlled (num_samples),
	// not mode-dependent, so this is one constant regardless of
	// online/offline — chunk size (onlineChunkMs/offlineChunkMs above) is
	// a separate, unrelated knob on audio.Pipeline's own accumulator.
	journalCapacity = 1500

	// Chunk widths used once the gateway is above its soft session
	// threshold — step 2 of build-plan.md's degradation order. Roughly
	// 2x the healthy width: fewer, larger backend calls for the same
	// audio, trading partial-update latency for throughput at the point
	// where the alternative is refusing the session outright.
	degradedOnlineChunkMs  = 320
	degradedOfflineChunkMs = 4000

	// admissionRetryAfter is what a client refused for lack of a capable
	// worker is told to wait. Advisory, and deliberately short: the
	// condition it reports (every worker ejected or out of budget) is
	// usually seconds long, not minutes.
	admissionRetryAfter = time.Second

	writeTimeout      = 2 * time.Second
	closeTimeout      = 2 * time.Second
	checkpointTimeout = 2 * time.Second
)

// connConfig is what every connection shares: one Router and one
// CheckpointStore, both process-lifetime, constructed once in
// cmd/gateway/main.go — see build-plan.md "Hop 3": the router is a
// library inside the gateway process, not a service, and every
// connection's sessionLoop calls Pick/Report on the SAME instance.
type connConfig struct {
	Router           *router.Router
	Checkpoints      *coord.CheckpointStore
	Admission        *admission.Controller
	Bifrost          *bifrost.Client // nil unless BIFROST_URL — nil-safe throughout
	BifrostFinals    bool            // route ONLINE finals through Bifrost too; off by default, see finalText
	NewAudioPipeline func(sampleRateHz uint32, chunkMs float64) (audio.Pipeline, error)
}

func newSessionID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "s-" + hex.EncodeToString(b)
}

func chunkMsFor(mode session.Mode) float64 {
	if mode == session.ModeOffline {
		return offlineChunkMs
	}
	return onlineChunkMs
}

func degradedChunkMs(mode session.Mode) float64 {
	if mode == session.ModeOffline {
		return degradedOfflineChunkMs
	}
	return degradedOnlineChunkMs
}

// inboundMsg is what readLoop hands to sessionLoop: either a decoded,
// wire-validated Frame, or a terminal decode/validation error. Wire-level
// concerns (frame too short, unknown type, declared-vs-actual duration
// mismatch) are fully resolved in readLoop; sessionLoop never re-derives
// them.
type inboundMsg struct {
	frame wire.Frame
	err   error
}

// handleConnection owns one WebSocket's whole lifecycle. Every one of the
// three loops below calls the same cancel on its way out (each is the
// first defer in its function), so a failure in any one of read/write/
// session promptly unblocks the other two rather than deadlocking on a
// channel nobody is draining any more.
func handleConnection(parentCtx context.Context, ws *websocket.Conn, cfg connConfig) {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	frames := make(chan inboundMsg, frameChanSize)
	events := make(chan any, eventChanSize)
	done := make(chan struct{})

	go readLoop(ctx, ws, frames)
	go writeLoop(ctx, cancel, ws, events, done)

	// sessionLoop deliberately does NOT cancel ctx itself (see its own
	// comment): only close(events). If it also canceled ctx here, that
	// could abort readLoop's still-pending Read (client sent nothing
	// further, e.g. after a malformed control message) at the same time
	// writeLoop is mid-flight writing the very error event this session
	// just produced — a real race that intermittently dropped the last
	// event before the client could read it. Cancellation is held here,
	// strictly after <-done confirms writeLoop has fully drained.
	sessionLoop(ctx, cfg, frames, events)

	<-done // writeLoop has now written everything sessionLoop sent, including any final/error event
	_ = ws.Close(websocket.StatusNormalClosure, "")
}

// readLoop deliberately does NOT cancel ctx on any exit path (unlike
// writeLoop) — only close(frames). readLoop can return the very message
// (a decode/validation failure) that sessionLoop is about to turn into an
// error event; canceling here raced that event's trySend against
// readLoop's own shutdown and intermittently dropped it before the client
// could read it. sessionLoop's range over frames ends on its own once
// close(frames) fires, and handleConnection's later ws.Close (only after
// <-done confirms writeLoop drained) is what unblocks a still-pending
// Read, not this.
func readLoop(ctx context.Context, ws *websocket.Conn, frames chan<- inboundMsg) {
	defer close(frames)

	for {
		typ, data, err := ws.Read(ctx)
		if err != nil {
			return // ctx canceled, client closed, or a transport error — all end the connection
		}
		if typ != websocket.MessageBinary {
			continue // client<->gateway audio/control is binary only; ignore stray text frames
		}

		f, decErr := wire.Decode(data)
		if decErr == nil && f.Type == wire.MsgAudio {
			decErr = wire.ValidateAudioFrame(f)
		}

		select {
		case frames <- inboundMsg{frame: f, err: decErr}:
		case <-ctx.Done():
			return
		}
		if decErr != nil {
			return // a protocol violation is terminal (docs/PROTOCOL.md) — stop reading further
		}
	}
}

func writeLoop(ctx context.Context, cancel context.CancelFunc, ws *websocket.Conn, events <-chan any, done chan<- struct{}) {
	defer cancel()
	defer close(done)

	// Plain range, not a select on ctx: sessionLoop closes events exactly
	// once on every return path (it's the first defer in sessionLoop), so
	// this drains whatever was already sent — e.g. a final event — before
	// exiting, rather than racing a cancellation against undelivered
	// events still sitting in the channel buffer.
	for ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			log.Printf("gateway: marshal event %T: %v", ev, err)
			continue
		}
		wctx, wcancel := context.WithTimeout(context.Background(), writeTimeout)
		err = ws.Write(wctx, websocket.MessageText, data)
		wcancel()
		if err != nil {
			return
		}
	}
}

// trySend guards every sessionLoop emit against a dead write side: if
// writeLoop has already exited (cancel fired), this returns false instead
// of blocking forever on a full, undrained events channel.
func trySend(ctx context.Context, events chan<- any, ev any) bool {
	select {
	case events <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

// sessionLoop is the SOLE owner of session state (build-plan.md: "the
// only writer") — nothing else in this file mutates session.InferenceState
// or the audio.Pipeline.
//
// Deliberately does NOT cancel ctx on its way out, only close(events):
// canceling here would race an early-return (e.g. a malformed control
// message) against writeLoop still flushing the very error event this
// return just produced. handleConnection cancels only after confirming,
// via <-done, that writeLoop has fully drained — see its comment.
func sessionLoop(ctx context.Context, cfg connConfig, frames <-chan inboundMsg, events chan<- any) {
	defer close(events)

	sessID := newSessionID()
	state := session.NewInferenceState(sessID, "")
	emitter := session.NewEmitter(state)
	seqV := session.NewSeqValidator()

	// deps bundles everything both the initial Open and every recovery
	// call need. State/Emitter/Journal/Router/Checkpoints are set once,
	// here, and never reassigned (their pointed-to content mutates
	// normally through the pointers). Pipeline starts nil and is set
	// exactly once, in handleControl's session.start case — deps must be
	// passed as *coord.RecoveryDeps wherever that assignment needs to be
	// visible afterward (handleControl only), and by value everywhere
	// that only reads it (handleAudioFrame, dispatchChunk) since Go
	// copies the struct but not what its pointer fields point to.
	deps := coord.RecoveryDeps{
		State:       state,
		Emitter:     emitter,
		Journal:     journal.New(journalCapacity),
		Router:      cfg.Router,
		Checkpoints: cfg.Checkpoints,
	}

	var (
		started  bool
		admitted bool
		client   backend.Client
	)
	defer func() {
		// Release before anything else can fail: the slot must come back
		// even if the backend Close below errors, or a gateway that saw a
		// few bad teardowns would leak capacity until it refused everyone.
		if admitted {
			cfg.Admission.Release()
		}
		if started {
			cctx, ccancel := context.WithTimeout(context.Background(), closeTimeout)
			if err := client.Close(cctx, state.Handle); err != nil {
				log.Printf("gateway[%s]: backend close: %v", sessID, err)
			}
			ccancel()
			if w, ok := cfg.Router.Find(state.WorkerID); ok {
				w.UnbindSession()
			}
		}
		cfg.Checkpoints.Delete(sessID) // avoid unbounded growth across the gateway process's lifetime
	}()

	for msg := range frames {
		if msg.err != nil {
			trySend(ctx, events, emitter.Error(msg.err.Error()))
			return
		}
		f := msg.frame

		outcome, gap := seqV.Validate(f.Seq)
		if outcome == session.SeqDuplicate {
			continue // client replayed after reconnect — already accounted for, do not re-ingest
		}
		if outcome == session.SeqGap {
			if !trySend(ctx, events, emitter.Discontinuity(gap)) {
				return
			}
			// fall through: the doc's own rule — still accept this frame
		}

		switch f.Type {
		case wire.MsgControl:
			if !handleControl(ctx, cfg, f, &deps, events, &started, &admitted, &client) {
				return
			}
		case wire.MsgAudio:
			if !started {
				trySend(ctx, events, emitter.Error("audio frame received before session.start"))
				return
			}
			if !handleAudioFrame(ctx, cfg, f, deps, &client, events) {
				return
			}
		}
	}
}

// handleControl processes one decoded CONTROL frame. Returns false if the
// session must terminate (a validation failure, a backend error, or a
// clean session.end that has already emitted its final).
func handleControl(
	ctx context.Context, cfg connConfig, f wire.Frame,
	deps *coord.RecoveryDeps, events chan<- any,
	started *bool, admitted *bool, client *backend.Client,
) bool {
	state, emitter := deps.State, deps.Emitter
	msg, err := wire.DecodeControl(f.Payload)
	if err != nil {
		trySend(ctx, events, emitter.Error(err.Error()))
		return false
	}

	switch m := msg.(type) {
	case *wire.SessionStart:
		if *started {
			trySend(ctx, events, emitter.Error("session.start received twice"))
			return false
		}
		if err := wire.ValidateSessionStart(*m); err != nil {
			trySend(ctx, events, emitter.Error(err.Error()))
			return false
		}

		// Admission first, before any work is done on this session's
		// behalf. Refusing costs a pipeline construction and an Open if it
		// happens later, and under a ramp that is precisely the work the
		// gateway cannot afford — the point of refusing is to not spend.
		if d := cfg.Admission.Admit(); d.Refused {
			trySend(ctx, events, emitter.Overloaded(d.Reason, d.RetryAfter))
			return false // retryable, and NOT an error event: the session was never created
		}
		*admitted = true

		mode := session.Mode(m.Mode)
		state.Mode = mode
		state.SampleRateHz = m.SampleRateHz // needed again if a failover ever has to re-Open against a replacement
		newPipeline := cfg.NewAudioPipeline
		if newPipeline == nil {
			// Tests exercising gateway/router mechanics deliberately retain the
			// M1 pass-through implementation. M4's VAD contract is tested in
			// internal/audio, so those tests need not learn audio policy.
			newPipeline = func(sampleRateHz uint32, chunkMs float64) (audio.Pipeline, error) {
				return audio.NewPassthroughPipeline(sampleRateHz, chunkMs), nil
			}
		}
		// Step 2 of the degradation order: above the soft threshold a new
		// session gets wider chunks, cutting its backend call volume
		// before the gateway has to start refusing anyone. Decided once,
		// here, because chunk boundaries are part of what a replay must
		// reproduce (build-plan.md) and cannot change mid-session.
		chunkMs := cfg.Admission.ChunkPolicy(chunkMsFor(mode), degradedChunkMs(mode))
		pipeline, err := newPipeline(uint32(m.SampleRateHz), chunkMs)
		if err != nil {
			trySend(ctx, events, emitter.Error(fmt.Sprintf("audio pipeline setup failed: %v", err)))
			return false
		}
		deps.Pipeline = pipeline

		target, resp, err := openWithRetry(ctx, cfg.Router, state.SessionID, mode, m)
		if err != nil {
			// ErrNoCapacity means the filter stage emptied — every worker
			// is ejected, out of rate budget, or the wrong shape for this
			// mode. That is retryable backpressure, not a broken session,
			// so it gets `overloaded` like any other admission refusal.
			// Anything else (a worker that accepted the pick and then
			// failed to Open, repeatedly) is a real fault and stays
			// terminal.
			if errors.Is(err, router.ErrNoCapacity) {
				metrics.AdmissionRejectedTotal.Add(1)
				trySend(ctx, events, emitter.Overloaded("no capable worker available", admissionRetryAfter))
				return false
			}
			trySend(ctx, events, emitter.Error(fmt.Sprintf("no worker available: %v", err)))
			return false
		}
		target.BindSession()
		cfg.Router.Report(target.ID, true, 0)

		state.WorkerID = target.ID
		state.Handle = resp.Handle
		state.CompatibilityKey = target.CompatibilityKey
		state.Generation = resp.Generation
		*client = target.Client
		*started = true

		// Piggyback session_id delivery on the existing ack contract
		// (docs/PROTOCOL.md) rather than inventing a session.started
		// event: an ack for the session.start frame's own seq is both
		// truthful (the gateway has durably accepted through this seq)
		// and sufficient for the client to learn its session_id.
		return trySend(ctx, events, emitter.Ack(f.Seq))

	case *wire.SessionEnd:
		if !*started {
			trySend(ctx, events, emitter.Error("session.end received before session.start"))
			return false
		}
		for _, c := range deps.Pipeline.Flush() { // dispatch whatever tail never crossed a chunk threshold
			if !dispatchChunk(ctx, c, *deps, client, events) {
				return false
			}
		}
		// Not failover-protected: if the backend dies in exactly this
		// window (after the tail replay above, before this call lands),
		// the session errors out rather than attempting a second
		// recovery here. A deliberate, documented scope limit for M3,
		// not an oversight — the window is narrow and the failure mode
		// is a clean error, not a hang or silent corruption.
		text, err := finalText(ctx, cfg, *deps, client, state.UtteranceStartSeq)
		if err != nil {
			trySend(ctx, events, emitter.Error(fmt.Sprintf("backend flush failed: %v", err)))
			return false
		}
		finalEv, isNew := emitter.Final(text, 0, state.LastAppliedSeq)
		if isNew {
			// Trim on every committed final (build-plan.md). M1/M2: exactly
			// one utterance per session, so the final's own seq_end is the
			// correct floor directly — no min(committedSeq, openUtteranceStart)
			// needed yet (implementation-plan.md defect #8's fuller formula
			// applies once M4 allows a new utterance to already be open
			// when this fires).
			deps.Journal.TrimBefore(finalEv.SeqEnd)
		} else {
			metrics.DuplicateFinalsTotal.Add(1) // must stay zero — asserted by every chaos scenario
		}
		trySend(ctx, events, finalEv)
		return false // clean end: stop the session loop, same as an error return

	default:
		trySend(ctx, events, emitter.Error(fmt.Sprintf("unexpected control message %T", msg)))
		return false
	}
}

// openWithRetry picks and opens against a worker, retrying against a
// DIFFERENT candidate (excluding every one already tried) up to
// coord.MaxFailoverAttempts times if Open itself fails — e.g. the picked
// worker died in the window between Pick and Open. Simpler than
// coord.HandleBackendFailure on purpose: there is no prior session state
// to preserve or replay yet, since no audio has been sent.
func openWithRetry(ctx context.Context, r *router.Router, sessionID string, mode session.Mode, m *wire.SessionStart) (*router.Worker, backend.OpenResp, error) {
	excluded := map[string]bool{}
	var lastErr error
	for attempt := 0; attempt < coord.MaxFailoverAttempts; attempt++ {
		target, err := r.Pick(mode, excluded, "")
		if err != nil {
			return nil, backend.OpenResp{}, err // no capacity at all; no point retrying
		}
		resp, err := target.Client.Open(ctx, backend.OpenReq{SessionID: sessionID, SampleRateHz: m.SampleRateHz, Mode: m.Mode})
		if err == nil {
			return target, resp, nil
		}
		lastErr = err
		excluded[target.ID] = true
		r.Report(target.ID, false, 0)
	}
	return nil, backend.OpenResp{}, fmt.Errorf("exhausted after %d attempts: %w", coord.MaxFailoverAttempts, lastErr)
}

func handleAudioFrame(ctx context.Context, cfg connConfig, f wire.Frame, deps coord.RecoveryDeps, client *backend.Client, events chan<- any) bool {
	ref, err := deps.Pipeline.Ingest(f)
	if err != nil {
		trySend(ctx, events, deps.Emitter.Error(fmt.Sprintf("audio ingest failed: %v", err)))
		return false
	}
	// Every accepted frame is journaled, including silence (Voiced is
	// always true at M1/M2 — VAD lands at M4) — the journal's
	// completeness is what makes a full audio rebuild always possible
	// regardless of what gets gated out of chunk dispatch. See
	// internal/journal's package doc for why this, plus Recut, is the
	// complete replay mechanism with no separate dispatch-log structure.
	deps.Journal.Append(audio.Record{Seq: f.Seq, DurationMs: ref.DurationMs, Voiced: ref.Voiced, Payload: f.Payload})

	var endpoint bool
	for {
		ev, ok := deps.Pipeline.Boundary()
		if !ok {
			break
		}
		switch ev.Type {
		case audio.EventSpeechStart:
			if !trySend(ctx, events, deps.Emitter.SpeechStart(ev.Seq)) {
				return false
			}
		case audio.EventEndpoint:
			endpoint = true
		}
	}

	for _, c := range deps.Pipeline.Ready() {
		if !dispatchChunk(ctx, c, deps, client, events) {
			return false
		}
	}
	if endpoint {
		return finalizeEndpoint(ctx, cfg, deps, client, events)
	}
	return true
}

// finalText produces an utterance's final transcript, through Bifrost or
// via the pinned worker's Flush.
//
// **Which one, and why it is not simply "Bifrost when enabled".**
//
// build-plan.md reasons that a complete utterance IS a discrete stateless
// request, so finals belong behind Bifrost. That is true of the AUDIO and
// false of the WORKER. By the time an utterance ends, the pinned worker is
// holding inference state built from exactly that audio — the accumulated
// encoder and predictor context. A direct Flush finalizes that state. A
// Bifrost-routed final cannot: /v1/audio/transcriptions is stateless by
// construction (fresh state, infer, finalize, discard), so it throws the
// hot state away and re-transcribes from raw samples.
//
// So routing an ONLINE final through Bifrost discards the very thing this
// project is about, in order to recompute a result the pinned worker could
// have produced from state it already held. It is off by default for that
// reason (BifrostFinals) — not because of its latency, which is a symptom
// of the same thing.
//
// OFFLINE is the opposite case, and the one Bifrost genuinely fits:
// offline emits no partials, so nothing is being reused and nothing is
// discarded; there is no latency budget, so retry-with-backoff is correct
// rather than catastrophic; and the router has no advantage to offer — no
// session to pin, no compatibility key to prefer, no hot state to preserve.
// Backend selection for that class is Bifrost's to own.
//
// **Bifrost is never allowed to lose a final.** Any failure — unreachable,
// timeout, non-200, malformed body — falls through to the direct path,
// which is the same call the gateway would have made anyway.
func finalText(ctx context.Context, cfg connConfig, deps coord.RecoveryDeps, client *backend.Client, startSeq uint64) (string, error) {
	useBifrost := cfg.Bifrost.Enabled() &&
		(deps.State.Mode == session.ModeOffline || cfg.BifrostFinals)
	if useBifrost {
		// The journal already holds every frame of this utterance,
		// including the silence the VAD gated out of dispatch — so the
		// audio handed to Bifrost is the utterance as CAPTURED, not as
		// chunked. internal/audio wraps it; nothing here sees a sample.
		records := deps.Journal.ReadAfter(startSeq)
		if len(records) > 0 {
			wav := audio.WAVFromRecords(records, uint32(deps.State.SampleRateHz))
			text, err := cfg.Bifrost.Transcribe(ctx, wav)
			if err == nil {
				return text, nil
			}
			log.Printf("gateway[%s]: bifrost final failed, falling back to direct flush: %v",
				deps.State.SessionID, err)
		}
	}
	resp, err := (*client).Flush(ctx, deps.State.Handle)
	return resp.Text, err
}

// finalizeEndpoint ends the current VAD-delimited utterance but keeps the
// WebSocket session open. The worker handle is deliberately replaced: workers
// expose a stateful streaming API, so continuing to Push after Flush would
// make a new utterance inherit the old model state and transcript.
func finalizeEndpoint(ctx context.Context, cfg connConfig, deps coord.RecoveryDeps, client *backend.Client, events chan<- any) bool {
	for _, c := range deps.Pipeline.Flush() {
		if !dispatchChunk(ctx, c, deps, client, events) {
			return false
		}
	}
	text, err := finalText(ctx, cfg, deps, client, deps.State.UtteranceStartSeq)
	if err != nil {
		trySend(ctx, events, deps.Emitter.Error(fmt.Sprintf("backend flush after endpoint failed: %v", err)))
		return false
	}
	finalEv, isNew := deps.Emitter.Final(text, deps.State.UtteranceStartSeq, deps.State.LastAppliedSeq)
	if isNew {
		trimSeq := finalEv.SeqEnd
		shouldTrim := true
		if start, ok := deps.Pipeline.RetentionStart(); ok {
			// TrimBefore excludes its argument too, so retain the pre-roll's
			// first record by placing the floor immediately before it.
			if start == 0 {
				// There is no representable sequence before zero. Leaving the
				// journal intact is the safe bounded exception for this first
				// utterance; its ring capacity remains the memory backstop.
				shouldTrim = false
			} else if start-1 < trimSeq {
				trimSeq = start - 1
			}
		}
		if shouldTrim {
			deps.Journal.TrimBefore(trimSeq)
		}
	} else {
		metrics.DuplicateFinalsTotal.Add(1)
	}
	if !trySend(ctx, events, finalEv) {
		return false
	}

	oldHandle := deps.State.Handle
	if err := (*client).Close(ctx, oldHandle); err != nil {
		trySend(ctx, events, deps.Emitter.Error(fmt.Sprintf("backend close after endpoint failed: %v", err)))
		return false
	}
	resp, err := (*client).Open(ctx, backend.OpenReq{
		SessionID: deps.State.SessionID, SampleRateHz: deps.State.SampleRateHz, Mode: string(deps.State.Mode),
	})
	if err != nil {
		trySend(ctx, events, deps.Emitter.Error(fmt.Sprintf("backend open next utterance failed: %v", err)))
		return false
	}
	deps.Checkpoints.Delete(deps.State.SessionID)
	deps.State.Handle = resp.Handle
	deps.State.Generation = resp.Generation
	deps.State.LastAppliedSeq = finalEv.SeqEnd
	deps.State.NewUtterance()
	return true
}

// dispatchChunk pushes one chunk to the backend, transparently recovering
// via coord.HandleBackendFailure on a backend failure. A stale-generation
// error is NOT a failover trigger — invariant 3 (one active owner at a
// time) means that should never legitimately happen while this is the
// only writer, so it surfaces as a session error instead of masking a
// coordination bug as a transient backend problem.
//
// On a successful recovery, the chunk that triggered the failure is NOT
// re-pushed here: its underlying frames were already journaled before
// this Push was ever attempted (handleAudioFrame appends before
// dispatching), so the recovery's own replay — which reads the journal
// from the checkpoint or last committed boundary — already covers it.
func dispatchChunk(ctx context.Context, c audio.Chunk, deps coord.RecoveryDeps, client *backend.Client, events chan<- any) bool {
	pushStarted := time.Now()
	resp, err := (*client).Push(ctx, backend.PushReq{
		Handle:             deps.State.Handle,
		SeqStart:           c.SeqStart,
		SeqEnd:             c.SeqEnd,
		ExpectedGeneration: deps.State.Generation,
		Audio:              c.Bytes,
	})
	if err != nil {
		if errors.Is(err, backend.ErrStaleGeneration) {
			metrics.StaleGenerationWritesTotal.Add(1)
			trySend(ctx, events, deps.Emitter.Error("unexpected stale generation: "+err.Error()))
			return false
		}

		// A 429 is NOT a dead backend, and must not be waited out.
		// build-plan.md: "honour Retry-After, zero the bucket, and on the
		// online path do not sleep. A 500ms backoff is catastrophic
		// against a 200ms budget. Move to another backend."
		//
		// Zeroing the bucket is what makes the move stick: without it the
		// router would keep scoring this worker as a fine candidate and
		// failover could hand the session straight back to the backend
		// that just refused it, which is how a retry storm starts. The
		// failover path below then runs normally — from its point of view
		// this worker has simply failed, which is true.
		var rl *backend.RateLimitError
		if errors.As(err, &rl) {
			deps.Router.On429(deps.State.WorkerID, rl.RetryAfter)
		}

		newClient, resetEv, regenerated, ferr := coord.HandleBackendFailure(ctx, deps, err)
		if ferr != nil {
			trySend(ctx, events, deps.Emitter.Error(fmt.Sprintf("failover exhausted: %v", ferr)))
			return false
		}
		*client = newClient
		if !trySend(ctx, events, resetEv) {
			return false
		}
		if regenerated == nil {
			return true // the replay had nothing to regenerate — see replayChunks
		}
		return trySend(ctx, events, deps.Emitter.Partial(*regenerated))
	}
	metrics.BackendPushesTotal.Add(1)
	// Successful calls are the router's source of real latency samples.
	// Without this report the router's gray-failure policy only exists in
	// unit tests: no live worker ever accumulates a p95 to compare against
	// the fleet. Failure reporting remains in HandleBackendFailure so the
	// triggering error is counted exactly once before a replacement is Picked.
	deps.Router.Report(deps.State.WorkerID, true, time.Since(pushStarted))

	deps.State.Generation = resp.Generation
	deps.State.LastAppliedSeq = resp.LastSeqApplied

	// Async, best-effort, never on the critical path (invariant 13:
	// checkpoint failure must never take down healthy inference) — this
	// is what makes RecoverSameModel's cheap tail-replay path reachable
	// at all; without it, Checkpoints.Latest would always miss and every
	// same-key failover would silently degrade to the full-replay path.
	// Values are snapshotted HERE, on sessionLoop's own goroutine (the
	// sole writer to deps.State), and passed BY VALUE into the goroutine
	// rather than letting it read deps.State fields itself — reading
	// those concurrently with a later write (e.g. a subsequent failover
	// mutating Handle) would be a real data race.
	// Don't ask a backend that has already said it cannot serialize. From
	// M5 that is four of the fleet's six workers — sherpa-onnx and
	// CTranslate2 expose no way to save inference state — so without this
	// check the common case becomes one extra HTTP round trip per chunk
	// whose only possible answer is 501. The error path below handles
	// ErrNotSupported quietly and correctly; this just stops asking.
	if w, ok := deps.Router.Find(deps.State.WorkerID); ok && w.Capabilities.Serializable {
		go asyncCheckpoint(deps.Checkpoints, *client, deps.State.SessionID, deps.State.CompatibilityKey, deps.State.Handle)
	}

	if !trySend(ctx, events, deps.Emitter.Partial(resp.Text)) {
		return false
	}
	return trySend(ctx, events, deps.Emitter.Ack(resp.LastSeqApplied))
}

func asyncCheckpoint(checkpoints *coord.CheckpointStore, client backend.Client, sessionID string, key session.CacheCompatibilityKey, handle string) {
	ctx, cancel := context.WithTimeout(context.Background(), checkpointTimeout)
	defer cancel()
	resp, err := client.Checkpoint(ctx, handle)
	if err != nil {
		if !errors.Is(err, backend.ErrNotSupported) {
			// ErrNotSupported (Capabilities.Serializable == false) is an
			// expected, permanent fact about this adapter, not worth a
			// log line on every single push — every other error might be
			// worth noticing, even though it's still non-fatal here.
			log.Printf("gateway[%s]: async checkpoint failed (non-fatal, invariant 13): %v", sessionID, err)
		}
		return
	}
	checkpoints.Store(sessionID, key, resp)
}
