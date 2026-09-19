// Per-connection orchestration: readLoop / sessionLoop / writeLoop
// (build-plan.md "Goroutine structure"), wiring internal/wire,
// internal/session, internal/audio and internal/backend together.
//
// This lives in cmd/gateway, not internal/coord, deliberately: M1 has no
// router and no pool (build-plan.md Phase 1's own scope), so a session
// opens against one statically configured worker with no selection logic.
// internal/coord's package doc already earmarks the formal Session
// Coordinator — with failover, pinning, and a real router behind it — for
// M3. This file is what that coordinator will absorb and extend, not a
// permanent home for it.
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

	"asr-stress-gym/internal/audio"
	"asr-stress-gym/internal/backend"
	"asr-stress-gym/internal/journal"
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

	writeTimeout = 2 * time.Second
	closeTimeout = 2 * time.Second
)

// connConfig is what a connection needs to reach a backend. At M1 there
// is no router: every session opens against one statically configured
// worker (build-plan.md Phase 1: "No ... pool").
type connConfig struct {
	workerBaseURL string
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
	j := journal.New(journalCapacity)

	var (
		started  bool
		pipeline audio.Pipeline
		client   backend.Client
	)
	defer func() {
		if started {
			cctx, ccancel := context.WithTimeout(context.Background(), closeTimeout)
			defer ccancel()
			if err := client.Close(cctx, state.Handle); err != nil {
				log.Printf("gateway[%s]: backend close: %v", sessID, err)
			}
		}
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
			if !handleControl(ctx, cfg, f, state, emitter, events, j, &started, &pipeline, &client) {
				return
			}
		case wire.MsgAudio:
			if !started {
				trySend(ctx, events, emitter.Error("audio frame received before session.start"))
				return
			}
			if !handleAudioFrame(ctx, f, pipeline, client, state, emitter, events, j) {
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
	state *session.InferenceState, emitter *session.Emitter, events chan<- any, j *journal.Journal,
	started *bool, pipeline *audio.Pipeline, client *backend.Client,
) bool {
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

		mode := session.Mode(m.Mode)
		state.Mode = mode
		*pipeline = audio.NewPassthroughPipeline(uint32(m.SampleRateHz), chunkMsFor(mode))
		*client = backend.NewHTTPClient(cfg.workerBaseURL)

		resp, err := (*client).Open(ctx, backend.OpenReq{SessionID: state.SessionID, SampleRateHz: m.SampleRateHz, Mode: m.Mode})
		if err != nil {
			trySend(ctx, events, emitter.Error(fmt.Sprintf("backend open failed: %v", err)))
			return false
		}
		state.Handle = resp.Handle
		state.CompatibilityKey = session.CacheCompatibilityKey(resp.CompatibilityKeyHash)
		state.Generation = resp.Generation
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
		for _, c := range (*pipeline).Flush() { // dispatch whatever tail never crossed a chunk threshold
			if !dispatchChunk(ctx, c, *client, state, emitter, events) {
				return false
			}
		}
		flushResp, err := (*client).Flush(ctx, state.Handle)
		if err != nil {
			trySend(ctx, events, emitter.Error(fmt.Sprintf("backend flush failed: %v", err)))
			return false
		}
		finalEv, isNew := emitter.Final(flushResp.Text, 0, state.LastAppliedSeq)
		if isNew {
			// Trim on every committed final (build-plan.md). M1/M2: exactly
			// one utterance per session, so the final's own seq_end is the
			// correct floor directly — no min(committedSeq, openUtteranceStart)
			// needed yet (implementation-plan.md defect #8's fuller formula
			// applies once M4 allows a new utterance to already be open
			// when this fires).
			j.TrimBefore(finalEv.SeqEnd)
		}
		trySend(ctx, events, finalEv)
		return false // clean end: stop the session loop, same as an error return

	default:
		trySend(ctx, events, emitter.Error(fmt.Sprintf("unexpected control message %T", msg)))
		return false
	}
}

func handleAudioFrame(
	ctx context.Context, f wire.Frame, pipeline audio.Pipeline, client backend.Client,
	state *session.InferenceState, emitter *session.Emitter, events chan<- any, j *journal.Journal,
) bool {
	ref, err := pipeline.Ingest(f)
	if err != nil {
		trySend(ctx, events, emitter.Error(fmt.Sprintf("audio ingest failed: %v", err)))
		return false
	}
	// Every accepted frame is journaled, including silence (Voiced is
	// always true at M1/M2 — VAD lands at M4) — the journal's
	// completeness is what makes a full audio rebuild always possible
	// regardless of what gets gated out of chunk dispatch. See
	// internal/journal's package doc for why this, plus Recut, is the
	// complete replay mechanism with no separate dispatch-log structure.
	j.Append(audio.Record{Seq: f.Seq, DurationMs: ref.DurationMs, Voiced: ref.Voiced, Payload: f.Payload})

	for _, c := range pipeline.Ready() {
		if !dispatchChunk(ctx, c, client, state, emitter, events) {
			return false
		}
	}
	return true
}

// dispatchChunk pushes one chunk to the backend and emits partial+ack from
// the response. Returns false if the session must terminate.
func dispatchChunk(ctx context.Context, c audio.Chunk, client backend.Client, state *session.InferenceState, emitter *session.Emitter, events chan<- any) bool {
	resp, err := client.Push(ctx, backend.PushReq{
		Handle:             state.Handle,
		SeqStart:           c.SeqStart,
		SeqEnd:             c.SeqEnd,
		ExpectedGeneration: state.Generation,
		Audio:              c.Bytes,
	})
	if err != nil {
		reason := err.Error()
		if errors.Is(err, backend.ErrStaleGeneration) {
			// One live session has exactly one active owner (invariant
			// 3) and M1 has no failover yet, so this should never
			// legitimately happen — surfacing it as a session error
			// rather than silently retrying is the honest response to a
			// bug, not a transient condition to paper over.
			reason = "unexpected stale generation: " + reason
		}
		trySend(ctx, events, emitter.Error(reason))
		return false
	}
	state.Generation = resp.Generation
	state.LastAppliedSeq = resp.LastSeqApplied

	if !trySend(ctx, events, emitter.Partial(resp.Text)) {
		return false
	}
	return trySend(ctx, events, emitter.Ack(resp.LastSeqApplied))
}
