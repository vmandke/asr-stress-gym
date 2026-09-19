package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/coder/websocket"

	"asr-stress-gym/internal/corpus"
	"asr-stress-gym/internal/wire"
)

// stream is one synthetic client: one WebSocket, one session at a time,
// paced on its own ticker. Everything it needs is passed in at
// construction so a stream shares nothing mutable with its peers except
// the counters and the event channel, both of which are safe for
// concurrent use.
type stream struct {
	id     int
	cfg    config
	clip   corpus.Clip
	rng    *rand.Rand
	cnt    *counters
	events chan<- event
	start  time.Time

	// Per-connection state, reset on reconnect. seq does NOT reset: it is
	// the session's own monotonic sequence, and a resuming client
	// continues it from where its last ack left off rather than
	// restarting and colliding with audio the gateway already holds.
	seq          uint64
	lastAckedSeq uint64
	lastFrameAt  time.Time
	sentAt       map[uint64]time.Time

	// A `partial` carries no sequence number, so on its own the only
	// latency it supports is "time since the last frame I wrote" — which
	// is wrong in precisely the regime that matters. If the gateway is
	// slow, the partial for the chunk ending at seq 9 arrives after
	// frames 10..15 have gone out, and measuring against frame 15 reports
	// ~20ms for what was really ~120ms. Overload would look fast.
	//
	// The protocol pins it down instead: dispatchChunk emits `partial`
	// then `ack` for the same chunk, in that order, over one ordered
	// socket. So the next ack's seq identifies the frame that completed
	// the chunk this partial describes, and sentAt gives that frame's
	// write time exactly. Hold the partial until that ack arrives.
	pendingPartial *event

	// Finals are deduplicated per stream rather than per connection,
	// because M4's endpointing means finals now arrive mid-session too,
	// not only in response to session.end.
	seenFinal map[string]bool
}

func (s *stream) runWithDelay(ctx context.Context, delay time.Duration) {
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
	}

	// The run's deadline is measured from the moment this stream starts
	// generating, not from process start, so every stream gets the full
	// --duration regardless of where it landed in the ramp. A ramp that
	// ate into the measurement window would make late streams contribute
	// fewer samples than early ones and quietly bias every percentile.
	deadline := time.Now().Add(s.cfg.duration)
	s.sentAt = make(map[uint64]time.Time)
	s.seenFinal = make(map[string]bool)
	s.seq = 1

	for time.Now().Before(deadline) && ctx.Err() == nil {
		// One "leg" is one connection's worth of work: the whole run when
		// --reconnect-every is 0, or one slice of it when reconnecting.
		legEnd := deadline
		if s.cfg.reconnectEvery > 0 {
			if until := time.Now().Add(s.cfg.reconnectEvery); until.Before(legEnd) {
				legEnd = until
			}
		}
		if !s.runLeg(ctx, legEnd, legEnd.Equal(deadline)) {
			return // refused or errored; the counters and CSV already record why
		}
	}
}

// runLeg holds one connection open until legEnd. closeSession is true on
// the final leg, where the stream sends session.end and waits for its
// final; on an intermediate leg it just drops the socket, which is what
// makes --reconnect-every a reconnect rather than a new session.
//
// Returns false when the stream should stop entirely.
func (s *stream) runLeg(ctx context.Context, legEnd time.Time, closeSession bool) bool {
	dialedAt := time.Now()
	dialCtx, cancelDial := context.WithTimeout(ctx, 10*time.Second)
	conn, _, err := websocket.Dial(dialCtx, s.cfg.wsURL, nil)
	cancelDial()
	if err != nil {
		s.cnt.errors.Add(1)
		s.emit("error", -1, 0, false, nil)
		return false
	}

	// One reader goroutine on the connection's full-lifetime context,
	// feeding a channel. NOT a short-lived context per read: canceling a
	// coder/websocket Read closes the whole connection, not just that
	// call. This project has hit that twice (cmd/smoketest at M1,
	// cmd/chaostest at M3) and it presents as an unexplained
	// "broken pipe" mid-stream.
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	incoming := make(chan map[string]any, 256)
	go readLoop(connCtx, conn, incoming)

	defer conn.Close(websocket.StatusNormalClosure, "")

	if !s.sendSessionStart(ctx, conn) {
		return false
	}
	if !s.awaitOpen(ctx, incoming, dialedAt) {
		return false
	}
	s.cnt.streamsActive.Add(1)
	defer s.cnt.streamsActive.Add(-1)

	if !s.pump(ctx, conn, incoming, legEnd) {
		return false
	}

	if !closeSession {
		return true // intermediate leg: drop the socket, resume on the next one
	}
	return s.finish(ctx, conn, incoming)
}

func readLoop(ctx context.Context, conn *websocket.Conn, out chan<- map[string]any) {
	defer close(out)
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var ev map[string]any
		if json.Unmarshal(data, &ev) != nil {
			continue
		}
		select {
		case out <- ev:
		case <-ctx.Done():
			return
		}
	}
}

func (s *stream) sendSessionStart(ctx context.Context, conn *websocket.Conn) bool {
	body, _ := json.Marshal(wire.SessionStart{
		Type:           "session.start",
		Mode:           s.cfg.mode,
		SampleRateHz:   sampleRateHz,
		Encoding:       wire.RequiredEncoding,
		Channels:       wire.RequiredChannels,
		NominalFrameMs: nominalFrameMs,
	})
	f := wire.Frame{Type: wire.MsgControl, Seq: s.seq, Payload: body}
	if err := conn.Write(ctx, websocket.MessageBinary, wire.Encode(f)); err != nil {
		s.cnt.errors.Add(1)
		s.emit("error", int64(s.seq), 0, false, nil)
		return false
	}
	s.seq++
	return true
}

// awaitOpen waits for the ack that confirms session.start, or for the
// `overloaded` that says admission was refused. Refusal is an ordinary,
// expected outcome under load, recorded and counted rather than treated
// as a failure — that distinction is the whole point of scenario 9.
func (s *stream) awaitOpen(ctx context.Context, incoming <-chan map[string]any, dialedAt time.Time) bool {
	timeout := time.After(15 * time.Second)
	for {
		select {
		case ev, ok := <-incoming:
			if !ok {
				s.cnt.errors.Add(1)
				s.emit("error", -1, 0, false, nil)
				return false
			}
			switch eventType(ev) {
			case "ack":
				s.cnt.streamsOpened.Add(1)
				s.emit("session_open", -1, msSince(dialedAt), true, ev)
				return true
			case "overloaded":
				// Both counters, and they answer different questions:
				// `overloaded` counts refusal EVENTS wherever they occur,
				// `streams_refused` counts streams that never got a
				// session at all. Only the second is comparable against
				// --streams, which is what a capacity run reads.
				s.cnt.overloaded.Add(1)
				s.cnt.streamsRefused.Add(1)
				s.emit("overloaded", -1, msSince(dialedAt), true, ev)
				return false
			case "error":
				s.cnt.errors.Add(1)
				s.emit("error", -1, 0, false, ev)
				return false
			}
		case <-timeout:
			s.cnt.errors.Add(1)
			s.emit("error", -1, 0, false, nil)
			return false
		case <-ctx.Done():
			return false
		}
	}
}

// pump is the paced send loop: one audio frame every nominalFrameMs of
// wall clock, drained of inbound events between frames. Pacing is by
// ticker rather than by sleeping for the frame duration after each write,
// so the time spent writing does not accumulate into drift over a long
// run.
func (s *stream) pump(ctx context.Context, conn *websocket.Conn, incoming <-chan map[string]any, legEnd time.Time) bool {
	ticker := time.NewTicker(nominalFrameMs * time.Millisecond)
	defer ticker.Stop()

	offset := 0 // byte offset into the speech clip
	silenceRemaining := 0
	silence := make([]byte, frameBytes)

	for time.Now().Before(legEnd) {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}

		s.drain(incoming)

		var payload []byte
		if silenceRemaining > 0 {
			// Digital silence, still framed, sequenced and sent. The VAD
			// (internal/audio) is what decides this costs no backend call —
			// the client does not get to skip it, because a client that
			// stopped sending would be testing a different thing entirely.
			payload = silence
			silenceRemaining -= frameBytes
		} else {
			if offset+frameBytes > len(s.clip.PCM) {
				offset = 0
				if s.cfg.speechRatio < 1.0 {
					// Interleave a silence run sized so that speech is
					// speechRatio of the total: one clip of speech is
					// followed by clip*(1-r)/r of silence.
					total := float64(len(s.clip.PCM)) / s.cfg.speechRatio
					silenceRemaining = int(total) - len(s.clip.PCM)
					silenceRemaining -= silenceRemaining % frameBytes // whole frames only; s16le must stay sample-aligned
					continue
				}
			}
			payload = s.clip.PCM[offset : offset+frameBytes]
			offset += frameBytes
		}

		if s.cfg.jitterMs > 0 {
			time.Sleep(time.Duration(s.rng.Intn(s.cfg.jitterMs+1)) * time.Millisecond)
		}

		seq := s.seq
		s.seq++

		if s.cfg.dropPct > 0 && s.rng.Float64() < s.cfg.dropPct {
			// Consume the sequence number without sending: the gap is the
			// point. The gateway must notice and emit `discontinuity`
			// rather than silently renumbering around it.
			s.cnt.framesDropped.Add(1)
			continue
		}

		f := wire.Frame{
			Type:       wire.MsgAudio,
			Seq:        seq,
			CaptureMs:  uint32(time.Since(s.start).Milliseconds()),
			NumSamples: frameSamples,
			Payload:    payload,
		}
		now := time.Now()
		if err := conn.Write(ctx, websocket.MessageBinary, wire.Encode(f)); err != nil {
			s.cnt.errors.Add(1)
			s.emit("error", int64(seq), 0, false, nil)
			return false
		}
		s.lastFrameAt = now
		s.sentAt[seq] = now
		s.cnt.framesSent.Add(1)
		s.cnt.audioMsSent.Add(nominalFrameMs)
	}
	return true
}

// finish sends session.end and waits for the final, which is the only
// event whose latency is measured from a specific write rather than from
// the last audio frame.
func (s *stream) finish(ctx context.Context, conn *websocket.Conn, incoming <-chan map[string]any) bool {
	body, _ := json.Marshal(wire.SessionEnd{Type: "session.end"})
	f := wire.Frame{Type: wire.MsgControl, Seq: s.seq, Payload: body}
	endAt := time.Now()
	if err := conn.Write(ctx, websocket.MessageBinary, wire.Encode(f)); err != nil {
		s.cnt.errors.Add(1)
		s.emit("error", int64(s.seq), 0, false, nil)
		return false
	}
	s.seq++

	timeout := time.After(finalWaitTimeout)
	for {
		select {
		case ev, ok := <-incoming:
			if !ok {
				s.emit("session_close", -1, msSince(endAt), true, nil)
				return false
			}
			if eventType(ev) == "final" {
				// Any partial still held has no ack coming — session.end
				// closed the stream. Flush it on the last-frame estimate
				// rather than losing the sample.
				s.flushPartial(msSince(s.lastFrameAt), !s.lastFrameAt.IsZero())
				s.countFinal(ev)
				lat := msSince(endAt)
				s.cnt.finalLatencies.add(lat)
				s.emit("final", -1, lat, true, ev)
				s.emit("session_close", -1, msSince(endAt), true, nil)
				return true
			}
			s.record(ev)
		case <-timeout:
			s.cnt.errors.Add(1)
			s.emit("error", -1, 0, false, nil)
			return false
		case <-ctx.Done():
			return false
		}
	}
}

// drain consumes whatever has arrived without blocking. A blocking read
// between frames would couple this stream's pacing to the gateway's
// response timing, which is the thing being measured.
func (s *stream) drain(incoming <-chan map[string]any) {
	for {
		select {
		case ev, ok := <-incoming:
			if !ok {
				return
			}
			s.record(ev)
		default:
			return
		}
	}
}

func (s *stream) record(ev map[string]any) {
	switch eventType(ev) {
	case "ack":
		seq := intField(ev, "highest_contiguous_seq")
		if seq > 0 {
			s.lastAckedSeq = uint64(seq)
		}
		sent, known := s.sentAt[uint64(seq)]
		lat := 0.0
		if known {
			lat = msSince(sent)
		}
		// Resolve the partial this ack belongs to first, so both rows
		// carry the same, seq-accurate latency. See pendingPartial.
		s.flushPartial(lat, known)
		if known {
			// Everything at or below an acked seq is durably accepted, so
			// its send time can never be needed again. Without this the
			// map grows for the whole run — at 50 frames/sec/stream that
			// is a slow leak that only shows up in the long soak runs.
			for k := range s.sentAt {
				if k <= uint64(seq) {
					delete(s.sentAt, k)
				}
			}
		}
		s.emit("ack", seq, lat, known, ev)
	case "partial":
		// Held, not emitted: the ack that follows carries the seq this
		// partial's latency must be measured against.
		s.flushPartial(msSince(s.lastFrameAt), !s.lastFrameAt.IsZero()) // an unpaired predecessor, if any
		e := s.buildEvent("partial", -1, ev)
		s.pendingPartial = &e
	case "partial.reset":
		s.flushPartial(msSince(s.lastFrameAt), !s.lastFrameAt.IsZero())
		s.emit("partial.reset", -1, msSince(s.lastFrameAt), !s.lastFrameAt.IsZero(), ev)
	case "discontinuity":
		s.cnt.discontinuities.Add(1)
		s.emit("discontinuity", -1, 0, false, ev)
	case "overloaded":
		s.cnt.overloaded.Add(1)
		s.emit("overloaded", -1, 0, false, ev)
	case "error":
		s.cnt.errors.Add(1)
		s.emit("error", -1, 0, false, ev)
	case "final":
		// Reachable mid-session since M4: endpointing closes an utterance
		// on a detected pause, not only on session.end.
		s.countFinal(ev)
		s.emit("final", -1, 0, false, ev)
	}
}

// flushPartial emits a held `partial` with the latency the following ack
// established. Falls back to the last-frame estimate when a partial was
// never followed by an ack — which happens at session end — rather than
// dropping the sample.
func (s *stream) flushPartial(latencyMs float64, hasLatency bool) {
	if s.pendingPartial == nil {
		return
	}
	e := *s.pendingPartial
	s.pendingPartial = nil
	e.latencyMs, e.hasLatency = latencyMs, hasLatency
	if hasLatency {
		s.cnt.partialLatencies.add(latencyMs)
	}
	select {
	case s.events <- e:
	default:
	}
}

func (s *stream) countFinal(ev map[string]any) {
	uid, _ := ev["utterance_id"].(string)
	if s.seenFinal[uid] {
		// Counted client-side and independently of the gateway's own
		// duplicate_finals_total. Two observers of an invariant that must
		// stay zero beat one.
		s.cnt.duplicateFinals.Add(1)
	}
	s.seenFinal[uid] = true
	s.cnt.finals.Add(1)
}

// buildEvent snapshots a CSV row at the moment the event was observed.
// Separate from emit so a held partial records ITS timestamp and
// concurrency level, not the later ack's.
func (s *stream) buildEvent(kind string, seq int64, ev map[string]any) event {
	e := event{
		tsMs:          time.Since(s.start).Milliseconds(),
		streamID:      s.id,
		kind:          kind,
		seq:           seq,
		streamsActive: s.cnt.streamsActive.Load(),
		revision:      -1,
		failoverEpoch: -1,
	}
	if ev != nil {
		e.utteranceID, _ = ev["utterance_id"].(string)
		if v, ok := ev["revision"]; ok {
			e.revision = toInt(v)
		}
		if v, ok := ev["failover_epoch"]; ok {
			e.failoverEpoch = toInt(v)
		}
	}
	return e
}

func (s *stream) emit(kind string, seq int64, latencyMs float64, hasLatency bool, ev map[string]any) {
	e := s.buildEvent(kind, seq, ev)
	e.latencyMs, e.hasLatency = latencyMs, hasLatency
	// Never block a paced stream on the CSV writer. A dropped row costs
	// one sample; a stalled stream corrupts the pacing of every
	// measurement that follows it.
	select {
	case s.events <- e:
	default:
	}
}

func eventType(ev map[string]any) string {
	t, _ := ev["type"].(string)
	return t
}

func intField(ev map[string]any, key string) int64 {
	v, ok := ev[key]
	if !ok {
		return -1
	}
	return toInt(v)
}

func toInt(v any) int64 {
	switch n := v.(type) {
	case float64: // encoding/json decodes every number into float64
		return int64(n)
	case int64:
		return n
	default:
		return -1
	}
}

func msSince(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(time.Since(t).Microseconds()) / 1000
}
