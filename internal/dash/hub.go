// Package dash is the gateway's observation layer: it watches events that
// are being produced anyway, keeps a bounded amount of recent history, and
// serves it as a static dashboard, an SSE feed, and a small read API.
//
// Two rules shape everything in here.
//
// **It observes, it never participates.** No method blocks, allocates
// unboundedly, or returns an error the caller has to handle. The gateway's
// hot path calls into this package while holding no lock of its own and
// must be able to ignore the result. A dashboard that can slow down or
// deadlock the thing it is measuring is worse than no dashboard, because
// it makes the measurement wrong in exactly the overload regime the
// measurement exists for.
//
// **A nil *Hub is fully functional.** Every method is nil-receiver-safe,
// like internal/bifrost.Client, so cmd/gateway's tests and any future
// headless build wire a nil Hub and need no enabled/disabled branching at
// the ~8 call sites.
package dash

import (
	"sort"
	"sync"
	"time"

	"asr-stress-gym/internal/session"
)

const (
	// Per-stream inspector history. ~256 events is several utterances of
	// partials at the 160ms cadence — enough to show a failover with
	// context on both sides of it, which is what the inspector is for.
	streamLogCapacity = 256

	// Global ring of NOTABLE events, replayed to an SSE client that
	// reconnects with Last-Event-ID. Sized so a browser that drops for a
	// few seconds under load still resumes without a visible gap.
	notableCapacity = 512

	// Ended streams retained so the inspector can still be opened on a
	// session that just finished — the interesting moment is usually
	// *after* the session died, and a row that vanishes the instant it
	// ends is unreadable.
	endedCapacity = 64

	// Latency samples per series. Copy-and-sort at snapshot time over a
	// few thousand float64s costs microseconds every 250ms; a streaming
	// quantile sketch would be more code for no measurable gain at this
	// size.
	latencyCapacity = 4096
)

// Stream state chips, as rendered in the stream list.
const (
	StateListening = "LISTENING"
	StateSpeech    = "SPEECH"
	StateFailing   = "FAILING_OVER"
	StateFinal     = "FINALISING"
	StateEnded     = "ENDED"
	StateRefused   = "REFUSED"
)

// Event is one thing that happened, flattened. Deliberately NOT the
// session.*Event types: those are the client protocol and must not acquire
// dashboard-shaped fields, and a flat struct with omitempty keeps the SSE
// payload small enough to send several hundred per second.
type Event struct {
	ID        uint64 `json:"id"`
	AtMs      int64  `json:"at_ms"`
	Session   string `json:"session,omitempty"`
	Kind      string `json:"kind"`
	Worker    string `json:"worker,omitempty"`
	Utterance string `json:"utt,omitempty"`
	Revision  uint64 `json:"rev,omitempty"`
	Epoch     uint64 `json:"epoch,omitempty"`
	SeqStart  uint64 `json:"seq_start,omitempty"`
	SeqEnd    uint64 `json:"seq_end,omitempty"`
	Text      string `json:"text,omitempty"`
	Detail    string `json:"detail,omitempty"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
}

// Stream is one live (or recently ended) session, as the stream list and
// the topology's edge rendering need it.
type Stream struct {
	ID            string `json:"id"`
	Mode          string `json:"mode"`
	Worker        string `json:"worker"`
	CompatKey     string `json:"compat_key"`
	State         string `json:"state"`
	Utterance     string `json:"utt"`
	Revision      uint64 `json:"rev"`
	FailoverEpoch uint64 `json:"failover_epoch"`
	Partials      int64  `json:"partials"`
	Finals        int64  `json:"finals"`
	Failovers     int64  `json:"failovers"`
	LastSeq       uint64 `json:"last_seq"`
	LastText      string `json:"last_text"`
	StartedAtMs   int64  `json:"started_at_ms"`
	EndedAtMs     int64  `json:"ended_at_ms,omitempty"`

	// --- the data-flow ledger, per stream -----------------------------
	//
	// What the inspector needs to answer "where did this audio actually
	// go". These are the three places a frame can end up, and they are
	// deliberately counted separately rather than derived from one
	// another: a frame is ALWAYS journaled, is dispatched to the worker
	// only if the VAD judged it voiced, and is rebuilt on replay only
	// from the journal.
	FramesIn      int64 `json:"frames_in"`       // frames accepted off the socket
	FramesVoiced  int64 `json:"frames_voiced"`   // ...of which the VAD marked as speech
	FramesGated   int64 `json:"frames_gated"`    // ...of which the VAD gated out (silence)
	AudioMsIn     int64 `json:"audio_ms_in"`     // wall-clock audio ingested
	ChunksOut     int64 `json:"chunks_out"`      // chunks actually dispatched to a worker
	ChunkBytesOut int64 `json:"chunk_bytes_out"` // PCM bytes actually sent to a worker
	ReplayedChunk int64 `json:"replayed_chunks"` // chunks re-sent by a recovery replay

	// --- where the state lives ---------------------------------------
	Handle         string `json:"handle"`          // the worker-side hot state's name
	Generation     uint64 `json:"generation"`      // the handle's generation (invariant 5)
	JournalRecords int    `json:"journal_records"` // recovery tier: frames retained gateway-side
	JournalSpanMs  int64  `json:"journal_span_ms"`
	// Warm tier. CheckpointSeq is how far the checkpoint covers, which is
	// the number that matters: the gap between it and LastSeq is exactly
	// the audio a same-model recovery would still have to replay.
	CheckpointSeq   uint64 `json:"checkpoint_seq"`
	CheckpointBytes int    `json:"checkpoint_bytes"`
	CheckpointWhy   string `json:"checkpoint_why"` // why there is none, when there is none
	Serializable    bool   `json:"serializable"`   // can this worker checkpoint at all?

	log []Event // ring, oldest-first once wrapped; see appendLog
}

// Hub is the single process-lifetime observation point. One per gateway,
// constructed in main and passed to every connection through connConfig.
type Hub struct {
	mu sync.Mutex

	streams map[string]*Stream
	ended   []*Stream // bounded FIFO of recently finished streams

	notable []Event // ring of broadcast-worthy events
	nextID  uint64

	subs    map[uint64]chan Event
	nextSub uint64

	pushLat  *window
	finalLat *window

	// Per-worker push counters. The dashboard differentiates these
	// against the previous snapshot to draw "backend calls/sec per
	// worker", which is what makes load redistribution after a kill
	// visible as a shape rather than a claim.
	workerPushes map[string]int64

	// Utterance start times, keyed session+utterance, so a final can be
	// attributed a latency. Cleared on final; bounded by live sessions.
	uttStart map[string]time.Time
}

func NewHub() *Hub {
	return &Hub{
		streams:      map[string]*Stream{},
		subs:         map[uint64]chan Event{},
		pushLat:      newWindow(latencyCapacity),
		finalLat:     newWindow(latencyCapacity),
		workerPushes: map[string]int64{},
		uttStart:     map[string]time.Time{},
	}
}

func nowMs() int64 { return time.Now().UnixNano() / 1e6 }

// --- explicit hooks -----------------------------------------------------
//
// Four facts the dashboard needs are NOT in any client event, because the
// client has no business knowing them: which worker is serving, what its
// compatibility key is, how long a backend push took, and that a failover
// swapped one worker for another. Those get explicit calls. Everything
// else arrives through Observe, below, at a single tap.

// SessionStarted records a session that has been admitted AND opened
// against a worker. Called after the Open succeeds, so a stream row never
// exists without a worker to attribute it to.
func (h *Hub) SessionStarted(id, mode, worker, compatKey string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	s := &Stream{
		ID: id, Mode: mode, Worker: worker, CompatKey: compatKey,
		State: StateListening, StartedAtMs: nowMs(),
	}
	h.streams[id] = s
	ev := h.recordLocked(s, Event{
		Session: id, Kind: "session.start", Worker: worker,
		Detail: mode + " key=" + shortKey(compatKey),
	}, true)
	h.mu.Unlock()
	h.broadcast(ev)
}

// SessionEnded closes the row out but keeps it readable — see endedCapacity.
func (h *Hub) SessionEnded(id, reason string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	s := h.streams[id]
	if s == nil {
		h.mu.Unlock()
		return
	}
	s.State = StateEnded
	s.EndedAtMs = nowMs()
	ev := h.recordLocked(s, Event{Session: id, Kind: "session.end", Detail: reason}, true)
	delete(h.streams, id)
	h.ended = append(h.ended, s)
	if len(h.ended) > endedCapacity {
		h.ended = h.ended[len(h.ended)-endedCapacity:]
	}
	for k := range h.uttStart {
		if len(k) > len(id) && k[:len(id)] == id {
			delete(h.uttStart, k)
		}
	}
	h.mu.Unlock()
	h.broadcast(ev)
}

// Failover records that a session moved workers. sameKey is passed in
// rather than derived by comparing the two key strings here, because the
// gateway has already made that determination (internal/coord) and a
// second, independent comparison could disagree with the metric counters —
// which is exactly the class of drift the dashboard exists to expose, not
// to create.
func (h *Hub) Failover(id, from, to, fromKey, toKey string, sameKey bool) {
	if h == nil {
		return
	}
	kind := "failover.cross"
	if sameKey {
		kind = "failover.same"
	}
	h.mu.Lock()
	s := h.streams[id]
	if s == nil {
		h.mu.Unlock()
		return
	}
	s.Worker = to
	s.CompatKey = toKey
	s.Failovers++
	s.State = StateFailing
	// The compat keys are carried in the detail string verbatim so the
	// inspector log line shows the MATCH/MISMATCH that invariants 6 and 7
	// are about. A reviewer reading that line should not have to look
	// anything else up to see why a checkpoint was or was not eligible.
	match := "MISMATCH"
	if sameKey {
		match = "MATCH"
	}
	ev := h.recordLocked(s, Event{
		Session: id, Kind: kind, Worker: to,
		Epoch:  s.FailoverEpoch,
		Detail: from + "=" + shortKey(fromKey) + " " + to + "=" + shortKey(toKey) + " " + match,
	}, true)
	h.mu.Unlock()
	h.broadcast(ev)
}

// FrameIngested records one accepted audio frame and what the VAD made of
// it. This is the only hot-path hub call — once per 20ms frame per stream
// — so it does the minimum under the lock and never allocates.
//
// Counting voiced and gated separately is the whole point: "frames in"
// alone cannot show that VAD gating is working, and the gated count is
// precisely the backend calls the system did not have to make.
func (h *Hub) FrameIngested(id string, durationMs float64, voiced bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if s := h.streams[id]; s != nil {
		s.FramesIn++
		s.AudioMsIn += int64(durationMs)
		if voiced {
			s.FramesVoiced++
		} else {
			s.FramesGated++
		}
	}
	h.mu.Unlock()
}

// ChunkDispatched records a chunk actually sent to a worker. replay marks
// chunks re-sent by a recovery, which must not be confused with fresh
// audio: the same bytes crossing the wire twice is the cost of failover,
// and showing it as new traffic would hide that.
func (h *Hub) ChunkDispatched(id string, bytes int, replay bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if s := h.streams[id]; s != nil {
		s.ChunksOut++
		s.ChunkBytesOut += int64(bytes)
		if replay {
			s.ReplayedChunk++
		}
	}
	h.mu.Unlock()
}

// StateObserved records where this session's state currently lives: the
// worker-side handle (hot), what the gateway has journaled (recovery), and
// whether a warm checkpoint exists.
//
// `why` explains an ABSENT checkpoint, and which of the two reasons
// applies matters: the adapter cannot serialize at all, or it can and
// none has been taken yet. Those look identical in a counter and are
// completely different facts — the first is permanent and expected on
// four of this fleet's six workers, the second is transient.
func (h *Hub) StateObserved(id, handle string, generation uint64, journalRecords int, journalSpanMs int64, cpSeq uint64, cpBytes int, why string, serializable bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if s := h.streams[id]; s != nil {
		s.Handle = handle
		s.Generation = generation
		s.JournalRecords = journalRecords
		s.JournalSpanMs = journalSpanMs
		s.CheckpointSeq = cpSeq
		s.CheckpointBytes = cpBytes
		s.CheckpointWhy = why
		s.Serializable = serializable
	}
	h.mu.Unlock()
}

// PushObserved records one successful backend push: its duration, and the
// worker that served it. This is the latency series that spikes on a kill,
// and it is measured where the gateway already measures it for the
// router's own latency reporting — no second clock.
func (h *Hub) PushObserved(id, worker string, d time.Duration) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.pushLat.add(float64(d.Microseconds()) / 1000)
	h.workerPushes[worker]++
	if s := h.streams[id]; s != nil {
		s.Worker = worker
		if s.State == StateFailing {
			s.State = StateSpeech // a push landed: recovery is complete
		}
	}
	h.mu.Unlock()
}

// Refused records an admission refusal. These sessions never reach
// SessionStarted — they have no worker and no handle — so they are logged
// as a bare notable event rather than becoming a stream row that would
// then have to be special-cased everywhere as "a stream with no worker".
func (h *Hub) Refused(id, reason string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	ev := h.recordLocked(nil, Event{Session: id, Kind: "overloaded", Detail: reason}, true)
	h.mu.Unlock()
	h.broadcast(ev)
}

// Chaos records a control-plane action taken from the dashboard, so the
// event log and the latency chart's fault markers come from the same
// stream as everything else. Without this a reviewer sees a spike with no
// annotated cause, which is precisely the reading error the markers exist
// to prevent.
func (h *Hub) Chaos(worker, action, detail string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	ev := h.recordLocked(nil, Event{Kind: "chaos", Worker: worker, Detail: action + " " + detail}, true)
	h.mu.Unlock()
	h.broadcast(ev)
}

// --- the single event tap ----------------------------------------------

// Observe is called once per event on its way to the client, from
// writeLoop. Everything the client is told, the dashboard sees, with no
// second emit site to keep in sync — which matters because the alternative
// (a hub call beside each of the ten Emitter call sites) would drift the
// first time someone adds an event type and forgets one.
//
// The argument is the typed event rather than its marshaled bytes so this
// costs a type switch, not a JSON parse, on the gateway's write path.
func (h *Hub) Observe(ev any) {
	if h == nil {
		return
	}
	var (
		rec      Event
		notable  bool
		finalLat time.Duration
	)

	h.mu.Lock()
	switch e := ev.(type) {
	case session.AckEvent:
		// Acks are the highest-volume event and the lowest-information:
		// one per push, carrying a number the stream row already tracks.
		// They update the row and are deliberately NOT appended to the
		// per-stream log — at 6/s they would otherwise evict the whole
		// 256-entry inspector history every 40 seconds, hiding the
		// failover the inspector exists to show.
		if s := h.streams[e.SessionID]; s != nil {
			s.LastSeq = e.HighestContiguousSeq
		}
		h.mu.Unlock()
		return

	case session.PartialEvent:
		s := h.streams[e.SessionID]
		if s != nil {
			s.Revision = e.Revision
			s.FailoverEpoch = e.FailoverEpoch
			s.Utterance = e.UtteranceID
			s.LastText = e.Text
			s.Partials++
			if s.State != StateFailing {
				s.State = StateSpeech
			}
		}
		// Partials are logged per-stream but NOT broadcast: at a few
		// hundred concurrent streams the partial firehose is thousands of
		// events per second of mostly-identical text, which would drown
		// the SSE feed and the browser both. The stream list's revision
		// counter carries the same information at snapshot cadence, and
		// the inspector fetches the real text on demand for the ONE
		// stream being looked at. See docs/ARCHITECTURE.md.
		rec = Event{Session: e.SessionID, Kind: "partial", Utterance: e.UtteranceID,
			Revision: e.Revision, Epoch: e.FailoverEpoch, Text: e.Text}
		if s != nil {
			rec.Worker = s.Worker
		}

	case session.PartialResetEvent:
		if s := h.streams[e.SessionID]; s != nil {
			s.Revision = e.Revision
			s.FailoverEpoch = e.FailoverEpoch
			s.LastText = ""
		}
		rec = Event{Session: e.SessionID, Kind: "partial.reset", Utterance: e.UtteranceID,
			Revision: e.Revision, Epoch: e.FailoverEpoch}
		notable = true

	case session.SpeechStartEvent:
		if s := h.streams[e.SessionID]; s != nil {
			s.State = StateSpeech
			s.Utterance = e.UtteranceID
		}
		h.uttStart[e.SessionID+"/"+e.UtteranceID] = time.Now()
		rec = Event{Session: e.SessionID, Kind: "speech.start", Utterance: e.UtteranceID, SeqStart: e.SeqStart}
		notable = true

	case session.FinalEvent:
		key := e.SessionID + "/" + e.UtteranceID
		if started, ok := h.uttStart[key]; ok {
			finalLat = time.Since(started)
			h.finalLat.add(float64(finalLat.Microseconds()) / 1000)
			delete(h.uttStart, key)
		}
		if s := h.streams[e.SessionID]; s != nil {
			s.Finals++
			s.State = StateListening
			s.LastText = e.Text
		}
		rec = Event{Session: e.SessionID, Kind: "final", Utterance: e.UtteranceID,
			SeqStart: e.SeqStart, SeqEnd: e.SeqEnd, Text: e.Text,
			LatencyMs: finalLat.Milliseconds()}
		notable = true

	case session.DiscontinuityEvent:
		rec = Event{Session: e.SessionID, Kind: "discontinuity", SeqStart: e.SeqStart, SeqEnd: e.SeqEnd}
		notable = true

	case session.ErrorEvent:
		if s := h.streams[e.SessionID]; s != nil {
			s.State = StateEnded
		}
		rec = Event{Session: e.SessionID, Kind: "error", Detail: e.Reason}
		notable = true

	case session.OverloadedEvent:
		// Already recorded by Refused at the decision point, which knows
		// the reason in structured form. Dropping it here avoids a
		// duplicate line in the log for one refusal.
		h.mu.Unlock()
		return

	default:
		h.mu.Unlock()
		return
	}

	out := h.recordLocked(h.streams[rec.Session], rec, notable)
	h.mu.Unlock()

	if notable {
		h.broadcast(out)
	}
}

// recordLocked stamps an event, appends it to the owning stream's log and,
// if notable, to the global ring. Caller holds h.mu. Returns the stamped
// event so the caller can broadcast it after releasing the lock —
// broadcasting under the lock would let one slow subscriber stall every
// gateway connection, which violates this package's first rule.
func (h *Hub) recordLocked(s *Stream, ev Event, notable bool) Event {
	h.nextID++
	ev.ID = h.nextID
	ev.AtMs = nowMs()
	if s != nil {
		s.appendLog(ev)
		if ev.Worker == "" {
			ev.Worker = s.Worker
		}
	}
	if notable {
		h.notable = append(h.notable, ev)
		if len(h.notable) > notableCapacity {
			h.notable = h.notable[len(h.notable)-notableCapacity:]
		}
	}
	return ev
}

func (s *Stream) appendLog(ev Event) {
	s.log = append(s.log, ev)
	if len(s.log) > streamLogCapacity {
		s.log = s.log[len(s.log)-streamLogCapacity:]
	}
}

// broadcast is non-blocking by construction: a subscriber whose buffer is
// full is skipped, not waited for. A browser that cannot keep up loses
// individual events and resynchronises from the next 250ms snapshot, which
// is a far better failure than back-pressuring a transcription session.
func (h *Hub) broadcast(ev Event) {
	h.mu.Lock()
	subs := make([]chan Event, 0, len(h.subs))
	for _, c := range h.subs {
		subs = append(subs, c)
	}
	h.mu.Unlock()

	for _, c := range subs {
		select {
		case c <- ev:
		default:
		}
	}
}

// --- reads --------------------------------------------------------------

// Streams returns live streams first, then recently ended ones, newest
// first within each group.
func (h *Hub) Streams() []Stream {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]Stream, 0, len(h.streams)+len(h.ended))
	for _, s := range h.streams {
		c := *s
		c.log = nil // the list view never carries per-stream history
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAtMs > out[j].StartedAtMs })
	for i := len(h.ended) - 1; i >= 0; i-- {
		c := *h.ended[i]
		c.log = nil
		out = append(out, c)
	}
	return out
}

// Log returns one stream's event history, including partials. Live streams
// are checked first, then the recently-ended ring.
func (h *Hub) Log(id string) ([]Event, bool) {
	if h == nil {
		return nil, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if s := h.streams[id]; s != nil {
		return append([]Event(nil), s.log...), true
	}
	for _, s := range h.ended {
		if s.ID == id {
			return append([]Event(nil), s.log...), true
		}
	}
	return nil, false
}

// Since returns notable events newer than id — what an SSE client that
// reconnects with Last-Event-ID needs to close its gap. A client asking
// for an id older than the ring gets whatever the ring still holds, which
// is the correct degradation: a visible jump beats a silent one.
func (h *Hub) Since(id uint64) []Event {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	out := []Event{}
	for _, e := range h.notable {
		if e.ID > id {
			out = append(out, e)
		}
	}
	return out
}

func (h *Hub) subscribe() (uint64, chan Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextSub++
	// Buffered deep enough to absorb a failover burst (reset + partials +
	// final across many streams at once) without dropping, but bounded:
	// see broadcast.
	c := make(chan Event, 256)
	h.subs[h.nextSub] = c
	return h.nextSub, c
}

func (h *Hub) unsubscribe(id uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subs, id)
}

// shortKey renders a compatibility key for display. The full key is a long
// opaque token; the dashboard's job is to make "these two differ" obvious
// at a glance, and eight characters does that without wrapping a node box.
func shortKey(k string) string {
	if len(k) <= 8 {
		return k
	}
	return k[:8]
}

// --- latency windows ----------------------------------------------------

// window is a fixed-size ring of samples with percentile reads. Not a
// time-based window: a fixed sample count means the percentiles stay
// meaningful at low traffic (where a 10s window might hold three samples)
// and bounded at high traffic, and the snapshot reports the count so a
// reader can tell which regime they are in.
type window struct {
	buf  []float64
	next int
	full bool
}

func newWindow(n int) *window { return &window{buf: make([]float64, n)} }

func (w *window) add(v float64) {
	w.buf[w.next] = v
	w.next = (w.next + 1) % len(w.buf)
	if w.next == 0 {
		w.full = true
	}
}

func (w *window) samples() []float64 {
	n := w.next
	if w.full {
		n = len(w.buf)
	}
	out := make([]float64, n)
	copy(out, w.buf[:n])
	return out
}

// Percentiles reports p50/p95/p99 and the sample count. Nearest-rank, not
// interpolated — with thousands of samples the difference is noise, and
// nearest-rank has the property that every reported value is a latency
// that actually occurred.
func (w *window) percentiles() (p50, p95, p99 float64, n int) {
	s := w.samples()
	if len(s) == 0 {
		return 0, 0, 0, 0
	}
	sort.Float64s(s)
	at := func(q float64) float64 {
		i := int(q * float64(len(s)))
		if i >= len(s) {
			i = len(s) - 1
		}
		return s[i]
	}
	return at(0.50), at(0.95), at(0.99), len(s)
}
