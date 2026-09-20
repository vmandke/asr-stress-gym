package dash

import (
	"testing"
	"time"

	"asr-stress-gym/internal/session"
)

// A nil Hub is the configuration cmd/gateway's own tests run in, and the
// gateway calls into it from the write path on every event. If any of
// these panics, every gateway test panics with it.
func TestNilHubIsFullyUsable(t *testing.T) {
	var h *Hub
	h.SessionStarted("s-1", "online", "worker-a", "key-1")
	h.Observe(session.PartialEvent{Type: "partial", SessionID: "s-1", Text: "hi"})
	h.PushObserved("s-1", "worker-a", 5*time.Millisecond)
	h.Failover("s-1", "worker-a", "worker-b", "k1", "k1", true)
	h.Refused("s-2", "over capacity")
	h.Chaos("worker-a", "kill", "SIGKILL")
	h.SessionEnded("s-1", "closed")

	if got := h.Streams(); got != nil {
		t.Errorf("Streams() on nil hub = %v, want nil", got)
	}
	if _, ok := h.Log("s-1"); ok {
		t.Error("Log() on nil hub reported a hit")
	}
	if got := h.Since(0); got != nil {
		t.Errorf("Since() on nil hub = %v, want nil", got)
	}
	// Snapshot is called by the SSE loop four times a second; it must
	// produce a usable frame even with no hub behind it.
	if snap := h.Snapshot(nil); snap.AtMs == 0 {
		t.Error("Snapshot() on nil hub produced an unstamped frame")
	}
}

func TestObserveTracksStreamState(t *testing.T) {
	h := NewHub()
	h.SessionStarted("s-1", "online", "worker-a", "abcdef0123456789")
	h.Observe(session.SpeechStartEvent{Type: "speech.start", SessionID: "s-1", UtteranceID: "u1", SeqStart: 10})
	h.Observe(session.PartialEvent{Type: "partial", SessionID: "s-1", UtteranceID: "u1", Revision: 1, Text: "transfer five"})
	h.Observe(session.PartialEvent{Type: "partial", SessionID: "s-1", UtteranceID: "u1", Revision: 2, Text: "transfer five thousand"})
	h.Observe(session.AckEvent{Type: "ack", SessionID: "s-1", HighestContiguousSeq: 42})

	streams := h.Streams()
	if len(streams) != 1 {
		t.Fatalf("got %d streams, want 1", len(streams))
	}
	s := streams[0]
	if s.State != StateSpeech {
		t.Errorf("state = %q, want %q", s.State, StateSpeech)
	}
	if s.Revision != 2 || s.Partials != 2 {
		t.Errorf("rev=%d partials=%d, want 2 and 2", s.Revision, s.Partials)
	}
	if s.LastSeq != 42 {
		t.Errorf("last_seq = %d, want 42 (the ack must still update the row)", s.LastSeq)
	}
	if s.LastText != "transfer five thousand" {
		t.Errorf("last_text = %q", s.LastText)
	}
}

// Acks are one per push and carry a number the row already tracks. If they
// entered the log, a 256-entry ring would hold ~40s of history instead of
// several utterances — and the failover the inspector exists to show would
// have scrolled off by the time anyone looked.
func TestAcksNeverEnterTheStreamLog(t *testing.T) {
	h := NewHub()
	h.SessionStarted("s-1", "online", "worker-a", "k")
	for i := 0; i < 50; i++ {
		h.Observe(session.AckEvent{Type: "ack", SessionID: "s-1", HighestContiguousSeq: uint64(i)})
	}
	log, ok := h.Log("s-1")
	if !ok {
		t.Fatal("no log for s-1")
	}
	for _, ev := range log {
		if ev.Kind == "ack" {
			t.Fatalf("ack found in stream log: %+v", ev)
		}
	}
	if len(log) != 1 { // session.start only
		t.Errorf("log has %d entries, want 1 (session.start)", len(log))
	}
}

func TestFailoverRecordsKeyComparison(t *testing.T) {
	h := NewHub()
	h.SessionStarted("s-1", "online", "worker-a", "3f9a1111")
	h.Failover("s-1", "worker-a", "worker-c", "3f9a1111", "b7212222", false)

	streams := h.Streams()
	if streams[0].Worker != "worker-c" {
		t.Errorf("worker = %q, want worker-c", streams[0].Worker)
	}
	if streams[0].State != StateFailing {
		t.Errorf("state = %q, want %q", streams[0].State, StateFailing)
	}
	if streams[0].Failovers != 1 {
		t.Errorf("failovers = %d, want 1", streams[0].Failovers)
	}

	log, _ := h.Log("s-1")
	last := log[len(log)-1]
	if last.Kind != "failover.cross" {
		t.Errorf("kind = %q, want failover.cross", last.Kind)
	}
	// The log line is the evidence for invariants 6 and 7. A reviewer
	// reading it should not have to look anything else up.
	if want := "worker-a=3f9a1111 worker-c=b7212222 MISMATCH"; last.Detail != want {
		t.Errorf("detail = %q, want %q", last.Detail, want)
	}
}

// A push landing on the new worker is what proves recovery completed, so
// it is what clears the FAILING_OVER chip. Without this the chip is sticky
// and the dashboard shows a permanently-recovering stream.
func TestPushAfterFailoverClearsTheFailingChip(t *testing.T) {
	h := NewHub()
	h.SessionStarted("s-1", "online", "worker-a", "k1")
	h.Failover("s-1", "worker-a", "worker-b", "k1", "k1", true)
	if h.Streams()[0].State != StateFailing {
		t.Fatal("expected FAILING_OVER immediately after failover")
	}
	h.PushObserved("s-1", "worker-b", 12*time.Millisecond)
	if got := h.Streams()[0].State; got != StateSpeech {
		t.Errorf("state after a successful push = %q, want %q", got, StateSpeech)
	}
}

func TestFinalIsAttributedLatencyFromSpeechStart(t *testing.T) {
	h := NewHub()
	h.SessionStarted("s-1", "online", "worker-a", "k")
	h.Observe(session.SpeechStartEvent{Type: "speech.start", SessionID: "s-1", UtteranceID: "u1"})
	time.Sleep(5 * time.Millisecond)
	h.Observe(session.FinalEvent{Type: "final", SessionID: "s-1", UtteranceID: "u1", Text: "done"})

	snap := h.Snapshot(nil)
	if snap.Final.Count != 1 {
		t.Fatalf("final latency count = %d, want 1", snap.Final.Count)
	}
	if snap.Final.P50 < 4 {
		t.Errorf("final p50 = %.1fms, want >= ~5ms", snap.Final.P50)
	}
	// The utterance-start bookkeeping must not accumulate: it is keyed by
	// session+utterance and a long session has many utterances.
	h.mu.Lock()
	n := len(h.uttStart)
	h.mu.Unlock()
	if n != 0 {
		t.Errorf("uttStart retained %d entries after the final", n)
	}
}

// This is the invariant the whole package rests on: the gateway's write
// goroutine calls Observe, so a subscriber that has stopped reading must
// never be able to block it.
func TestBroadcastNeverBlocksOnAStalledSubscriber(t *testing.T) {
	h := NewHub()
	_, ch := h.subscribe()
	_ = ch // deliberately never drained

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.SessionStarted("s-1", "online", "worker-a", "k")
		// Far more notable events than the 256-deep subscriber buffer.
		for i := 0; i < 5000; i++ {
			h.Observe(session.SpeechStartEvent{Type: "speech.start", SessionID: "s-1", UtteranceID: "u1"})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("broadcast blocked on a subscriber that stopped reading")
	}
}

func TestSinceReplaysOnlyNewerNotableEvents(t *testing.T) {
	h := NewHub()
	h.SessionStarted("s-1", "online", "worker-a", "k")
	h.Chaos("worker-a", "kill", "SIGKILL")
	h.Chaos("worker-a", "restore", "respawn")

	all := h.Since(0)
	if len(all) != 3 {
		t.Fatalf("Since(0) returned %d events, want 3", len(all))
	}
	rest := h.Since(all[0].ID)
	if len(rest) != 2 || rest[0].ID != all[1].ID {
		t.Errorf("Since(%d) returned %d events starting at %d", all[0].ID, len(rest), rest[0].ID)
	}
	if got := h.Since(all[len(all)-1].ID); len(got) != 0 {
		t.Errorf("Since(latest) returned %d events, want 0", len(got))
	}
}

// Ending a stream must keep it readable — the interesting moment is
// usually just after a session died, and a row that vanishes on the tick
// it ends cannot be inspected at all.
func TestEndedStreamsStayInspectable(t *testing.T) {
	h := NewHub()
	h.SessionStarted("s-1", "online", "worker-a", "k")
	h.Observe(session.ErrorEvent{Type: "error", SessionID: "s-1", Reason: "failover exhausted"})
	h.SessionEnded("s-1", "closed")

	streams := h.Streams()
	if len(streams) != 1 || streams[0].State != StateEnded {
		t.Fatalf("streams = %+v, want one ENDED row", streams)
	}
	log, ok := h.Log("s-1")
	if !ok {
		t.Fatal("log unavailable for an ended stream")
	}
	var sawError bool
	for _, ev := range log {
		if ev.Kind == "error" {
			sawError = true
		}
	}
	if !sawError {
		t.Error("the error that ended the session is missing from its log")
	}
}

func TestEndedStreamsAreBounded(t *testing.T) {
	h := NewHub()
	for i := 0; i < endedCapacity*3; i++ {
		id := "s-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		h.SessionStarted(id, "online", "worker-a", "k")
		h.SessionEnded(id, "closed")
	}
	h.mu.Lock()
	n := len(h.ended)
	h.mu.Unlock()
	if n != endedCapacity {
		t.Errorf("retained %d ended streams, want %d", n, endedCapacity)
	}
}

func TestStreamLogIsBounded(t *testing.T) {
	h := NewHub()
	h.SessionStarted("s-1", "online", "worker-a", "k")
	for i := 0; i < streamLogCapacity*3; i++ {
		h.Observe(session.PartialEvent{Type: "partial", SessionID: "s-1", Revision: uint64(i), Text: "x"})
	}
	log, _ := h.Log("s-1")
	if len(log) != streamLogCapacity {
		t.Errorf("log length = %d, want %d", len(log), streamLogCapacity)
	}
	// Oldest-evicted, so the newest event survives.
	if last := log[len(log)-1]; last.Revision != uint64(streamLogCapacity*3-1) {
		t.Errorf("newest retained revision = %d", last.Revision)
	}
}

func TestPercentilesAreNearestRank(t *testing.T) {
	w := newWindow(100)
	for i := 1; i <= 100; i++ {
		w.add(float64(i))
	}
	p50, p95, p99, n := w.percentiles()
	if n != 100 {
		t.Fatalf("count = %d, want 100", n)
	}
	// Nearest-rank: every reported value is a sample that actually
	// occurred, so these are exact, not interpolated.
	if p50 != 51 || p95 != 96 || p99 != 100 {
		t.Errorf("p50/p95/p99 = %v/%v/%v, want 51/96/100", p50, p95, p99)
	}
}

func TestEmptyWindowReportsZeroSamplesRatherThanZeroLatency(t *testing.T) {
	_, _, _, n := newWindow(16).percentiles()
	if n != 0 {
		t.Errorf("count = %d, want 0", n)
	}
}

func TestWindowWrapsWithoutGrowing(t *testing.T) {
	w := newWindow(8)
	for i := 0; i < 100; i++ {
		w.add(float64(i))
	}
	if got := len(w.samples()); got != 8 {
		t.Errorf("samples = %d, want 8", got)
	}
	p50, _, _, _ := w.percentiles()
	if p50 < 92 {
		t.Errorf("p50 = %v; the window should hold only the newest 8 samples (92..99)", p50)
	}
}

func TestSnapshotBoundsTheStreamListButReportsTheTotal(t *testing.T) {
	h := NewHub()
	for i := 0; i < maxStreamsInSnapshot+25; i++ {
		h.SessionStarted("s-"+string(rune('a'+i%26))+string(rune('0'+i/26)), "online", "worker-a", "k")
	}
	snap := h.Snapshot(nil)
	if len(snap.Streams) != maxStreamsInSnapshot {
		t.Errorf("snapshot carried %d rows, want %d", len(snap.Streams), maxStreamsInSnapshot)
	}
	if snap.StreamsTotal != maxStreamsInSnapshot+25 {
		t.Errorf("streams_total = %d, want %d", snap.StreamsTotal, maxStreamsInSnapshot+25)
	}
}
