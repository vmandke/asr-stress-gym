package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"asr-stress-gym/internal/wire"
)

// --- a stateful fake worker, mirroring worker/server.py's push semantics
// (idempotent replay, generation compare-and-commit) closely enough to
// exercise the gateway for real, without a Python process in the loop. ---

type fakeWorkerRecord struct {
	mu             sync.Mutex
	generation     uint64
	lastSeqApplied uint64
	lastText       string
	pushCount      int
}

func newFakeWorker(t *testing.T) *httptest.Server {
	t.Helper()
	var (
		mu      sync.Mutex
		records = map[string]*fakeWorkerRecord{}
		nextID  = 0
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream/open", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		nextID++
		handle := fmt.Sprintf("fake-handle-%d", nextID)
		records[handle] = &fakeWorkerRecord{}
		mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{
			"handle":                 handle,
			"compatibility_key_hash": "sha256:fake",
			"capabilities": map[string]any{
				"streaming": true, "serializable": true, "endpointing": false,
				"modes": []string{"online", "offline"}, "min_chunk_ms": 20, "max_chunk_ms": 5000,
			},
			"generation": 0,
		})
	})

	mux.HandleFunc("/v1/stream/push", func(w http.ResponseWriter, r *http.Request) {
		handle := r.Header.Get("X-Handle")
		var seqEnd, expectedGen uint64
		fmt.Sscanf(r.Header.Get("X-Seq-End"), "%d", &seqEnd)
		fmt.Sscanf(r.Header.Get("X-Expected-Generation"), "%d", &expectedGen)

		mu.Lock()
		rec := records[handle]
		mu.Unlock()
		if rec == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		rec.mu.Lock()
		defer rec.mu.Unlock()
		if seqEnd <= rec.lastSeqApplied {
			json.NewEncoder(w).Encode(map[string]any{
				"text": rec.lastText, "last_seq_applied": rec.lastSeqApplied, "generation": rec.generation,
			})
			return
		}
		if rec.generation != expectedGen {
			w.WriteHeader(http.StatusConflict)
			return
		}
		rec.pushCount++
		rec.generation++
		rec.lastSeqApplied = seqEnd
		rec.lastText = fmt.Sprintf("mock%d", rec.pushCount)
		json.NewEncoder(w).Encode(map[string]any{
			"text": rec.lastText, "last_seq_applied": rec.lastSeqApplied, "generation": rec.generation,
		})
	})

	mux.HandleFunc("/v1/stream/flush", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Handle string `json:"handle"` }
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		rec := records[body.Handle]
		mu.Unlock()
		text := ""
		if rec != nil {
			rec.mu.Lock()
			text = rec.lastText
			rec.mu.Unlock()
		}
		json.NewEncoder(w).Encode(map[string]any{"text": text, "final": true})
	})

	mux.HandleFunc("/v1/stream/close", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Handle string `json:"handle"` }
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		delete(records, body.Handle)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"worker_id": "fake", "status": "READY"})
	})

	return httptest.NewServer(mux)
}

// --- test harness: a real gateway ws endpoint over the fake worker ---

func newTestGateway(t *testing.T) string {
	t.Helper()
	worker := newFakeWorker(t)
	t.Cleanup(worker.Close)

	cfg := connConfig{workerBaseURL: worker.URL}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Logf("ws accept: %v", err)
			return
		}
		handleConnection(r.Context(), ws, cfg)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + srv.URL[len("http"):] + "/ws"
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	return c
}

func sendControl(t *testing.T, c *websocket.Conn, seq uint64, payload string) {
	t.Helper()
	f := wire.Frame{Type: wire.MsgControl, Seq: seq, Payload: []byte(payload)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageBinary, wire.Encode(f)); err != nil {
		t.Fatalf("send control: %v", err)
	}
}

func sendAudio(t *testing.T, c *websocket.Conn, seq uint64, durationMs float64) {
	t.Helper()
	numSamples := uint32(durationMs * 16000 / 1000)
	payload := make([]byte, int(numSamples)*wire.BytesPerSample)
	for i := range payload {
		payload[i] = byte(i) // non-zero content, not that anything inspects it
	}
	f := wire.Frame{Type: wire.MsgAudio, Seq: seq, NumSamples: numSamples, Payload: payload}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageBinary, wire.Encode(f)); err != nil {
		t.Fatalf("send audio: %v", err)
	}
}

// readEvent reads one JSON text event and returns it as a generic map,
// skipping non-text frames (there should be none on this hop).
func readEvent(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read event: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("got message type %v, want MessageText", typ)
	}
	var ev map[string]any
	if err := json.Unmarshal(data, &ev); err != nil {
		t.Fatalf("unmarshal event: %v (data=%s)", err, data)
	}
	return ev
}

const sessionStartJSON = `{"type":"session.start","mode":"online","sample_rate_hz":16000,"encoding":"pcm_s16le","channels":1,"nominal_frame_ms":20}`

// M1's own done-when bar (docs/build-plan.md Phase 1 / implementation-plan.md
// M1): one stream runs end to end.
func TestStreamEndToEnd(t *testing.T) {
	c := dial(t, newTestGateway(t))
	defer c.Close(websocket.StatusNormalClosure, "")

	sendControl(t, c, 0, sessionStartJSON)
	ack := readEvent(t, c)
	if ack["type"] != "ack" {
		t.Fatalf("got %v after session.start, want ack", ack)
	}
	sessionID, _ := ack["session_id"].(string)
	if sessionID == "" {
		t.Fatal("ack carried no session_id")
	}

	// 8 frames * 20ms = 160ms = exactly one chunk at the online chunkMs.
	for seq := uint64(1); seq <= 8; seq++ {
		sendAudio(t, c, seq, 20)
	}
	partial := readEvent(t, c)
	if partial["type"] != "partial" {
		t.Fatalf("got %v, want partial", partial)
	}
	if partial["session_id"] != sessionID {
		t.Fatalf("partial session_id = %v, want %v", partial["session_id"], sessionID)
	}
	if partial["revision"].(float64) != 1 {
		t.Fatalf("partial revision = %v, want 1", partial["revision"])
	}
	ack2 := readEvent(t, c)
	if ack2["type"] != "ack" {
		t.Fatalf("got %v, want ack", ack2)
	}

	sendControl(t, c, 9, `{"type":"session.end"}`)
	final := readEvent(t, c)
	if final["type"] != "final" {
		t.Fatalf("got %v, want final", final)
	}
	if final["session_id"] != sessionID {
		t.Fatalf("final session_id = %v, want %v", final["session_id"], sessionID)
	}
	if text, _ := final["text"].(string); text == "" {
		t.Fatal("final carried empty text")
	}
}

// "a deliberately dropped frame produces discontinuity"
func TestDroppedFrameProducesDiscontinuity(t *testing.T) {
	c := dial(t, newTestGateway(t))
	defer c.Close(websocket.StatusNormalClosure, "")

	sendControl(t, c, 0, sessionStartJSON)
	readEvent(t, c) // ack for session.start

	// seq 1 alone is 20ms, well under the 160ms online chunk threshold —
	// nothing is dispatched yet, so nothing is emitted for it. Only the
	// gap below produces an event.
	sendAudio(t, c, 1, 20)

	// Deliberately skip seq 2-4: jump straight to seq 5.
	sendAudio(t, c, 5, 20)
	disc := readEvent(t, c)
	if disc["type"] != "discontinuity" {
		t.Fatalf("got %v, want discontinuity", disc)
	}
	if disc["seq_start"].(float64) != 2 || disc["seq_end"].(float64) != 5 {
		t.Fatalf("discontinuity span = [%v,%v], want [2,5]", disc["seq_start"], disc["seq_end"])
	}
}

// "a frame whose declared duration disagrees with its payload is rejected"
func TestDurationMismatchIsRejected(t *testing.T) {
	c := dial(t, newTestGateway(t))
	defer c.Close(websocket.StatusNormalClosure, "")

	sendControl(t, c, 0, sessionStartJSON)
	readEvent(t, c) // ack

	// NumSamples says 320 (640 bytes expected) but the payload is only 4 bytes.
	badFrame := wire.Frame{Type: wire.MsgAudio, Seq: 1, NumSamples: 320, Payload: []byte{1, 2, 3, 4}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageBinary, wire.Encode(badFrame)); err != nil {
		t.Fatalf("send bad frame: %v", err)
	}

	ev := readEvent(t, c)
	if ev["type"] != "error" {
		t.Fatalf("got %v, want error", ev)
	}
}

// A CONTROL frame with an unparseable body is a protocol violation too,
// not silently ignored.
func TestMalformedControlIsRejected(t *testing.T) {
	c := dial(t, newTestGateway(t))
	defer c.Close(websocket.StatusNormalClosure, "")

	sendControl(t, c, 0, `not json at all`)
	ev := readEvent(t, c)
	if ev["type"] != "error" {
		t.Fatalf("got %v, want error", ev)
	}
}

// A locked-format violation in session.start means the session is never
// created (docs/PROTOCOL.md).
func TestWrongAudioFormatIsRejected(t *testing.T) {
	c := dial(t, newTestGateway(t))
	defer c.Close(websocket.StatusNormalClosure, "")

	sendControl(t, c, 0, `{"type":"session.start","mode":"online","sample_rate_hz":8000,"encoding":"pcm_s16le","channels":1}`)
	ev := readEvent(t, c)
	if ev["type"] != "error" {
		t.Fatalf("got %v, want error", ev)
	}
}

// Re-delivering session.end (or any redundant final trigger) must not
// double-finalize — invariant 9, exercised over the wire this time, not
// just at the internal/session unit level.
func TestFinalIsNotFollowedByFurtherEvents(t *testing.T) {
	c := dial(t, newTestGateway(t))
	defer c.Close(websocket.StatusNormalClosure, "")

	sendControl(t, c, 0, sessionStartJSON)
	readEvent(t, c) // ack
	sendControl(t, c, 1, `{"type":"session.end"}`)
	final := readEvent(t, c)
	if final["type"] != "final" {
		t.Fatalf("got %v, want final", final)
	}

	// The session loop has already returned; the socket should close
	// rather than accept further frames silently.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := c.Read(ctx)
	if err == nil {
		t.Fatal("expected the connection to close after final, but it stayed open")
	}
}
