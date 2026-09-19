// Command smoketest is the M1 "done when" bar, run against a real,
// running gateway + worker (docker compose), not a fake:
//
//  1. one stream runs end to end from a corpus clip
//  2. a deliberately dropped frame produces discontinuity
//  3. a frame whose declared duration disagrees with its payload is rejected
//
// cmd/gateway's own tests already cover this logic exhaustively against a
// fake worker; this program's job is to catch what only shows up talking
// to the REAL Python worker — field-naming, status-code, and framing
// mismatches two independently hand-written sides of one protocol are
// exactly prone to. Exits non-zero on any failure, matching the chaos
// scripts' convention (build-plan.md: "a script with assertions and a
// non-zero exit on breach").
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/coder/websocket"

	"asr-stress-gym/internal/corpus"
	"asr-stress-gym/internal/wire"
)

func main() {
	wsURL := flag.String("ws-url", envOr("GATEWAY_WS_URL", "ws://localhost:7070/ws"), "gateway WebSocket URL")
	clipPath := flag.String("clip", envOr("SMOKETEST_CLIP", "corpus/01_short_greeting.wav"), "corpus WAV clip to stream")
	fast := flag.Bool("fast", false, "send frames back-to-back instead of at 20ms real-time pacing")
	flag.Parse()

	failed := false
	check := func(name string, err error) {
		if err != nil {
			log.Printf("FAIL %s: %v", name, err)
			failed = true
			return
		}
		log.Printf("PASS %s", name)
	}

	check("stream end to end", testStreamEndToEnd(*wsURL, *clipPath, *fast))
	check("dropped frame produces discontinuity", testDroppedFrameDiscontinuity(*wsURL))
	check("duration mismatch is rejected", testDurationMismatchRejected(*wsURL))

	if failed {
		os.Exit(1)
	}
	log.Println("smoketest: all checks passed")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

const sampleRateHz = 16000

func dial(ctx context.Context, wsURL string) (*websocket.Conn, error) {
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	return c, err
}

func writeFrame(ctx context.Context, c *websocket.Conn, f wire.Frame) error {
	return c.Write(ctx, websocket.MessageBinary, wire.Encode(f))
}

func readEvent(ctx context.Context, c *websocket.Conn) (map[string]any, error) {
	typ, data, err := c.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageText {
		return nil, fmt.Errorf("got message type %v, want text", typ)
	}
	var ev map[string]any
	if err := json.Unmarshal(data, &ev); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w (data=%s)", err, data)
	}
	return ev, nil
}

func sessionStart() string {
	return fmt.Sprintf(`{"type":"session.start","mode":"online","sample_rate_hz":%d,"encoding":"pcm_s16le","channels":1,"nominal_frame_ms":20}`, sampleRateHz)
}

// testStreamEndToEnd streams a real corpus clip and requires at least one
// partial and a non-empty final, with no error along the way.
func testStreamEndToEnd(wsURL, clipPath string, fast bool) error {
	clip, err := corpus.LoadWAV(clipPath)
	if err != nil {
		return fmt.Errorf("load %s: %w", clipPath, err)
	}
	if clip.SampleRateHz != sampleRateHz || clip.Channels != 1 || clip.BitsPerSample != 16 {
		return fmt.Errorf("%s is %dHz/%dch/%dbit, want %d/1/16", clipPath, clip.SampleRateHz, clip.Channels, clip.BitsPerSample, sampleRateHz)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := dial(ctx, wsURL)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	if err := writeFrame(ctx, c, wire.Frame{Type: wire.MsgControl, Seq: 0, Payload: []byte(sessionStart())}); err != nil {
		return fmt.Errorf("send session.start: %w", err)
	}

	// A dedicated reader goroutine on the connection's one long-lived
	// ctx, never a short per-poll one: coder/websocket closes the
	// connection when a Read's context is canceled (including by
	// timeout), so "poll with a short deadline to check without
	// blocking" is not a safe pattern on this library — it was silently
	// tearing the connection down between audio frames. This is the same
	// dedicated-reader-goroutine-plus-channel shape cmd/gateway's own
	// writeLoop uses server-side, which never hit this because it never
	// gives Read anything but the connection's full-lifetime context.
	events := make(chan map[string]any, 256)
	readErrs := make(chan error, 1)
	go func() {
		defer close(events)
		for {
			ev, err := readEvent(ctx, c)
			if err != nil {
				readErrs <- err
				return
			}
			events <- ev
		}
	}()

	ack, ok := <-events
	if !ok {
		return fmt.Errorf("read ack: %w", <-readErrs)
	}
	if ack["type"] != "ack" {
		return fmt.Errorf("got %v after session.start, want ack", ack)
	}

	const bytesPerSample = 2
	const nominalFrameMs = 20
	samplesPerFrame := sampleRateHz * nominalFrameMs / 1000
	bytesPerFrame := samplesPerFrame * bytesPerSample

	var ticker *time.Ticker
	if !fast {
		ticker = time.NewTicker(nominalFrameMs * time.Millisecond)
		defer ticker.Stop()
	}

	seq := uint64(1)
	sawPartial := false
	drain := func() error {
		for {
			select {
			case ev, ok := <-events:
				if !ok {
					return nil // reader goroutine exited; checked by the caller via readErrs where it matters
				}
				if ev["type"] == "error" {
					return fmt.Errorf("gateway reported error mid-stream: %v", ev)
				}
				if ev["type"] == "partial" {
					sawPartial = true
				}
			default:
				return nil // nothing buffered right now — a plain non-blocking channel receive, no connection risk
			}
		}
	}

	for off := 0; off < len(clip.PCM); off += bytesPerFrame {
		end := off + bytesPerFrame
		if end > len(clip.PCM) {
			end = len(clip.PCM)
		}
		payload := clip.PCM[off:end]
		numSamples := uint32(len(payload) / bytesPerSample)

		if ticker != nil {
			<-ticker.C
		}
		f := wire.Frame{Type: wire.MsgAudio, Seq: seq, NumSamples: numSamples, Payload: payload}
		if err := writeFrame(ctx, c, f); err != nil {
			return fmt.Errorf("send audio frame seq=%d: %w", seq, err)
		}
		seq++

		if err := drain(); err != nil {
			return err
		}
	}

	if err := writeFrame(ctx, c, wire.Frame{Type: wire.MsgControl, Seq: seq, Payload: []byte(`{"type":"session.end"}`)}); err != nil {
		return fmt.Errorf("send session.end: %w", err)
	}

	// From here, block on the channel (with the outer 60s ctx as the only
	// bound) until final arrives — no more non-blocking polling needed.
	for {
		ev, ok := <-events
		if !ok {
			return fmt.Errorf("connection closed before final arrived: %w", <-readErrs)
		}
		switch ev["type"] {
		case "error":
			return fmt.Errorf("gateway reported error: %v", ev)
		case "partial":
			sawPartial = true
		case "final":
			text, _ := ev["text"].(string)
			if text == "" {
				return fmt.Errorf("final carried empty text: %v", ev)
			}
			if !sawPartial {
				return fmt.Errorf("stream produced a final with no partial ever observed")
			}
			return nil
		}
	}
}

func testDroppedFrameDiscontinuity(wsURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := dial(ctx, wsURL)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	if err := writeFrame(ctx, c, wire.Frame{Type: wire.MsgControl, Seq: 0, Payload: []byte(sessionStart())}); err != nil {
		return err
	}
	if _, err := readEvent(ctx, c); err != nil { // ack
		return err
	}

	// seq 1 arrives normally, then jump straight to seq 5 — 2,3,4 lost.
	small := wire.Frame{Type: wire.MsgAudio, Seq: 1, NumSamples: 320, Payload: make([]byte, 640)}
	if err := writeFrame(ctx, c, small); err != nil {
		return err
	}
	gapFrame := wire.Frame{Type: wire.MsgAudio, Seq: 5, NumSamples: 320, Payload: make([]byte, 640)}
	if err := writeFrame(ctx, c, gapFrame); err != nil {
		return err
	}

	ev, err := readEvent(ctx, c)
	if err != nil {
		return fmt.Errorf("read discontinuity: %w", err)
	}
	if ev["type"] != "discontinuity" {
		return fmt.Errorf("got %v, want discontinuity", ev)
	}
	return nil
}

func testDurationMismatchRejected(wsURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := dial(ctx, wsURL)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	if err := writeFrame(ctx, c, wire.Frame{Type: wire.MsgControl, Seq: 0, Payload: []byte(sessionStart())}); err != nil {
		return err
	}
	if _, err := readEvent(ctx, c); err != nil { // ack
		return err
	}

	bad := wire.Frame{Type: wire.MsgAudio, Seq: 1, NumSamples: 320, Payload: []byte{1, 2, 3, 4}} // claims 640 bytes, sends 4
	if err := writeFrame(ctx, c, bad); err != nil {
		return err
	}
	ev, err := readEvent(ctx, c)
	if err != nil {
		return fmt.Errorf("read error event: %w", err)
	}
	if ev["type"] != "error" {
		return fmt.Errorf("got %v, want error", ev)
	}
	return nil
}
