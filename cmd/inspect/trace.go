package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/coder/websocket"

	"asr-stress-gym/internal/corpus"
	"asr-stress-gym/internal/wire"
)

// cmdTrace runs ONE real session against a running gateway and prints
// every event with the time it arrived and what it cost.
//
// The point is attribution, not a log. A `partial` carries no sequence
// number, so "when did it arrive" is easy and "what was it FOR" is not —
// and without the second the timing means nothing. The protocol pins it
// down: the gateway emits `partial` then `ack` for the same chunk, in that
// order, over one ordered socket. So the next ack's seq identifies the
// frame that completed the chunk, and its send time is the clock start.
// Everything below hangs off that pairing.
func cmdTrace(clipPath string) error {
	wsURL := envOr("GATEWAY_WS_URL", "ws://localhost:7070/ws")
	debugURL := envOr("GATEWAY_DEBUG_URL", "http://localhost:7000")

	if clipPath == "" {
		c, err := corpus.AnyClip()
		if err != nil {
			return err
		}
		clipPath = c
	}
	clip, err := corpus.LoadWAV(clipPath)
	if err != nil {
		return err
	}

	before, _ := fetchMetrics(debugURL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w (is the stack up? `make up`)", wsURL, err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// One reader goroutine on the connection's full-lifetime context.
	// Canceling a coder/websocket Read closes the WHOLE connection, not
	// just that call — a per-poll timeout here would look like the gateway
	// hanging up mid-stream.
	incoming := make(chan map[string]any, 512)
	go func() {
		defer close(incoming)
		for {
			typ, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			if typ != websocket.MessageText {
				continue
			}
			var ev map[string]any
			if json.Unmarshal(data, &ev) == nil {
				incoming <- ev
			}
		}
	}()

	t0 := time.Now()
	rel := func() float64 { return float64(time.Since(t0).Microseconds()) / 1000 }

	fmt.Printf("clip     %s (%.2fs)\n", clipPath, float64(len(clip.PCM))/2/16000)
	fmt.Printf("gateway  %s\n\n", wsURL)
	fmt.Printf("%-11s %-15s %s\n", "t (ms)", "event", "detail")
	fmt.Printf("%-11s %-15s %s\n", "-----------", "---------------", "------")

	// --- session.start ---
	start := []byte(`{"type":"session.start","mode":"online","sample_rate_hz":16000,"encoding":"pcm_s16le","channels":1,"nominal_frame_ms":20}`)
	dialedAt := time.Now()
	if err := writeFrame(ctx, conn, wire.Frame{Type: wire.MsgControl, Seq: 0, Payload: start}); err != nil {
		return err
	}
	fmt.Printf("%-11.1f %-15s mode=online 16kHz s16le mono\n", rel(), "→ session.start")

	sessionID := ""
	for ev := range incoming {
		t, _ := ev["type"].(string)
		if t == "overloaded" {
			return fmt.Errorf("gateway refused the session: %v", ev)
		}
		if t == "ack" {
			sessionID, _ = ev["session_id"].(string)
			fmt.Printf("%-11.1f %-15s session_id=%s  (admitted + worker pinned + Open'd in this time)\n",
				rel(), "← ack", sessionID)
			break
		}
	}
	openMs := float64(time.Since(dialedAt).Microseconds()) / 1000

	// --- paced audio ---
	const frameMs, frameBytes = 20, 640
	sentAt := map[uint64]time.Time{}
	var (
		seq          uint64 = 1
		pendingSeq   uint64
		havePending  bool
		chunkLatency []float64
		partials     int
		acks         int
		finals       int
		firstPartial = -1.0
	)

	ticker := time.NewTicker(frameMs * time.Millisecond)
	defer ticker.Stop()

	drain := func() {
		for {
			select {
			case ev, ok := <-incoming:
				if !ok {
					return
				}
				switch t, _ := ev["type"].(string); t {
				case "speech.start":
					fmt.Printf("%-11.1f %-15s utterance=%v\n", rel(), "← speech.start", ev["utterance_id"])
				case "partial":
					partials++
					if firstPartial < 0 {
						firstPartial = rel()
					}
					havePending = true
					pendingSeq = 0
					txt, _ := ev["text"].(string)
					if len(txt) > 46 {
						txt = txt[len(txt)-46:]
					}
					fmt.Printf("%-11.1f %-15s rev=%v  ...%s\n", rel(), "← partial", ev["revision"], txt)
				case "ack":
					acks++
					s := uint64(toF(ev["highest_contiguous_seq"]))
					if sent, ok := sentAt[s]; ok {
						lat := float64(time.Since(sent).Microseconds()) / 1000
						if havePending {
							chunkLatency = append(chunkLatency, lat)
							havePending = false
						}
						fmt.Printf("%-11.1f %-15s seq=%d  round trip %.1fms (frame written → chunk inferred → ack)\n",
							rel(), "← ack", s, lat)
						for k := range sentAt {
							if k <= s {
								delete(sentAt, k)
							}
						}
					}
					_ = pendingSeq
				case "final":
					finals++
					txt, _ := ev["text"].(string)
					if len(txt) > 42 {
						txt = txt[:42] + "..."
					}
					fmt.Printf("%-11.1f %-15s utterance=%v seq=%v-%v  %q\n",
						rel(), "← FINAL", ev["utterance_id"], ev["seq_start"], ev["seq_end"], txt)
				case "partial.reset":
					fmt.Printf("%-11.1f %-15s failover_epoch=%v — discard displayed text\n",
						rel(), "← partial.reset", ev["failover_epoch"])
				case "discontinuity":
					fmt.Printf("%-11.1f %-15s seq %v-%v lost\n", rel(), "← discontinuity", ev["seq_start"], ev["seq_end"])
				case "error":
					fmt.Printf("%-11.1f %-15s %v\n", rel(), "← error", ev["reason"])
				}
			default:
				return
			}
		}
	}

	for off := 0; off < len(clip.PCM); off += frameBytes {
		end := off + frameBytes
		if end > len(clip.PCM) {
			end = len(clip.PCM)
		}
		<-ticker.C
		drain()
		now := time.Now()
		if err := writeFrame(ctx, conn, wire.Frame{
			Type: wire.MsgAudio, Seq: seq,
			NumSamples: uint32((end - off) / 2), Payload: clip.PCM[off:end],
		}); err != nil {
			return fmt.Errorf("frame seq=%d: %w", seq, err)
		}
		sentAt[seq] = now
		seq++
	}

	// --- session.end ---
	endAt := time.Now()
	if err := writeFrame(ctx, conn, wire.Frame{Type: wire.MsgControl, Seq: seq, Payload: []byte(`{"type":"session.end"}`)}); err != nil {
		return err
	}
	fmt.Printf("%-11.1f %-15s flush tail, finalize open utterance\n", rel(), "→ session.end")

	deadline := time.After(30 * time.Second)
	for {
		select {
		case ev, ok := <-incoming:
			if !ok {
				goto done
			}
			if t, _ := ev["type"].(string); t == "final" {
				finals++
				txt, _ := ev["text"].(string)
				if len(txt) > 42 {
					txt = txt[:42] + "..."
				}
				fmt.Printf("%-11.1f %-15s utterance=%v  %q   (%.1fms after session.end)\n",
					rel(), "← FINAL", ev["utterance_id"], txt,
					float64(time.Since(endAt).Microseconds())/1000)
				goto done
			}
		case <-deadline:
			goto done
		}
	}
done:

	after, _ := fetchMetrics(debugURL)

	sort.Float64s(chunkLatency)
	p := func(q float64) float64 {
		if len(chunkLatency) == 0 {
			return 0
		}
		i := int(float64(len(chunkLatency)) * q)
		if i >= len(chunkLatency) {
			i = len(chunkLatency) - 1
		}
		return chunkLatency[i]
	}

	audioS := float64(len(clip.PCM)) / 2 / 16000
	fmt.Printf("\nwhere the time went\n")
	fmt.Printf("  session open (admit + pick + Open)   %7.1f ms\n", openMs)
	if firstPartial >= 0 {
		fmt.Printf("  first partial (time to first text)   %7.1f ms\n", firstPartial)
	}
	fmt.Printf("  chunk round trip  p50 / p95          %7.1f / %.1f ms   (n=%d)\n", p(0.50), p(0.95), len(chunkLatency))
	fmt.Printf("\nwhat the gateway did\n")
	fmt.Printf("  frames received                      %7d   (%.2fs of audio)\n", seq-1, audioS)
	fmt.Printf("  partials emitted                     %7d\n", partials)
	fmt.Printf("  acks emitted                         %7d\n", acks)
	fmt.Printf("  finals emitted                       %7d\n", finals)
	fmt.Printf("  backend pushes (gateway → worker)    %7d   ← from the gateway's own counters\n",
		after.BackendPushesTotal-before.BackendPushesTotal)
	fmt.Printf("  ungated, this clip would have been   %7.0f\n", audioS*1000/160)
	fmt.Printf("  failovers during this session        %7d\n", after.FailoverTotal-before.FailoverTotal)
	fmt.Printf("  duplicate finals (must be zero)      %7d\n", after.DuplicateFinalsTotal-before.DuplicateFinalsTotal)
	return nil
}

func writeFrame(ctx context.Context, c *websocket.Conn, f wire.Frame) error {
	return c.Write(ctx, websocket.MessageBinary, wire.Encode(f))
}

func toF(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}

type metricsSnap struct {
	DuplicateFinalsTotal int64 `json:"duplicate_finals_total"`
	FailoverTotal        int64 `json:"failover_total"`
	BackendPushesTotal   int64 `json:"backend_pushes_total"`
}

func fetchMetrics(debugURL string) (metricsSnap, error) {
	var m metricsSnap
	resp, err := http.Get(debugURL + "/api/debug/metrics")
	if err != nil {
		return m, err
	}
	defer resp.Body.Close()
	return m, json.NewDecoder(resp.Body).Decode(&m)
}
