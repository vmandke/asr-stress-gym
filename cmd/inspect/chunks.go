package main

import (
	"fmt"
	"strings"

	"asr-stress-gym/internal/audio"
	"asr-stress-gym/internal/corpus"
	"asr-stress-gym/internal/wire"
)

// These mirror cmd/gateway/conn.go. Duplicated deliberately rather than
// exported from there: this tool exists to SHOW what the gateway does, and
// a reader comparing the two files should see the same numbers written
// down twice, not an indirection to chase.
const (
	nominalFrameMs = 20
	sampleRateHz   = 16000
	onlineChunkMs  = 160
	bytesPerSample = 2
)

// cmdChunks runs a clip through the real internal/audio pipeline and
// prints every decision it makes. No gateway, no worker, no network —
// this is the audio boundary in isolation, which is the point: if chunking
// behaviour can only be observed by running the whole stack, the boundary
// is not doing its job.
func cmdChunks(path string) error {
	clip, err := corpus.LoadWAV(path)
	if err != nil {
		return err
	}
	if clip.SampleRateHz != sampleRateHz || clip.Channels != 1 || clip.BitsPerSample != 16 {
		return fmt.Errorf("%s: expected mono 16kHz s16le, got %dHz %dch %dbit",
			path, clip.SampleRateHz, clip.Channels, clip.BitsPerSample)
	}

	pipeline, err := audio.NewVADPipeline(sampleRateHz, onlineChunkMs, audio.DefaultVADConfig)
	if err != nil {
		return err
	}

	frameBytes := sampleRateHz * nominalFrameMs / 1000 * bytesPerSample
	totalMs := float64(len(clip.PCM)) / bytesPerSample / sampleRateHz * 1000

	fmt.Printf("clip      %s\n", path)
	fmt.Printf("duration  %.2fs  (%d frames of %dms, %d bytes each)\n",
		totalMs/1000, (len(clip.PCM)+frameBytes-1)/frameBytes, nominalFrameMs, frameBytes)
	fmt.Printf("policy    chunk at %dms accumulated DURATION (never frame count)\n", onlineChunkMs)
	fmt.Printf("vad       window=%dms start=%dms endSilence=%dms preRoll=%dms\n\n",
		audio.DefaultVADConfig.WindowMs, audio.DefaultVADConfig.StartSpeechMs,
		audio.DefaultVADConfig.EndSilenceMs, audio.DefaultVADConfig.PreRollMs)

	// The voicing timeline is the most useful single thing here: it shows
	// at a glance where speech is, where the VAD thinks it is, and where
	// chunks land relative to both.
	var timeline strings.Builder

	var (
		seq          uint64 = 1
		voicedFrames int
		chunks       []audio.Chunk
		events       []string
		chunkAtMs    []float64
	)

	for off := 0; off < len(clip.PCM); off += frameBytes {
		end := off + frameBytes
		if end > len(clip.PCM) {
			end = len(clip.PCM)
		}
		payload := clip.PCM[off:end]
		nowMs := float64(off) / bytesPerSample / sampleRateHz * 1000

		ref, err := pipeline.Ingest(wire.Frame{
			Type:       wire.MsgAudio,
			Seq:        seq,
			NumSamples: uint32(len(payload) / bytesPerSample),
			Payload:    payload,
		})
		if err != nil {
			return fmt.Errorf("ingest seq=%d: %w", seq, err)
		}
		if ref.Voiced {
			voicedFrames++
			timeline.WriteByte('#')
		} else {
			timeline.WriteByte('.')
		}

		if ev, ok := pipeline.Boundary(); ok {
			events = append(events, fmt.Sprintf("  %8.2fs  %-13s seq=%d", nowMs/1000, ev.Type, ev.Seq))
		}
		for _, c := range pipeline.Ready() {
			chunks = append(chunks, c)
			chunkAtMs = append(chunkAtMs, nowMs)
		}
		seq++
	}
	for _, c := range pipeline.Flush() {
		chunks = append(chunks, c)
		chunkAtMs = append(chunkAtMs, totalMs)
	}

	totalFrames := int(seq - 1)
	fmt.Println("voicing timeline  ('#' = VAD says speech, '.' = silence, one char per 20ms frame)")
	for i := 0; i < timeline.Len(); i += 100 {
		j := i + 100
		if j > timeline.Len() {
			j = timeline.Len()
		}
		fmt.Printf("  %6.1fs %s\n", float64(i)*nominalFrameMs/1000, timeline.String()[i:j])
	}

	if len(events) > 0 {
		fmt.Println("\nVAD boundary events")
		for _, e := range events {
			fmt.Println(e)
		}
	}

	fmt.Printf("\nchunks dispatched to the backend (%d)\n", len(chunks))
	fmt.Printf("  %-4s %-12s %-10s %-10s %s\n", "#", "cut at", "seq range", "bytes", "audio ms")
	shown := 0
	for i, c := range chunks {
		if len(chunks) > 14 && i == 7 {
			fmt.Printf("  ... %d more ...\n", len(chunks)-14)
		}
		if len(chunks) > 14 && i >= 7 && i < len(chunks)-7 {
			continue
		}
		ms := float64(len(c.Bytes)) / bytesPerSample / sampleRateHz * 1000
		fmt.Printf("  %-4d %-12s %-10s %-10d %.0f\n",
			c.ID, fmt.Sprintf("%.2fs", chunkAtMs[i]/1000),
			fmt.Sprintf("%d-%d", c.SeqStart, c.SeqEnd), len(c.Bytes), ms)
		shown++
	}

	var dispatchedMs float64
	for _, c := range chunks {
		dispatchedMs += float64(len(c.Bytes)) / bytesPerSample / sampleRateHz * 1000
	}
	ungated := totalMs / onlineChunkMs

	fmt.Printf("\nwhat this cost\n")
	fmt.Printf("  frames in                 %d  (%.2fs of audio)\n", totalFrames, totalMs/1000)
	fmt.Printf("  frames the VAD called speech  %d  (%.0f%%)\n",
		voicedFrames, 100*float64(voicedFrames)/float64(totalFrames))
	fmt.Printf("  backend calls made        %d\n", len(chunks))
	fmt.Printf("  backend calls if ungated  %.0f\n", ungated)
	if ungated > 0 {
		fmt.Printf("  saved by gating silence   %.0f%%\n", 100*(1-float64(len(chunks))/ungated))
	}
	fmt.Printf("  audio actually dispatched %.2fs of %.2fs\n", dispatchedMs/1000, totalMs/1000)
	fmt.Printf("\n  Every frame above is journaled regardless of voicing — gating decides\n")
	fmt.Printf("  what reaches a MODEL, never what is retained for replay.\n")
	return nil
}
