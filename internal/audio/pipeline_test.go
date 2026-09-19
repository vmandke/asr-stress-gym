package audio

import (
	"bytes"
	"testing"

	"asr-stress-gym/internal/wire"
)

const sampleRateHz = 16000

func pcmOfDuration(ms float64) []byte {
	numSamples := int(ms * sampleRateHz / 1000)
	return make([]byte, numSamples*wire.BytesPerSample)
}

func audioFrame(seq uint64, ms float64) wire.Frame {
	numSamples := uint32(ms * sampleRateHz / 1000)
	return wire.Frame{
		Type:       wire.MsgAudio,
		Seq:        seq,
		NumSamples: numSamples,
		Payload:    pcmOfDuration(ms),
	}
}

// Invariant 2: the model is never called per transport frame. At a 20ms
// arrival cadence and a 160ms chunk, ~8 frames should produce exactly one
// chunk, not eight backend calls.
func TestNeverCallsPerFrame(t *testing.T) {
	p := NewPassthroughPipeline(sampleRateHz, 160)
	var allReady []Chunk
	for seq := uint64(0); seq < 8; seq++ {
		if _, err := p.Ingest(audioFrame(seq, 20)); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		allReady = append(allReady, p.Ready()...)
	}
	// 8 * 20ms = 160ms, so exactly one chunk should have been cut, on the
	// 8th frame (accumulated 160 >= chunkMs 160).
	if len(allReady) != 1 {
		t.Fatalf("got %d chunks from 8x20ms frames at a 160ms chunk, want 1", len(allReady))
	}
	if allReady[0].SeqStart != 0 || allReady[0].SeqEnd != 7 {
		t.Fatalf("chunk span = [%d,%d], want [0,7]", allReady[0].SeqStart, allReady[0].SeqEnd)
	}
}

// The boundary contract test implementation-plan.md commits to: a client
// that batches N nominal frames into one message produces the same chunk
// count as a client sending them as N separate messages. Accumulation is
// by declared duration, never by how many Ingest calls it took to arrive.
func TestChunkCountIndependentOfFraming(t *testing.T) {
	const chunkMs = 160
	const totalMs = 480 // well under any single-frame-exceeds-chunk edge case

	singles := NewPassthroughPipeline(sampleRateHz, chunkMs)
	var singlesChunks []Chunk
	seq := uint64(0)
	for accumulated := 0.0; accumulated < totalMs; accumulated += 20 {
		if _, err := singles.Ingest(audioFrame(seq, 20)); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		singlesChunks = append(singlesChunks, singles.Ready()...)
		seq++
	}

	batched := NewPassthroughPipeline(sampleRateHz, chunkMs)
	var batchedChunks []Chunk
	// Same total duration, delivered as 3 frames of 160ms each instead of
	// 24 frames of 20ms each.
	for i, seq := 0, uint64(0); i < 3; i, seq = i+1, seq+1 {
		if _, err := batched.Ingest(audioFrame(seq, 160)); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		batchedChunks = append(batchedChunks, batched.Ready()...)
	}

	if len(singlesChunks) != len(batchedChunks) {
		t.Fatalf("chunk count depends on framing: %d (singles) vs %d (batched), want equal",
			len(singlesChunks), len(batchedChunks))
	}
	if len(singlesChunks) != 3 { // 480ms / 160ms
		t.Fatalf("got %d chunks, want 3", len(singlesChunks))
	}
	var singlesTotal, batchedTotal int
	for _, c := range singlesChunks {
		singlesTotal += len(c.Bytes)
	}
	for _, c := range batchedChunks {
		batchedTotal += len(c.Bytes)
	}
	if singlesTotal != batchedTotal {
		t.Fatalf("total dispatched bytes differ: %d (singles) vs %d (batched)", singlesTotal, batchedTotal)
	}
}

func TestReadyDrainsBetweenCalls(t *testing.T) {
	p := NewPassthroughPipeline(sampleRateHz, 40)
	if _, err := p.Ingest(audioFrame(0, 40)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	first := p.Ready()
	if len(first) != 1 {
		t.Fatalf("got %d chunks, want 1", len(first))
	}
	second := p.Ready()
	if len(second) != 0 {
		t.Fatalf("Ready() after drain returned %d chunks, want 0", len(second))
	}
}

func TestPassthroughNeverFiresBoundary(t *testing.T) {
	p := NewPassthroughPipeline(sampleRateHz, 160)
	if _, err := p.Ingest(audioFrame(0, 200)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if _, ok := p.Boundary(); ok {
		t.Fatal("pass-through pipeline fired a boundary event; VAD isn't wired until M4")
	}
}

// Boundary integrity: Recut, replaying stored Records, must reproduce the
// exact chunk cuts live Ingest produced for the same audio. This is what
// lets recovery call a backend with the same chunk boundaries the dead
// worker originally saw (implementation-plan.md defects #2/#3).
func TestRecutReproducesLiveDispatch(t *testing.T) {
	const chunkMs = 160
	live := NewPassthroughPipeline(sampleRateHz, chunkMs)

	var records []Record
	var liveChunks []Chunk
	for seq := uint64(0); seq < 12; seq++ {
		f := audioFrame(seq, 20)
		ref, err := live.Ingest(f)
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		records = append(records, Record{Seq: seq, DurationMs: ref.DurationMs, Voiced: ref.Voiced, Payload: f.Payload})
		liveChunks = append(liveChunks, live.Ready()...)
	}

	replayed, err := live.Recut(records)
	if err != nil {
		t.Fatalf("Recut: %v", err)
	}

	if len(replayed) != len(liveChunks) {
		t.Fatalf("Recut produced %d chunks, live produced %d", len(replayed), len(liveChunks))
	}
	for i := range liveChunks {
		if replayed[i].SeqStart != liveChunks[i].SeqStart || replayed[i].SeqEnd != liveChunks[i].SeqEnd {
			t.Fatalf("chunk %d span mismatch: live=[%d,%d] replayed=[%d,%d]",
				i, liveChunks[i].SeqStart, liveChunks[i].SeqEnd, replayed[i].SeqStart, replayed[i].SeqEnd)
		}
		if !bytes.Equal(replayed[i].Bytes, liveChunks[i].Bytes) {
			t.Fatalf("chunk %d bytes mismatch between live dispatch and Recut", i)
		}
	}
}

// Without Flush, a trailing tail shorter than one chunk would sit in
// pending forever and never reach a backend — silently dropped from the
// transcript exactly when an utterance or session ends.
func TestFlushForcesOutTrailingSubThresholdAudio(t *testing.T) {
	p := NewPassthroughPipeline(sampleRateHz, 160)
	if _, err := p.Ingest(audioFrame(0, 60)); err != nil { // well under the 160ms threshold
		t.Fatalf("Ingest: %v", err)
	}
	if ready := p.Ready(); len(ready) != 0 {
		t.Fatalf("got %d chunks before threshold, want 0", len(ready))
	}
	flushed := p.Flush()
	if len(flushed) != 1 {
		t.Fatalf("got %d chunks from Flush, want 1", len(flushed))
	}
	if flushed[0].SeqStart != 0 || flushed[0].SeqEnd != 0 {
		t.Fatalf("flushed chunk span = [%d,%d], want [0,0]", flushed[0].SeqStart, flushed[0].SeqEnd)
	}
}

func TestFlushOnEmptyPipelineIsANoop(t *testing.T) {
	p := NewPassthroughPipeline(sampleRateHz, 160)
	if flushed := p.Flush(); len(flushed) != 0 {
		t.Fatalf("got %d chunks from Flush on an empty pipeline, want 0", len(flushed))
	}
}

func TestSilenceNeverEntersChunkContent(t *testing.T) {
	acc := accumulator{chunkMs: 100}
	// A silent record contributes duration to nothing and no bytes ever
	// appear in a cut chunk — silence enters the (M2) journal, never the
	// dispatched payload. See build-plan.md's chunker: "Silence enters
	// the journal but never pending."
	if cut := acc.feed(0, 200, false, pcmOfDuration(200)); cut != nil {
		t.Fatalf("silent record produced a chunk: %+v", cut)
	}
	if acc.hasPending {
		t.Fatal("silent record left state in the accumulator")
	}
}
