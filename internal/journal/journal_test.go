package journal

import (
	"testing"

	"asr-stress-gym/internal/audio"
	"asr-stress-gym/internal/wire"
)

func rec(seq uint64) audio.Record {
	return audio.Record{Seq: seq, DurationMs: 20, Voiced: true, Payload: []byte{byte(seq)}}
}

func seqs(records []audio.Record) []uint64 {
	out := make([]uint64, len(records))
	for i, r := range records {
		out[i] = r.Seq
	}
	return out
}

func eqSeqs(t *testing.T, got []audio.Record, want []uint64) {
	t.Helper()
	gotSeqs := seqs(got)
	if len(gotSeqs) != len(want) {
		t.Fatalf("got seqs %v, want %v", gotSeqs, want)
	}
	for i := range want {
		if gotSeqs[i] != want[i] {
			t.Fatalf("got seqs %v, want %v", gotSeqs, want)
		}
	}
}

func TestNewPanicsOnNonPositiveCapacity(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for capacity 0")
		}
	}()
	New(0)
}

func TestAppendAndReadAfter(t *testing.T) {
	j := New(10)
	for seq := uint64(0); seq < 5; seq++ {
		j.Append(rec(seq))
	}
	eqSeqs(t, j.ReadAfter(1), []uint64{2, 3, 4}) // exclusive of 1, per Recut's replay-from-checkpoint semantics
	eqSeqs(t, j.ReadAfter(4), []uint64{})
	eqSeqs(t, j.ReadAfter(0), []uint64{1, 2, 3, 4})
}

func TestReadAfterOnEmptyJournal(t *testing.T) {
	j := New(10)
	eqSeqs(t, j.ReadAfter(0), []uint64{})
}

func TestReadFromCommittedBeforeAnyCommit(t *testing.T) {
	j := New(10)
	for seq := uint64(0); seq < 3; seq++ {
		j.Append(rec(seq))
	}
	// Nothing has ever committed — everything retained counts as "since
	// committed" (there is no earlier boundary to exclude).
	eqSeqs(t, j.ReadFromCommitted(), []uint64{0, 1, 2})
}

func TestTrimBeforeAdvancesCommittedFloor(t *testing.T) {
	j := New(10)
	for seq := uint64(0); seq < 6; seq++ {
		j.Append(rec(seq))
	}
	j.TrimBefore(2) // a final committed with seq_end=2
	// Exclusive: seq 2 itself is behind the committed boundary, never
	// needed again (it's covered by the immutable final).
	eqSeqs(t, j.ReadFromCommitted(), []uint64{3, 4, 5})

	j.Append(rec(6))
	eqSeqs(t, j.ReadFromCommitted(), []uint64{3, 4, 5, 6})

	j.TrimBefore(5) // a second final commits later
	eqSeqs(t, j.ReadFromCommitted(), []uint64{6})
}

func TestTrimBeforeBeyondRetainedIsNotAnError(t *testing.T) {
	j := New(10)
	j.Append(rec(0))
	j.Append(rec(1))
	j.TrimBefore(100) // nothing that far ahead exists yet — must not panic
	eqSeqs(t, j.ReadFromCommitted(), []uint64{})
	j.Append(rec(101))
	eqSeqs(t, j.ReadFromCommitted(), []uint64{101})
}

// The Done-when bar's explicit "wraparound" requirement: a fixed-capacity
// ring must silently evict the oldest entry once full, and logical
// ordering (oldest-to-newest) must stay correct across the wrap.
func TestWraparoundEvictsOldestAndPreservesOrder(t *testing.T) {
	j := New(4)
	for seq := uint64(0); seq < 4; seq++ {
		j.Append(rec(seq))
	}
	if j.Len() != 4 || j.Cap() != 4 {
		t.Fatalf("Len=%d Cap=%d, want 4/4", j.Len(), j.Cap())
	}
	eqSeqs(t, j.ReadAfter(0), []uint64{1, 2, 3})

	// Appending a 5th record must evict seq 0 (oldest), not grow past capacity.
	j.Append(rec(4))
	if j.Len() != 4 {
		t.Fatalf("Len=%d after wraparound, want 4 (capacity never exceeded)", j.Len())
	}
	eqSeqs(t, j.ReadFromCommitted(), []uint64{1, 2, 3, 4}) // seq 0 is gone

	// Wrap around multiple full cycles to make sure the write index math
	// (mod capacity) stays correct, not just on the first wrap.
	for seq := uint64(5); seq < 21; seq++ {
		j.Append(rec(seq))
	}
	eqSeqs(t, j.ReadFromCommitted(), []uint64{17, 18, 19, 20})
}

// A TrimBefore floor set before wraparound removed the entries anyway
// must not resurrect or misbehave once wraparound has physically evicted
// them — ReadFromCommitted just has nothing earlier to return.
func TestWraparoundPastACommittedFloorIsHarmless(t *testing.T) {
	j := New(3)
	j.Append(rec(0))
	j.Append(rec(1))
	j.TrimBefore(0)
	// Now push past capacity so seq 1 itself gets physically evicted too.
	j.Append(rec(2))
	j.Append(rec(3))
	j.Append(rec(4))
	eqSeqs(t, j.ReadFromCommitted(), []uint64{2, 3, 4})
}

// Ties M1 and M2 together: records read back out of the journal, fed
// through the SAME audio.Pipeline.Recut that M1 already proved reproduces
// live dispatch, must still cut identical chunk boundaries. This is the
// concrete demonstration that Journal + Recut together deliver what a
// separate dispatch-log structure would have (see the package doc).
func TestJournalRecordsFeedRecutCorrectly(t *testing.T) {
	const sampleRateHz = 16000
	const chunkMs = 160

	live := audio.NewPassthroughPipeline(sampleRateHz, chunkMs)
	j := New(100)

	var liveChunks []audio.Chunk
	for seq := uint64(0); seq < 12; seq++ {
		numSamples := uint32(20 * sampleRateHz / 1000)
		payload := make([]byte, int(numSamples)*wire.BytesPerSample)
		f := wire.Frame{Type: wire.MsgAudio, Seq: seq, NumSamples: numSamples, Payload: payload}

		ref, err := live.Ingest(f)
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		j.Append(audio.Record{Seq: seq, DurationMs: ref.DurationMs, Voiced: ref.Voiced, Payload: payload})
		liveChunks = append(liveChunks, live.Ready()...)
	}

	replayed, err := live.Recut(j.ReadFromCommitted())
	if err != nil {
		t.Fatalf("Recut: %v", err)
	}
	if len(replayed) != len(liveChunks) {
		t.Fatalf("Recut via journal produced %d chunks, live produced %d", len(replayed), len(liveChunks))
	}
	for i := range liveChunks {
		if replayed[i].SeqStart != liveChunks[i].SeqStart || replayed[i].SeqEnd != liveChunks[i].SeqEnd {
			t.Fatalf("chunk %d span mismatch: live=[%d,%d] replayed=[%d,%d]",
				i, liveChunks[i].SeqStart, liveChunks[i].SeqEnd, replayed[i].SeqStart, replayed[i].SeqEnd)
		}
	}
}
