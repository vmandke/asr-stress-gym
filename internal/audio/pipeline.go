// Package audio is the ONLY code in this repository that touches PCM
// sample bytes. Everything above this package — session, journal, coord,
// router, backend — deals exclusively in Ref, Chunk, Record and Span, and
// never inspects payload content. See docs/implementation-plan.md, "The
// audio boundary".
//
// This file is the M1 pass-through Pipeline: duration-based chunk cutting
// is real (invariant 2 holds from the first commit), but there is no VAD
// yet, so Voiced is always true and Boundary never fires. M4 replaces the
// internals — not the exported types, not the interface — with a
// library-backed VAD, a reframer and a hysteresis state machine. A change
// there that requires touching a test outside this package is the signal
// the boundary leaked.
package audio

import "asr-stress-gym/internal/wire"

// Ref is what Ingest hands back for one wire frame that has been folded
// into the pipeline's accounting. It carries no payload — callers outside
// this package never see sample bytes, only that a frame of a given
// duration was accepted and whether it was judged speech.
type Ref struct {
	Seq        uint64
	DurationMs float64
	Voiced     bool
}

// Chunk is what a backend is called with: an opaque payload plus the
// sequence span it was cut from. Nothing above this package inspects
// Bytes.
type Chunk struct {
	ID       uint64
	SeqStart uint64
	SeqEnd   uint64
	Bytes    []byte
}

// Record is the shape the (M2) journal stores and later replays through
// Recut. Defined here, not in internal/journal, so the journal depends on
// audio's opaque shape rather than the other way around.
type Record struct {
	Seq        uint64
	DurationMs float64
	Voiced     bool
	Payload    []byte
}

// Span identifies one originally-dispatched chunk by the seq range it
// covered — the (M2/M3) dispatch log replays these, not raw journal
// order, so a streaming encoder's state is reconstructed from the same
// call boundaries the original worker saw. See implementation-plan.md
// defects #2/#3.
type Span struct {
	ChunkID  uint64
	SeqStart uint64
	SeqEnd   uint64
}

type EventType string

const (
	EventSpeechStart EventType = "speech.start"
	EventEndpoint    EventType = "endpoint"
)

// Event is a VAD-driven boundary (M4). The pass-through pipeline never
// produces one.
type Event struct {
	Type EventType
	Seq  uint64
}

// Pipeline is the entire surface the rest of the system depends on.
// Everything sample-level a concrete implementation needs — VAD,
// reframing window, hysteresis, pre-roll — is internal to it.
type Pipeline interface {
	// Ingest folds one decoded, already-validated audio frame into the
	// pipeline's accounting and returns a Ref describing it.
	Ingest(f wire.Frame) (Ref, error)

	// Ready drains and returns any chunks cut since the last call.
	Ready() []Chunk

	// Boundary reports a VAD-driven speech/endpoint event, if one fired
	// since the last call. The pass-through implementation never returns
	// true.
	Boundary() (Event, bool)

	// RetentionStart reports the oldest sequence that must remain in the
	// journal for a possible next utterance's pre-roll. A pipeline with no
	// retained pre-roll returns ok=false. This keeps journal trimming safe
	// without exposing sample data above internal/audio.
	RetentionStart() (seq uint64, ok bool)

	// Recut reproduces, from a stored sequence of Records, the same
	// chunk boundaries live Ingest would have cut. Used by replay (M2/M3)
	// so recovery calls a backend with byte-identical chunk cuts to what
	// the original worker saw, not just the same underlying samples.
	Recut(records []Record) ([]Chunk, error)

	// Flush forces out any pending sub-threshold audio as one final
	// chunk (0 or 1), regardless of accumulated duration. Called when an
	// utterance or session ends: without it, a trailing tail shorter
	// than one chunk would never cross Ready()'s threshold and would be
	// silently dropped from the transcript rather than dispatched.
	Flush() []Chunk
}

// accumulator holds one in-progress chunk's worth of state. Both live
// Ingest and Recut push records through feed, so the two can never
// silently diverge in how they cut a chunk boundary — that agreement is
// the boundary-integrity property implementation-plan.md commits to.
type accumulator struct {
	chunkMs      float64
	nextChunkID  uint64
	pending      []byte
	pendingMs    float64
	pendingStart uint64
	pendingEnd   uint64
	hasPending   bool
}

// feed appends one record's audio (if voiced) to the in-progress chunk
// and cuts a chunk when accumulated duration reaches chunkMs.
//
// Known, deliberate simplification: a single record whose own duration
// already exceeds chunkMs (an unusually large batched frame) is cut as
// one oversized chunk rather than split mid-record — invariant 2 (never
// call the model per transport frame) still holds either way, and
// splitting mid-record would need byte-rate arithmetic this package has
// no reason to carry yet. Nothing above this package can observe the
// difference: it only ever sees "a chunk was ready", never how large one
// was expected to be.
func (a *accumulator) feed(seq uint64, durationMs float64, voiced bool, payload []byte) []Chunk {
	if !voiced {
		return nil // silence never enters the chunk content, only the journal (M2)
	}
	if !a.hasPending {
		a.pendingStart = seq
		a.hasPending = true
	}
	a.pending = append(a.pending, payload...)
	a.pendingMs += durationMs
	a.pendingEnd = seq

	if a.pendingMs < a.chunkMs {
		return nil
	}
	c := Chunk{ID: a.nextChunkID, SeqStart: a.pendingStart, SeqEnd: a.pendingEnd, Bytes: a.pending}
	a.nextChunkID++
	a.pending = nil
	a.pendingMs = 0
	a.hasPending = false
	return []Chunk{c}
}

// forceCut cuts whatever is pending regardless of chunkMs — the shared
// implementation behind Pipeline.Flush.
func (a *accumulator) forceCut() []Chunk {
	if !a.hasPending {
		return nil
	}
	c := Chunk{ID: a.nextChunkID, SeqStart: a.pendingStart, SeqEnd: a.pendingEnd, Bytes: a.pending}
	a.nextChunkID++
	a.pending = nil
	a.pendingMs = 0
	a.hasPending = false
	return []Chunk{c}
}

type passthroughPipeline struct {
	sampleRateHz uint32
	live         accumulator
	ready        []Chunk
}

// NewPassthroughPipeline builds the M1 Pipeline: real duration-based
// chunk cutting, no VAD (every frame is treated as voiced), no boundary
// detection. chunkMs matches build-plan.md's online/offline constants
// (160ms / 2000ms) via the caller's choice.
func NewPassthroughPipeline(sampleRateHz uint32, chunkMs float64) Pipeline {
	return &passthroughPipeline{
		sampleRateHz: sampleRateHz,
		live:         accumulator{chunkMs: chunkMs},
	}
}

func (p *passthroughPipeline) Ingest(f wire.Frame) (Ref, error) {
	dur := f.DurationMs(p.sampleRateHz)
	ref := Ref{Seq: f.Seq, DurationMs: dur, Voiced: true}
	cut := p.live.feed(f.Seq, dur, true, f.Payload)
	p.ready = append(p.ready, cut...)
	return ref, nil
}

func (p *passthroughPipeline) Ready() []Chunk {
	out := p.ready
	p.ready = nil
	return out
}

func (p *passthroughPipeline) Boundary() (Event, bool) {
	return Event{}, false
}

func (p *passthroughPipeline) RetentionStart() (uint64, bool) { return 0, false }

func (p *passthroughPipeline) Flush() []Chunk {
	return p.live.forceCut()
}

func (p *passthroughPipeline) Recut(records []Record) ([]Chunk, error) {
	acc := accumulator{chunkMs: p.live.chunkMs}
	var out []Chunk
	for _, r := range records {
		out = append(out, acc.feed(r.Seq, r.DurationMs, r.Voiced, r.Payload)...)
	}
	return out, nil
}
