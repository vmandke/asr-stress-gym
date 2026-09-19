package session

// SeqOutcome classifies an incoming frame's sequence number against what
// the session expects next. Mirrors the three-way switch in
// build-plan.md, "Sequence validation".
type SeqOutcome int

const (
	// SeqExpected: seq == expected. Advance and accept.
	SeqExpected SeqOutcome = iota
	// SeqDuplicate: seq < expected — the client replayed after a
	// reconnect. Do not re-ingest; do not emit discontinuity.
	SeqDuplicate
	// SeqGap: seq > expected — over TCP this means the client genuinely
	// skipped frames, not reordering. Emit discontinuity AND still accept
	// this frame (build-plan.md: "s.accept(f)" runs in the gap case too).
	SeqGap
)

// DiscontinuityRange is only meaningful when Validate returns SeqGap.
type DiscontinuityRange struct {
	SeqStart uint64
	SeqEnd   uint64
}

// SeqValidator is per-session, single-writer (the sessionLoop goroutine
// only — see build-plan.md "Goroutine structure"). Not safe for
// concurrent use, by design: one live session has exactly one active
// writer (invariant 3).
type SeqValidator struct {
	expected uint64
}

func NewSeqValidator() *SeqValidator { return &SeqValidator{} }

// Validate classifies seq and, for SeqExpected/SeqGap, advances what the
// validator expects next. The caller decides what to do with the
// classification: ingest into the audio pipeline for SeqExpected and
// SeqGap, but never re-ingest a SeqDuplicate (that would double-count
// audio already accounted for).
func (v *SeqValidator) Validate(seq uint64) (SeqOutcome, DiscontinuityRange) {
	switch {
	case seq == v.expected:
		v.expected++
		return SeqExpected, DiscontinuityRange{}
	case seq < v.expected:
		return SeqDuplicate, DiscontinuityRange{}
	default: // seq > v.expected
		gap := DiscontinuityRange{SeqStart: v.expected, SeqEnd: seq}
		v.expected = seq + 1
		return SeqGap, gap
	}
}
