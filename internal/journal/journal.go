// Package journal is the bounded, model-independent audio recovery store
// (docs/build-plan.md, "The audio journal" — tier 3 of the three tiers:
// "the system must be correct with tier 3 alone"). It retains every
// accepted audio.Record — voiced or not, since a full rebuild must work
// regardless of what VAD (M4) gates out of dispatch — in a fixed-capacity
// ring.
//
// There is deliberately no separate "dispatch log" of audio.Span here,
// even though implementation-plan.md's original phrasing suggested one.
// M1 already proved (audio.Pipeline.Recut, TestRecutReproducesLiveDispatch)
// that replaying the ORIGINAL RECORDS through Recut deterministically
// reproduces the original chunk boundaries, provided replay only ever
// starts from a point where the live accumulator was empty. Both of this
// system's actual replay entry points satisfy that: a checkpoint is only
// ever taken right after a chunk was fully applied (the accumulator had
// just emptied), and a committed final is only ever reached right after
// Pipeline.Flush() forces the accumulator empty. A separately persisted
// Span log would be redundant with what Journal + Recut already achieve
// together — see docs/STATUS.md's M2 entry for the fuller reasoning.
package journal

import "asr-stress-gym/internal/audio"

// Journal is a fixed-capacity ring of audio.Record, preallocated
// (build-plan.md: "In-memory bounded ring, preallocated"). Capacity
// bounds memory structurally — wraparound silently overwrites the oldest
// entry once full, which is the backstop for a session that never
// commits. TrimBefore (called on every committed final in the healthy
// case) is what keeps REPLAY COST bounded, not memory: memory is already
// bounded by capacity regardless of whether TrimBefore is ever called.
//
// Deliberately a concrete type, not build-plan.md's literal Journal
// interface: there is exactly one implementation this project will ever
// have (build-plan.md's own non-goal: "Do not reach for Redis or object
// storage"), so an interface with one implementation and no error paths
// worth modeling would be needless indirection. Append reflects this too
// — it cannot fail against an in-memory preallocated ring, so unlike
// build-plan.md's sketch it does not return a fake always-nil error.
type Journal struct {
	records       []audio.Record
	writeIdx      int
	size          int
	committedSeq  uint64
	haveCommitted bool
}

// New builds a Journal with a fixed capacity. Size for the session's
// expected retention at nominal framing — e.g. 1500 entries covers ~30s
// at the nominal 20ms cadence (build-plan.md's own retention target); a
// client that batches larger frames covers proportionally more audio in
// the same entry count.
func New(capacity int) *Journal {
	if capacity <= 0 {
		panic("journal: capacity must be positive")
	}
	return &Journal{records: make([]audio.Record, capacity)}
}

// Append adds one record, evicting the oldest entry (wraparound) once
// capacity is reached. Every accepted frame is appended here, including
// silence — the journal's completeness is what makes a full rebuild from
// audio always possible (build-plan.md's central principle: "Audio is
// the recovery source of truth").
func (j *Journal) Append(r audio.Record) {
	capN := len(j.records)
	j.records[j.writeIdx] = r
	j.writeIdx = (j.writeIdx + 1) % capN
	if j.size < capN {
		j.size++
	}
}

// entries returns everything currently retained, oldest first, unwinding
// the ring's physical layout into logical order.
func (j *Journal) entries() []audio.Record {
	capN := len(j.records)
	out := make([]audio.Record, 0, j.size)
	start := (j.writeIdx - j.size + capN) % capN
	for i := 0; i < j.size; i++ {
		out = append(out, j.records[(start+i)%capN])
	}
	return out
}

// ReadAfter returns retained records with Seq > seq, in order — the tail
// replay path after a same-model checkpoint restore (build-plan.md Mode
// 1: "replay journal tail: seq 1181..1241" after a checkpoint at 1180).
func (j *Journal) ReadAfter(seq uint64) []audio.Record {
	var out []audio.Record
	for _, r := range j.entries() {
		if r.Seq > seq {
			out = append(out, r)
		}
	}
	return out
}

// ReadFromCommitted returns everything retained since the last committed
// final (or everything retained, if nothing has committed yet) — the
// full-utterance replay path for cross-model failover (build-plan.md Mode
// 2: "replay from the last committed FINAL boundary").
func (j *Journal) ReadFromCommitted() []audio.Record {
	if !j.haveCommitted {
		return j.entries()
	}
	return j.ReadAfter(j.committedSeq)
}

// TrimBefore marks seq as the new committed floor: ReadFromCommitted will
// only return records after it from now on. Called on every committed
// final (build-plan.md: "Trim on every committed final").
//
// At M1/M2, with exactly one utterance per session, the caller passes
// that utterance's final seq_end directly. Once M4 allows multiple
// utterances per session, the caller must instead pass
// min(committedSeq, openUtteranceStart) — never trimming audio a
// still-open utterance's eventual pre-roll might need
// (implementation-plan.md defect #8). Journal itself has no opinion on
// that; it trusts the seq it is given.
func (j *Journal) TrimBefore(seq uint64) {
	j.committedSeq = seq
	j.haveCommitted = true
}

// Len reports how many records are currently retained (<= Cap).
func (j *Journal) Len() int { return j.size }

// Cap reports the fixed capacity this Journal was constructed with.
func (j *Journal) Cap() int { return len(j.records) }

// SpanMs is how much audio time the retained records cover. Reported for
// the dashboard's recovery-tier view, where the useful question is not
// "how many records" but "how many seconds of audio could this session
// rebuild from" — which is the actual bound on what a cross-model
// failover can replay.
func (j *Journal) SpanMs() int64 {
	var total float64
	for _, r := range j.entries() {
		total += r.DurationMs
	}
	return int64(total)
}
