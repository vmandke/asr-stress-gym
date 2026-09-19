package session

import "fmt"

// Transcript event JSON shapes — docs/PROTOCOL.md, "Gateway -> Client".
// Struct tags match the wire shape exactly so these can be marshaled
// directly by cmd/gateway's writeLoop.

type AckEvent struct {
	Type                 string `json:"type"` // "ack"
	SessionID            string `json:"session_id"`
	HighestContiguousSeq uint64 `json:"highest_contiguous_seq"`
}

type PartialEvent struct {
	Type          string `json:"type"` // "partial"
	SessionID     string `json:"session_id"`
	UtteranceID   string `json:"utterance_id"`
	Revision      uint64 `json:"revision"`
	FailoverEpoch uint64 `json:"failover_epoch"`
	Text          string `json:"text"`
}

// PartialResetEvent tells the client to discard what's displayed — a
// failover occurred. It carries its own Revision, part of the SAME
// strictly-increasing sequence as PartialEvent (build-plan.md's example:
// revision 17 [partial], 18 [partial.reset], 19 [partial] — reset doesn't
// restart the count, it's simply the next emission in it).
type PartialResetEvent struct {
	Type          string `json:"type"` // "partial.reset"
	SessionID     string `json:"session_id"`
	UtteranceID   string `json:"utterance_id"`
	Revision      uint64 `json:"revision"`
	FailoverEpoch uint64 `json:"failover_epoch"`
}

type FinalEvent struct {
	Type        string `json:"type"` // "final"
	SessionID   string `json:"session_id"`
	UtteranceID string `json:"utterance_id"`
	SeqStart    uint64 `json:"seq_start"`
	SeqEnd      uint64 `json:"seq_end"`
	Text        string `json:"text"`
}

type DiscontinuityEvent struct {
	Type      string `json:"type"` // "discontinuity"
	SessionID string `json:"session_id"`
	SeqStart  uint64 `json:"seq_start"`
	SeqEnd    uint64 `json:"seq_end"`
}

type ErrorEvent struct {
	Type      string `json:"type"` // "error"
	SessionID string `json:"session_id"`
	Reason    string `json:"reason"`
}

// Emitter is the ONLY place transcript-emission invariants are enforced
// (implementation-plan.md defect #10: finals-dedupe was previously
// unspecified). One Emitter per InferenceState, owned by the same
// single-writer sessionLoop that owns the state itself.
//
// Enforced here:
//   - invariant 9: no event follows a final for the same utterance_id.
//     Partial (and, from M3, partial.reset) panic if called after Final —
//     that is a coordinator bug, not a runtime condition to handle
//     gracefully, so it is asserted loudly in test/dev builds rather than
//     silently ignored.
//   - invariant 10: PartialRevision strictly increases within an
//     utterance; a partial always replaces, never merges.
//   - "re-delivering an identical final is a no-op": Final is idempotent —
//     calling it again after the utterance is finalized returns the same
//     event and reports isNew=false, so an at-least-once caller can
//     re-send safely.
//
// Ack and Discontinuity are session/seq-scoped, not utterance-scoped
// (neither carries utterance_id on the wire — see docs/PROTOCOL.md), so
// they are exempt from the "not after final" guard: they may legitimately
// continue after the utterance they were about has finalized.
type Emitter struct {
	state     *InferenceState
	lastFinal *FinalEvent
}

func NewEmitter(state *InferenceState) *Emitter {
	return &Emitter{state: state}
}

func (e *Emitter) mustNotBeFinalized(eventType string) {
	if e.state.Finalized {
		panic(fmt.Sprintf("session: %s emitted after final for utterance %s (session %s) — invariant 9 violated",
			eventType, e.state.UtteranceLabel(), e.state.SessionID))
	}
}

func (e *Emitter) Ack(highestContiguousSeq uint64) AckEvent {
	return AckEvent{
		Type:                 "ack",
		SessionID:            e.state.SessionID,
		HighestContiguousSeq: highestContiguousSeq,
	}
}

func (e *Emitter) Partial(text string) PartialEvent {
	e.mustNotBeFinalized("partial")
	e.state.PartialRevision++
	e.state.LastPartial = text
	return PartialEvent{
		Type:          "partial",
		SessionID:     e.state.SessionID,
		UtteranceID:   e.state.UtteranceLabel(),
		Revision:      e.state.PartialRevision,
		FailoverEpoch: e.state.FailoverEpoch,
		Text:          text,
	}
}

// PartialReset tells the client to discard the displayed partial — a
// failover just happened. Always emitted on ANY failover, same-model or
// cross (implementation-plan.md defect #9: build-plan.md's suggestion
// that same-model recovery "may continue without reset" would need a
// safe cross-generation text diff to justify skipping this, which is a
// correctness risk for a cosmetic saving — this implementation never
// takes it). Bumps PartialRevision like Partial does, and is subject to
// the same "not after final" guard.
func (e *Emitter) PartialReset() PartialResetEvent {
	e.mustNotBeFinalized("partial.reset")
	e.state.PartialRevision++
	e.state.LastPartial = ""
	return PartialResetEvent{
		Type:          "partial.reset",
		SessionID:     e.state.SessionID,
		UtteranceID:   e.state.UtteranceLabel(),
		Revision:      e.state.PartialRevision,
		FailoverEpoch: e.state.FailoverEpoch,
	}
}

// Final locks the current utterance. isNew is false when this utterance
// was already finalized — the caller (an at-least-once delivery path)
// gets back the same event rather than a fresh one, which is what makes
// re-delivery a no-op instead of a duplicate.
func (e *Emitter) Final(text string, seqStart, seqEnd uint64) (ev FinalEvent, isNew bool) {
	if e.state.Finalized {
		return *e.lastFinal, false
	}
	e.state.Finalized = true
	e.state.CommittedSeq = seqEnd
	ev = FinalEvent{
		Type:        "final",
		SessionID:   e.state.SessionID,
		UtteranceID: e.state.UtteranceLabel(),
		SeqStart:    seqStart,
		SeqEnd:      seqEnd,
		Text:        text,
	}
	e.lastFinal = &ev
	return ev, true
}

func (e *Emitter) Discontinuity(r DiscontinuityRange) DiscontinuityEvent {
	return DiscontinuityEvent{
		Type:      "discontinuity",
		SessionID: e.state.SessionID,
		SeqStart:  r.SeqStart,
		SeqEnd:    r.SeqEnd,
	}
}

func (e *Emitter) Error(reason string) ErrorEvent {
	return ErrorEvent{
		Type:      "error",
		SessionID: e.state.SessionID,
		Reason:    reason,
	}
}
