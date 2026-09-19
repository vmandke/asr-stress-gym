// Package session owns SessionInferenceState (here: InferenceState), the
// three counters (StreamEpoch, FailoverEpoch, Generation), sequence
// validation, and the transcript emission contract (revision
// monotonicity, final immutability). See docs/implementation-plan.md.
package session

import "fmt"

type Mode string

const (
	ModeOnline  Mode = "online"
	ModeOffline Mode = "offline"
)

// CacheCompatibilityKey is an opaque token the gateway only ever compares
// for equality. It deliberately does NOT mirror the 7-field struct in
// build-plan.md (ModelFamily, Runtime, Dtype, ...) — that composition is
// the worker's business alone (worker/adapters/base.py's CompatKey,
// hashed into this token by the worker and returned from `open`).
// Carrying the full struct here would mean the gateway knows a backend is
// a model, which is exactly what build-plan.md's own boundary rule 2
// forbids ("Coordinator never knows a backend is a model"). Equality of
// this token is all a router needs to prefer a worker for cheap
// same-model recovery (M3).
type CacheCompatibilityKey string

// InferenceState is the platform-side session record. It holds no model
// state — ModelState lives entirely behind the worker's HTTP surface,
// addressed only by Handle. See implementation-plan.md's Interface
// contracts: this is what defect #1 (three conflicting backend
// interfaces) collapses down to on the gateway side.
type InferenceState struct {
	SessionID   string
	UtteranceID uint64
	// UtteranceStartSeq is the sequence at which VAD confirmed the current
	// utterance. It is set at M4's speech.start and used for the auto-final
	// range; a client-ended, never-voiced session retains the zero default.
	UtteranceStartSeq uint64
	Mode              Mode
	SampleRateHz      int // set once from session.start, alongside Mode — needed again at M3 to re-Open against a replacement worker on failover

	// ownership
	WorkerID         string
	Handle           string // opaque, returned by backend.Client.Open
	CompatibilityKey CacheCompatibilityKey

	// the three counters — never collapse them (invariant 11)
	StreamEpoch   uint64 // bumped on client reconnect
	FailoverEpoch uint64 // bumped on backend change
	Generation    uint64 // bumped on state mutation (worker-side; mirrored here after each push)

	// progress
	LastAppliedSeq uint64
	CommittedSeq   uint64 // last FINAL boundary

	// transcript — all three are scoped to the CURRENT utterance
	// (UtteranceID) and reset by NewUtterance, never accumulated across
	// utterance transitions. Emitter.Final's dedupe (events.go) is keyed
	// off Finalized for exactly this reason: gating it session-wide
	// instead would permanently block every later utterance's partials
	// the moment any one utterance finalized.
	PartialRevision uint64
	LastPartial     string
	Finalized       bool
}

func NewInferenceState(sessionID string, mode Mode) *InferenceState {
	return &InferenceState{
		SessionID:   sessionID,
		UtteranceID: 1, // the session's first utterance
		Mode:        mode,
	}
}

// NewUtterance advances the session to a new utterance, resetting the
// per-utterance emission counters (invariant 10: revision is strictly
// increasing WITHIN an utterance, not across them). LastAppliedSeq and
// CommittedSeq are untouched — sequence numbers are session-wide, never
// reset per utterance.
//
// Not called anywhere yet: M1/M2 sessions live their whole life as
// UtteranceID 1 (no VAD-driven utterance boundaries until M4). Added now,
// ahead of that caller, so the multi-utterance case has a real,
// independently-testable transition rather than being unverifiable until
// M4 lands — see internal/session/events_test.go.
func (s *InferenceState) NewUtterance() {
	s.UtteranceID++
	s.UtteranceStartSeq = 0
	s.PartialRevision = 0
	s.LastPartial = ""
	s.Finalized = false
}

// UtteranceLabel is the wire-facing "u<N>" form used in transcript events
// (docs/PROTOCOL.md).
func (s *InferenceState) UtteranceLabel() string {
	return fmt.Sprintf("u%d", s.UtteranceID)
}
