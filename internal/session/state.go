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
	Mode        Mode

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

	// transcript
	PartialRevision uint64
	LastPartial     string
	Finalized       bool // M1: single implicit utterance per session; full
	// per-utterance finals-dedupe set (needed once a session can carry
	// more than one utterance) lands at M2.
}

func NewInferenceState(sessionID string, mode Mode) *InferenceState {
	return &InferenceState{
		SessionID:   sessionID,
		UtteranceID: 1, // M1: no VAD-driven utterance boundaries yet, so a
		// session has exactly one utterance for its whole life. M4 assigns
		// new UtteranceIDs on each speech.start.
		Mode: mode,
	}
}

// UtteranceLabel is the wire-facing "u<N>" form used in transcript events
// (docs/PROTOCOL.md).
func (s *InferenceState) UtteranceLabel() string {
	return fmt.Sprintf("u%d", s.UtteranceID)
}
