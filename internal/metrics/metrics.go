// Package metrics is deliberately minimal at M3: a handful of process-
// lifetime counters, exactly enough for the chaos scripts to assert
// docs/build-plan.md's correctness invariant — "duplicate_finals_total —
// must stay zero... asserted zero in every chaos run" — over HTTP,
// without requiring a special reset endpoint (scripts read a snapshot
// before and after a scenario and assert the DELTA, not an absolute
// value, since the gateway process is long-lived across scenarios).
//
// The full histogram set in build-plan.md's "Metrics" section (latency
// milestones, stage breakdown, cache accounting) is explicitly later
// milestone work (M6-M8) — building it now, before anything measures
// real latencies worth histogramming, would be premature.
package metrics

import "sync/atomic"

var (
	// DuplicateFinalsTotal: must stay zero. Incremented wherever
	// session.Emitter.Final reports isNew=false — see cmd/gateway.
	DuplicateFinalsTotal atomic.Int64

	// StaleGenerationWritesTotal: must stay zero on the online path at
	// M3 (invariant 3: one active owner at a time) — a non-zero value
	// here would mean two writers raced the same session's state.
	StaleGenerationWritesTotal atomic.Int64

	// FailoverTotal is SameModel + CrossModel; kept as an explicit
	// counter (not derived by summing the two) so a reader doesn't have
	// to know that invariant to get the total.
	FailoverTotal atomic.Int64

	// SameModel vs CrossModel answers ONE question: did the replacement
	// worker share the dead one's compatibility key? Nothing more.
	//
	// Until M5 these also, accidentally, answered "was a checkpoint
	// restored?", because the only same-key pair in the fleet was two
	// mock workers and mock is the only serializable adapter — the two
	// questions had the same answer in every case that existed. The real
	// adapters split them apart: worker-a and worker-b run identical
	// weights and advertise one key, and neither can serialize a thing,
	// so a→b is unambiguously a same-model failover that recovers by
	// audio replay. Counting that as cross-model, as the M3 code did,
	// made `failover_same_model_total` read 0 for a fleet whose whole
	// point is that pair.
	FailoverSameModelTotal  atomic.Int64 // replacement shared the compatibility key
	FailoverCrossModelTotal atomic.Int64 // replacement had a different key: fresh state, full replay
	FailoverExhaustedTotal  atomic.Int64 // every attempt within MaxFailoverAttempts failed

	// The warm-checkpoint tier, counted separately from the above — this
	// is the *other* axis. Exactly one of these fires per same-model
	// failover, so their sum is how often the cheap path was attempted
	// and Restores alone is how often it paid off.
	//
	// On the fleet as built, Degraded is the common case and Restores is
	// mock-only. That is the documented expectation, not a regression:
	// sherpa-onnx and CTranslate2 expose no way to serialize inference
	// state (docs/DECISIONS.md, "Checkpoint tier"), so these two counters
	// are where that degradation becomes a number instead of a claim.
	CheckpointRestoresTotal atomic.Int64 // a checkpoint was validated and restored; only the tail was replayed
	CheckpointDegradedTotal atomic.Int64 // same key, but no usable checkpoint — fell through to full audio replay
)

// Snap is a point-in-time read of every counter, JSON-tagged for the
// gateway's debug endpoint (cmd/gateway).
type Snap struct {
	DuplicateFinalsTotal       int64 `json:"duplicate_finals_total"`
	StaleGenerationWritesTotal int64 `json:"stale_generation_writes_total"`
	FailoverTotal              int64 `json:"failover_total"`
	FailoverSameModelTotal     int64 `json:"failover_same_model_total"`
	FailoverCrossModelTotal    int64 `json:"failover_cross_model_total"`
	FailoverExhaustedTotal     int64 `json:"failover_exhausted_total"`
	CheckpointRestoresTotal    int64 `json:"checkpoint_restores_total"`
	CheckpointDegradedTotal    int64 `json:"checkpoint_degraded_total"`
}

func Snapshot() Snap {
	return Snap{
		DuplicateFinalsTotal:       DuplicateFinalsTotal.Load(),
		StaleGenerationWritesTotal: StaleGenerationWritesTotal.Load(),
		FailoverTotal:              FailoverTotal.Load(),
		FailoverSameModelTotal:     FailoverSameModelTotal.Load(),
		FailoverCrossModelTotal:    FailoverCrossModelTotal.Load(),
		FailoverExhaustedTotal:     FailoverExhaustedTotal.Load(),
		CheckpointRestoresTotal:    CheckpointRestoresTotal.Load(),
		CheckpointDegradedTotal:    CheckpointDegradedTotal.Load(),
	}
}
