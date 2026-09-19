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
	FailoverTotal           atomic.Int64
	FailoverSameModelTotal  atomic.Int64 // the cheap path: checkpoint restored, tail replayed
	FailoverCrossModelTotal atomic.Int64 // fresh state + full replay — also same-model-with-no-valid-checkpoint, which degrades here
	FailoverExhaustedTotal  atomic.Int64 // every attempt within MaxFailoverAttempts failed
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
}

func Snapshot() Snap {
	return Snap{
		DuplicateFinalsTotal:       DuplicateFinalsTotal.Load(),
		StaleGenerationWritesTotal: StaleGenerationWritesTotal.Load(),
		FailoverTotal:              FailoverTotal.Load(),
		FailoverSameModelTotal:     FailoverSameModelTotal.Load(),
		FailoverCrossModelTotal:    FailoverCrossModelTotal.Load(),
		FailoverExhaustedTotal:     FailoverExhaustedTotal.Load(),
	}
}
