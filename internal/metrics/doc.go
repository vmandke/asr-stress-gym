// Package metrics defines the histograms, gauges and counters listed in
// docs/build-plan.md "Metrics", including duplicate_finals_total and
// stale_generation_writes_total, which every chaos scenario asserts stay
// zero. Filled in alongside the components that emit them, M1 onward.
package metrics
