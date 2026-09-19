// Package coord is the Session Coordinator: drives both failover recovery
// algorithms (same-model checkpoint+tail-replay, cross-model fresh-state+
// full-replay), compare-and-commit, and the bounded failure-handling loop.
// This is the thesis of the project. Filled in at M3, before VAD and before
// real models — see docs/implementation-plan.md.
package coord
