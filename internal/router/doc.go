// Package router selects and pins a worker per session: filter (health,
// rate-limit budget, memory headroom, capability match for mode) then score
// (least-outstanding, compatibility-key preference). A library inside the
// gateway process, not a service — see docs/build-plan.md "Hop 3". Filled
// in at M3 (mock backends) and extended at M6 (capability filtering).
package router
