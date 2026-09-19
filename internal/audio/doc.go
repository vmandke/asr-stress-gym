// Package audio is the ONLY code in this repository that touches PCM samples.
//
// Everything above this package — session, journal, coord, router, backend —
// deals exclusively in Ref, Chunk, Span, and declared durations. It never
// decodes, inspects, or reasons about sample content.
//
// Filled in at M4 (see docs/implementation-plan.md). M1 ships a pass-through
// Pipeline so the boundary exists from the first commit; the VAD algorithm,
// reframing window, hysteresis thresholds and chunk-cutting policy are all
// internal to this package and swappable without touching a test outside it.
package audio
