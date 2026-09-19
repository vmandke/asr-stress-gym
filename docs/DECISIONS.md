# Locked decisions

These replace [build-plan.md](build-plan.md)'s "What remains unspecified" —
resolved during planning rather than left to be guessed mid-build. See
[implementation-plan.md](implementation-plan.md) for the reasoning behind
each.

| Question | Decision |
|---|---|
| Audio format | Mono 16kHz s16le PCM, required in `session.start`, validated not guessed. Format handling lives entirely inside `internal/audio`; the rest of the system is format-agnostic. Why this exact format: [FAQ.md](FAQ.md). |
| Latency milestone | `final_latency_ms` (endpoint decision → `final` on the wire) p95 ≤ 300ms; `partial_latency_ms` p95 ≤ 250ms. **All server-side.** The interview's ~100ms network term is not observable inside `docker compose`; it is budgeted, not measured, and the README says so. |
| Concurrency target | 200 concurrent online streams on the mock adapter; 20 on a real sherpa-onnx streaming adapter. Both measured and reported, not claimed in advance. |
| Partial transcripts | Yes for online mode, no for offline mode. |
| Checkpoint tier | Real (serialize/deserialize) on the mock adapter only. Every real adapter reports `serializable == False`, which exercises the documented degradation path (fresh state + full audio replay) rather than hiding it. |
| VAD | Library-backed, behind `audio.VAD`, one implementation swappable for another. Thresholds are config; the values chosen are reported in the README, not defended as correct. |
| Gateway↔worker transport | HTTP/1.1 keep-alive, binary body for audio, JSON for control/response. |
| Bifrost | Finals and offline jobs only, behind `BIFROST_ENABLED`, with a direct-to-worker fallback always present. First thing cut if the schedule slips (see implementation-plan.md "Risks and the cut line"). |

## Why these are decisions, not defaults

[build-plan.md](build-plan.md) intentionally left each of these open,
flagging them as assumptions to state rather than guess silently. Guessing
silently mid-build is worse than stating an assumption once, up front, so
they were resolved here before any milestone that depends on them started.
