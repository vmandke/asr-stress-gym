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
| Model weights | Fetched by `models/fetch.sh` at **image build time only**, baked into the worker image, never downloaded when a container starts. One image hosts all five adapters, so the ~365MB of weights is a single shared layer rather than five copies. |
| Runtime versions | **Pinned exactly** in `worker/pyproject.toml`, not floored. `runtime_version` is one of the seven fields of the compatibility key, so an unpinned upgrade would silently change the fleet's identity — two workers built a week apart would stop being same-model and the cheap failover path would quietly disappear. A dependency bump is a fleet change. |
| What "same model" means | Two workers are same-model because they run the same `ADAPTER` against the same baked weights — a fact about the image — not because they share a `MODEL` label. `MODEL` is display-only from M5 onward. |
| Admission | Checked **once**, at `session.start`, and never again. Refusal is `overloaded` (retryable), not `error` (terminal). There is deliberately no mechanism capable of dropping an admitted session, because that is the only way to actually guarantee build-plan.md's step 5. |
| Queueing | **None.** A refused session is refused now, not parked. build-plan.md: "Queueing converts a fast failure into a slow one. A transcript delivered eight seconds late is worthless." |
| Rate-limit response | Never sleep on the online path. A 429 zeroes that worker's bucket for its own `Retry-After` and the session moves to another backend; backoff-and-retry is for the offline path, where nobody is waiting. |
| Benchmark corpus | `corpus/large`, generated on demand and git-ignored (~200MB). Reproducible from `--seed`, so "the same corpus" is a number you pass rather than a blob you ship. The committed `corpus/*.wav` stay as protocol-level ground truth. |

## The rate-limit division problem (deferred, not forgotten)

build-plan.md flags it: "If each of N gateways runs a token bucket sized
to the provider's full limit, you will overshoot by N×."

With one gateway that does not bite, so `internal/router.Bucket` is sized
per-gateway today. It is recorded here because the HA profile (M9) adds a
second gateway, and that is the moment the bug appears — silently, as
workers refusing traffic the fleet thought it had budget for. Whoever
lands HA either divides the limit by the gateway count or moves the budget
somewhere shared; what they must not do is add a gateway and leave this
alone.

## M5 deviations from the planned fleet

Recorded here rather than silently absorbed, because
[implementation-plan.md](implementation-plan.md)'s fleet table names
specific checkpoints and two of them did not survive contact.

| Planned | Built | Why |
|---|---|---|
| `worker-c`: `zipformer-ctc-en` (sherpa-onnx) | NVIDIA fast-conformer CTC, English, 480ms, int8 (`conformer_ctc`) | No English streaming zipformer-CTC export is published — the upstream streaming zipformer-CTC models are Chinese. Pointing an English corpus at a Chinese model would make every transcript assertion in the suite meaningless. The role the plan actually needed was *a streaming backend of a different family, with a different state shape and a different key*, and this fills it exactly. |
| `worker-e`: whisper-tiny.en under raw `onnxruntime` | whisper-tiny.en under ONNX Runtime **via sherpa-onnx's** offline Whisper recognizer (`whisper_onnx`) | Same runtime underneath, minus a hand-written mel/beam-search implementation that would have been this project's largest piece of un-graded code. The d↔e claim — same weights, different runtime, therefore cache-incompatible — is unaffected and is asserted directly by `test_same_weights_under_different_runtimes_are_incompatible`. |

## Findings from building against the real models

Both were found by running the models, not by reading about them, and
both are the kind of thing that would otherwise surface much later
wearing a disguise.

- **sherpa-onnx's Whisper recognizer SIGSEGVs on a zero-length buffer** —
  it takes the whole worker process down with no Python exception. An
  utterance that ends having buffered nothing is entirely ordinary (a
  session closed during silence), and without a guard it would look
  identical to a crashed worker: exactly the signal chaos testing depends
  on being real. `adapters/buffered.py`'s `MIN_UTTERANCE_S` guard exists
  for this and is covered by `test_finalize_with_no_audio_is_safe`.
- **A streaming encoder needs tail padding before `input_finished()`** or
  the final is truncated — `"...near the river"` instead of
  `"...near the river bank"` on zipformer, and `"...lazy dog ne"` on the
  CTC model. 0.6s of trailing silence recovers the full text on both; 0.3s
  recovers only part of it. Measured on `corpus/06_medium_sentence.wav`,
  and reported rather than defended (`sherpa_online.TAIL_PADDING_S`).

## Why these are decisions, not defaults

[build-plan.md](build-plan.md) intentionally left each of these open,
flagging them as assumptions to state rather than guess silently. Guessing
silently mid-build is worse than stating an assumption once, up front, so
they were resolved here before any milestone that depends on them started.
