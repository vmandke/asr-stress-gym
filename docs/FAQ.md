# FAQ

Answers to "why" questions about locked decisions that are worth recording
once rather than re-explaining. See [DECISIONS.md](DECISIONS.md) for the
decisions themselves; this doc is the reasoning behind the ones that aren't
self-evident from a one-line table entry.

## Why mono 16kHz s16le PCM for the wire audio format?

Four reasons converged on it, not one:

1. **It's [build-plan.md](build-plan.md)'s own answer.** Its "What remains
   unspecified" section already proposes exactly this ("Assume 16kHz mono
   PCM s16le; require it... reject anything else"). [DECISIONS.md](DECISIONS.md)
   made that concrete instead of leaving it a guess.

2. **It keeps audio processing abstracted, per an explicit project
   requirement** (see [implementation-plan.md](implementation-plan.md),
   "The audio boundary"). s16le PCM is uncompressed and fixed-width, so
   validating it is arithmetic — `duration_ms = num_samples * 1000 /
   16000` — not decoding. A compressed wire format (Opus, AAC) would force
   `internal/audio` to contain a real codec, exactly the DSP surface the
   project keeps out of that boundary. Mono avoids a second judgment call
   (how to downmix stereo) that has no clean answer and doesn't matter for
   single-speaker voice traffic.

3. **It's what every model in the fleet wants natively.** sherpa-onnx
   (zipformer, zipformer-ctc), ctranslate2-whisper and onnxruntime-whisper
   all expect 16kHz mono PCM at their input layer — see the model fleet in
   implementation-plan.md. Because the whole fleet agrees, one fixed format
   means zero resampling code anywhere in the pipeline, not just deferred
   resampling code.

4. **It's declared and rejected, never inferred** — the same philosophy as
   frame-duration validation (invariant 1). `session.start` states the
   format; anything else is refused, not transcoded or guessed.

**If this needs to change:** the fleet was specifically checked for this —
every current model family converges on 16kHz mono s16le, so a single wire
format serves all of them. A model family that genuinely needs a different
native rate (8kHz telephony-style audio, for instance) is a real design
fork — per-session format negotiation in `session.start` — not a constant
to tweak. Worth a fresh decision, not a silent edit, if that fleet member
is ever added.
