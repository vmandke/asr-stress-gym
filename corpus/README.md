# Corpus

Committed here at M1: ~20 short clips generated from known text, 16kHz
mono s16le, plus `transcripts.json` mapping each clip to its ground-truth
text. Known text means chaos and VAD assertions can check *correctness*
(does the first word survive pre-roll, does the transcript match), not
just latency — see [../docs/build-plan.md](../docs/build-plan.md#the-load-generator).

Not generated at container runtime and not downloaded on demand: a
reviewer on a bad connection still gets a working demo.
