#!/usr/bin/env python3
"""Measure real-time factor per adapter and write docs/RTF.md.

RTF = inference wall-seconds / audio-seconds. Below 1.0 a backend keeps up
with a realtime stream; the reciprocal is roughly how many concurrent
streams one pinned core could carry before it stops keeping up, which is
the number docs/implementation-plan.md promises to *measure and report*
rather than claim.

Adapters are exercised in-process, not over HTTP, on purpose: this
measures the model, not FastAPI, the network, or the gateway's chunking.
End-to-end latency is a different number and belongs to the benchmarks
(M8).

Streaming adapters are fed in chunks, the way the gateway feeds them, so
the number includes the per-chunk decode overhead a single whole-clip call
would hide. Non-streaming adapters buffer and do all their work in
finalize, so for them the measured cost lands there.

Usage:
    cd worker && .venv/bin/python ../scripts/measure_rtf.py
    ADAPTERS=zipformer,whisper_ct2 ... ./scripts/measure_rtf.py
"""

from __future__ import annotations

import os
import statistics
import sys
import time
import wave
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "worker"))

from adapters import pcm, weights  # noqa: E402
from adapters.registry import _REGISTRY, build  # noqa: E402

# 200ms: the gateway's chunk size is a policy of internal/audio, and this
# is its M1 pass-through value. Stated here because RTF is chunk-size
# dependent on a streaming model — a smaller chunk means more decode calls
# over the same audio — so a number quoted without one is not reproducible.
CHUNK_MS = 200
REPEATS = 3  # median of three; a single cold run is dominated by first-call warmup

# A sample of the corpus, not all 500 clips: at ~15s each, the full corpus
# would be ~2 hours of audio per adapter per repeat. Twelve clips is ~3
# minutes of audio, enough to average over utterance variety.
RTF_CLIPS = 12


def load_clip(path: Path) -> bytes:
    with wave.open(str(path)) as w:
        if (w.getframerate(), w.getnchannels(), w.getsampwidth()) != (16000, 1, 2):
            raise SystemExit(f"{path}: expected mono 16kHz s16le (see docs/FAQ.md)")
        return w.readframes(w.getnframes())


def measure(name: str, clips: list[bytes]) -> dict:
    adapter = build(name)
    caps = adapter.capabilities()
    chunk_bytes = int(pcm.SAMPLE_RATE_HZ * CHUNK_MS / 1000) * pcm.BYTES_PER_SAMPLE

    # Warm up outside the timer: the first inference pays onnxruntime's
    # graph/arena setup, which is a startup cost, not a per-utterance one.
    warm = adapter.create_state("warmup")
    _, warm = adapter.infer(clips[0], warm)
    adapter.finalize(warm)

    rtfs = []
    for _ in range(REPEATS):
        for audio in clips:
            st = adapter.create_state("rtf")
            t0 = time.perf_counter()
            for i in range(0, len(audio), chunk_bytes):
                _, st = adapter.infer(audio[i : i + chunk_bytes], st)
            adapter.finalize(st)
            elapsed = time.perf_counter() - t0
            rtfs.append(elapsed / pcm.duration_s(audio))

    rtfs.sort()
    return {
        "name": name,
        "streaming": caps.streaming,
        "serializable": caps.serializable,
        "key": adapter.compatibility_key(),
        "p50": statistics.median(rtfs),
        "p95": rtfs[min(int(len(rtfs) * 0.95), len(rtfs) - 1)],
        "n": len(rtfs),
    }


def main() -> None:
    names = os.environ.get("ADAPTERS")
    names = names.split(",") if names else sorted(_REGISTRY)
    # The large corpus (10-20s utterances), not the committed 1-3s clips:
    # RTF on a 1.2s clip is dominated by the per-utterance finalize cost,
    # which makes short-clip numbers look far worse than the steady-state
    # throughput they are quoted as. A bounded sample keeps this to
    # seconds rather than minutes while still averaging over real variety.
    corpus_dir = REPO / "corpus" / "large"
    # rglob: one subdirectory per clip kind. Taking a stride across the
    # sorted list rather than the first N keeps the sample spread over all
    # kinds — the first 12 would be twelve dialogues and would report
    # dialogue RTF as if it were the fleet's.
    all_clips = sorted(corpus_dir.rglob("*.wav"))
    stride = max(1, len(all_clips) // RTF_CLIPS) if all_clips else 1
    clip_paths = all_clips[::stride][:RTF_CLIPS]
    if not clip_paths:
        raise SystemExit(
            f"no clips in {corpus_dir} — run scripts/gen_corpus_large.py "
            "(it is git-ignored, so a fresh checkout has none)"
        )
    clips = [load_clip(p) for p in clip_paths]
    total_audio_s = sum(pcm.duration_s(c) for c in clips)

    rows = []
    for name in names:
        if not weights.available() and name != "mock":
            print(f"skipping {name}: no weights on disk (models/fetch.sh)", file=sys.stderr)
            continue
        print(f"measuring {name} ...", file=sys.stderr)
        rows.append(measure(name, clips))

    out = REPO / "docs" / "RTF.md"
    with out.open("w") as f:
        f.write("# Real-time factor, per adapter\n\n")
        f.write(
            "Generated by `scripts/measure_rtf.py` — regenerate rather than edit.\n"
            "RTF is inference wall-seconds per audio-second: **lower is faster**, and\n"
            "below 1.0 means the backend keeps up with a realtime stream.\n\n"
        )
        f.write(
            f"- Measured {date.today().isoformat()} on `{os.uname().sysname} "
            f"{os.uname().machine}`, single-threaded (`num_threads=1`, matching the\n"
            "  pinned 1.5 vCPU per worker in `docker-compose.yml`).\n"
            f"- {len(clips)} corpus clips, {total_audio_s:.1f}s of audio, "
            f"{REPEATS} repeats; streaming adapters fed in {CHUNK_MS}ms chunks.\n"
            "- Numbers from a developer host are indicative; the reproducible ones come\n"
            "  from inside the pinned containers.\n"
            "- These cover a **whole utterance**: every chunk's `infer` plus the\n"
            "  `finalize` that ends it. A worker's `/health` `rtf_p50` is a *different,\n"
            "  narrower* number — per-push `infer` only — so it reads lower, and the two\n"
            "  are not comparable. Finalize is where the streaming adapters push 0.6s of\n"
            "  tail padding and the non-streaming ones do all of their work at once,\n"
            "  which is exactly the cost a per-push figure leaves out.\n\n"
        )
        f.write("| Adapter | Family | Runtime | Streaming | Serializable | RTF p50 | RTF p95 |\n")
        f.write("|---|---|---|---|---|---|---|\n")
        for r in rows:
            k = r["key"]
            f.write(
                f"| `{r['name']}` | {k.model_family} | {k.runtime} | "
                f"{'yes' if r['streaming'] else '**no**'} | "
                f"{'**yes**' if r['serializable'] else 'no'} | "
                f"{r['p50']:.3f} | {r['p95']:.3f} |\n"
            )
        f.write(
            "\n## Reading this table\n\n"
            "`mock` is not a model: it sleeps on a fixed RTF constant "
            "(`adapters/mock.py`) so the gateway's own behaviour can be measured "
            "without a model's variance underneath it. Its row is a control, not a result.\n\n"
            "`serializable` is the column that decides which recovery path a failover "
            "takes. Only `mock` is serializable, so only `mock` exercises the warm-"
            "checkpoint tier; every real adapter degrades to audio replay, which is the "
            "documented and expected outcome rather than a gap — sherpa-onnx and "
            "CTranslate2 expose no way to serialize inference state "
            "(docs/implementation-plan.md, \"Findings that change the design\").\n\n"
            "`streaming: no` on the two Whisper adapters is why the router filters on "
            "capabilities before it scores: they are healthy backends that must never "
            "receive a mid-utterance chunk.\n"
        )
    print(f"wrote {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
