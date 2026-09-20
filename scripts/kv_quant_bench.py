#!/usr/bin/env python3
"""Benchmark J: what does quantizing the KV cache cost, and buy?

Run from worker/ with its venv, e.g.:

    cd worker && .venv/bin/python ../scripts/kv_quant_bench.py

The question is NOT "how much memory does int8 save" — that is arithmetic,
1.09 MB to 273 KB. The question is whether a restore from a quantized
checkpoint still produces the same transcript, because a cache that halves
transfer cost and changes the words is worthless here.

Method, per clip and per width:

  1. transcribe uninterrupted                        -> reference text
  2. transcribe half, checkpoint AT THAT WIDTH, restore, finish
  3. compare, character for character

Any drift is reported as drift. There is no tolerance band, because the
repository's claim is that a failover is invisible to the client, and
"invisible except for some words" is not that claim.
"""

from __future__ import annotations

import sys
import time
import wave
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "worker"))

from adapters.registry import build  # noqa: E402
from kvcache import serialize  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
WIDTHS = ("fp32", "fp16", "int8")


def read_pcm(path: Path) -> bytes:
    with wave.open(str(path), "rb") as w:
        return w.readframes(w.getnframes())


def halves(pcm: bytes) -> tuple[bytes, bytes]:
    mid = (len(pcm) // 2) & ~1  # s16le sample alignment
    return pcm[:mid], pcm[mid:]


def reference(adapter, pcm: bytes) -> str:
    st = adapter.create_state("ref")
    _, st = adapter.infer(pcm, st)
    return adapter.finalize(st).text


def restored_text(adapter, pcm: bytes, width: str) -> tuple[str, int, float]:
    """Checkpoint mid-utterance at `width`, restore, finish."""
    first, second = halves(pcm)
    st = adapter.create_state("killed")
    _, st = adapter.infer(first, st)

    t0 = time.perf_counter()
    blob = serialize(st.bank, adapter._layout, adapter.compatibility_key().hash(), dtype=width)
    encode_ms = (time.perf_counter() - t0) * 1000

    st2 = adapter.deserialize(blob)
    _, st2 = adapter.infer(second, st2)
    return adapter.finalize(st2).text, len(blob), encode_ms


def main() -> int:
    clips = sorted((REPO / "corpus").glob("*.wav"))
    if not clips:
        print("no corpus clips", file=sys.stderr)
        return 1

    adapter = build("zipformer_kv")
    print(f"clips: {len(clips)}   widths: {', '.join(WIDTHS)}\n")

    rows = []
    for width in WIDTHS:
        # The adapter's own _dtype governs what deserialize expects; the
        # blob carries its width in the header, so we only need to vary the
        # serialize side.
        identical = 0
        total_bytes = 0
        total_ms = 0.0
        drifts = []
        for clip in clips:
            pcm = read_pcm(clip)
            ref = reference(adapter, pcm)
            got, nbytes, ms = restored_text(adapter, pcm, width)
            total_bytes += nbytes
            total_ms += ms
            if got == ref:
                identical += 1
            else:
                drifts.append((clip.name, ref, got))
        rows.append((width, identical, len(clips), total_bytes // len(clips), total_ms / len(clips), drifts))

    print(f"{'width':6} {'identical':>12} {'blob':>10} {'encode':>9}")
    for width, ok, n, avg_bytes, avg_ms, _ in rows:
        print(f"{width:6} {f'{ok}/{n}':>12} {avg_bytes/1e6:>7.2f} MB {avg_ms:>7.1f} ms")

    print()
    for width, ok, n, _, _, drifts in rows:
        if not drifts:
            print(f"{width}: no drift")
            continue
        print(f"{width}: {len(drifts)} clip(s) drifted")
        for name, ref, got in drifts[:3]:
            print(f"    {name}")
            print(f"      reference: {ref!r}")
            print(f"      restored : {got!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
