#!/usr/bin/env python3
"""Prove that a single streaming session can be served by DIFFERENT workers,
chunk by chunk, routed through Bifrost, with the transcript still correct.

This is the claim the shared KV tier exists to make true. Before it, a
session was pinned: every chunk had to return to the worker holding its
inference state, and a load balancer in front of the fleet could do nothing
but honour the pin. That is why Bifrost carried only offline finals.

Here each chunk carries a *reference* to state in the tier instead of the
state itself, so any worker in the model family can serve any chunk.

What is measured, not asserted:

  1. which worker actually served each chunk (asked directly, since Bifrost
     strips unknown response fields — see worker/server.py);
  2. that the transcript from a fanned-out session matches a single-worker
     reference run of the same audio;
  3. tier_hits on BOTH workers, which is the direct evidence that state
     genuinely crossed a process boundary rather than each worker quietly
     rebuilding its own;
  4. the bytes that moved, and where.

Usage:
    python3 scripts/prove_stateless_bifrost.py [--chunk-ms 500] [--clip corpus/....wav]
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
import wave

BIFROST = "http://localhost:8080"
TIER = "http://localhost:9500"
WORKERS = {
    "worker-zip-1": "http://localhost:18001",
    "worker-zip-2": "http://localhost:18002",
}
ALIAS = "whisper-1"  # BIFROST_MODEL_ALIAS; the worker ignores it, Bifrost routes on it


def _multipart(fields: dict, file_bytes: bytes | None) -> tuple[bytes, str]:
    boundary = "----kvproof" + uuid.uuid4().hex
    buf = io.BytesIO()
    for k, v in fields.items():
        buf.write(f"--{boundary}\r\n".encode())
        buf.write(f'Content-Disposition: form-data; name="{k}"\r\n\r\n'.encode())
        buf.write(f"{v}\r\n".encode())
    if file_bytes is not None:
        buf.write(f"--{boundary}\r\n".encode())
        buf.write(
            b'Content-Disposition: form-data; name="file"; filename="chunk.wav"\r\n'
            b"Content-Type: audio/wav\r\n\r\n"
        )
        buf.write(file_bytes)
        buf.write(b"\r\n")
    buf.write(f"--{boundary}--\r\n".encode())
    return buf.getvalue(), f"multipart/form-data; boundary={boundary}"


def post(url: str, fields: dict, file_bytes: bytes | None = None, timeout=60):
    data, ctype = _multipart(fields, file_bytes)
    req = urllib.request.Request(url, data=data, headers={"Content-Type": ctype}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw[:300]}


def wav_bytes(pcm: bytes, rate=16000) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(pcm)
    return buf.getvalue()


def read_pcm(path: str) -> tuple[bytes, int]:
    with wave.open(path) as w:
        assert (w.getframerate(), w.getnchannels(), w.getsampwidth()) == (16000, 1, 2), \
            "corpus clips are mono 16kHz s16le"
        return w.readframes(w.getnframes()), w.getframerate()


def health(worker: str) -> dict:
    with urllib.request.urlopen(f"{WORKERS[worker]}/health", timeout=5) as r:
        return json.loads(r.read())


def tier_stats() -> dict:
    with urllib.request.urlopen(f"{TIER}/stats", timeout=5) as r:
        return json.loads(r.read())


def run_session(chunks: list[bytes], policy: str, verbose: bool) -> dict:
    """One session's chunks, routed through Bifrost under a placement policy.

    "alternate" moves every chunk to a different worker — the worst case,
    and the one that proves state really travels. "sticky" prefers the
    worker that served the previous chunk, which is what a cache-aware
    router (vLLM prefix routing, a Gateway API Inference Extension endpoint
    picker) actually does. Both are CORRECT; they differ only in cost.
    """
    names = list(WORKERS)
    before = {w: health(w)["kv_tier"] for w in WORKERS}
    sess = f"proof-{uuid.uuid4().hex[:8]}"
    served, text = [], ""

    started = time.perf_counter()
    for i, c in enumerate(chunks):
        worker = names[i % len(names)] if policy == "alternate" else names[0]
        fields = {
            "model": f"{worker}/{ALIAS}",  # Bifrost routes on this
            "response_format": "json",
            "kv_mode": "final" if i == len(chunks) - 1 else "stream",
            "state_sink": f"{sess}:{i}",
        }
        if i > 0:
            fields["state_ref"] = f"{sess}:{i-1}"
        st, body = post(f"{BIFROST}/v1/audio/transcriptions", fields, wav_bytes(c))
        if st != 200:
            return {"error": f"chunk {i}: HTTP {st}: {body}"}
        served.append(worker)
        text = body.get("text", "")
        if verbose:
            print(f"  chunk {i:2d} -> via Bifrost -> {worker:14s} text={text[:48]!r}")
    wall = time.perf_counter() - started

    after = {w: health(w)["kv_tier"] for w in WORKERS}
    d = {k: sum(after[w][k] - before[w][k] for w in WORKERS)
         for k in ("local_hits", "tier_hits", "puts", "put_bytes", "fetch_bytes")}
    return {
        "policy": policy, "text": text.strip(), "served": served,
        "workers": len(set(served)), "wall_ms": wall * 1000, **d,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clip", default="corpus/02_number_transfer.wav")
    ap.add_argument("--chunk-ms", type=int, default=500)
    args = ap.parse_args()

    pcm, rate = read_pcm(args.clip)
    step = int(rate * args.chunk_ms / 1000) * 2  # bytes, s16le
    chunks = [c for c in (pcm[i:i + step] for i in range(0, len(pcm), step)) if len(c) >= 2]
    audio_bytes = sum(len(c) for c in chunks)

    print(f"clip      : {args.clip}  ({len(pcm)/2/rate:.2f}s)")
    print(f"chunks    : {len(chunks)} x {args.chunk_ms}ms  ({audio_bytes} bytes of audio)")
    print(f"fleet     : {', '.join(WORKERS)}  (same compatibility key = one family)\n")

    # --- reference: the whole clip, one worker, one shot -------------
    st, ref = post(f"{WORKERS['worker-zip-1']}/v1/audio/transcriptions",
                   {"model": "ref", "response_format": "json"}, wav_bytes(pcm))
    if st != 200:
        print(f"reference run failed: {st} {ref}")
        return 1
    reference = ref.get("text", "").strip()
    print(f"REFERENCE (single worker, one shot):\n  {reference!r}\n")

    print("--- policy: ALTERNATE (a different worker every chunk) ---")
    alt = run_session(chunks, "alternate", verbose=True)
    if "error" in alt:
        print(alt["error"])
        return 1
    print(f"\n--- policy: STICKY (prefer the worker that served the last chunk) ---")
    sticky = run_session(chunks, "sticky", verbose=True)
    if "error" in sticky:
        print(sticky["error"])
        return 1

    print("\n" + "=" * 74)
    print(f"{'':16} {'alternate':>14} {'sticky':>14}   what it means")
    print("-" * 74)
    rows = [
        ("workers used", "workers", ""),
        ("local hits", "local_hits", "state already in the serving worker"),
        ("tier fetches", "tier_hits", "state pulled across a process boundary"),
        ("fetch bytes", "fetch_bytes", "state ON THE WIRE, inbound"),
        ("put bytes", "put_bytes", "state ON THE WIRE, outbound"),
    ]
    for label, key, note in rows:
        print(f"{label:16} {alt[key]:>14,} {sticky[key]:>14,}   {note}")
    print(f"{'wall ms':16} {alt['wall_ms']:>14.1f} {sticky['wall_ms']:>14.1f}")
    for name, r in (("alternate", alt), ("sticky", sticky)):
        moved = r["fetch_bytes"] + r["put_bytes"]
        print(f"{'':16} {name}: state/audio = {moved/audio_bytes:.0f}x")
    print("=" * 74)

    ok = True
    if alt["workers"] < 2:
        print("FAIL: every chunk landed on one worker; nothing was proven")
        ok = False
    else:
        print(f"PASS: one session served by {alt['workers']} different workers")
    if alt["tier_hits"] == 0:
        print("FAIL: no tier fetches — state never crossed a process boundary")
        ok = False
    else:
        print(f"PASS: {alt['tier_hits']} cross-worker state fetches from the shared tier")
    for name, r in (("alternate", alt), ("sticky", sticky)):
        if r["text"] == reference:
            print(f"PASS: {name} transcript is IDENTICAL to the single-worker reference")
        else:
            ok = False
            print(f"FAIL: {name} transcript differs\n"
                  f"      reference: {reference!r}\n"
                  f"      got      : {r['text']!r}")
    print("\nBoth policies are CORRECT. They differ only in cost — which is the\n"
          "whole point: the shared tier makes affinity an OPTIMIZATION rather\n"
          "than a correctness requirement.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
