#!/usr/bin/env python3
"""Benchmark cache reuse against rebuilding a complete utterance.

This talks directly to the serializable mock worker.  In ``cached`` mode
one handle receives successive 160 ms chunks; in ``rebuild`` mode every
point opens a fresh handle and sends all audio accumulated so far.  The
worker returns its own additive ``inference_ms`` field, so the chart is
model service time rather than the benchmark client's HTTP overhead.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
import urllib.request
import wave
from pathlib import Path


SAMPLE_RATE = 16_000
CHUNK_MS = 160


def request(url: str, method: str, body: bytes, headers: dict[str, str]) -> dict:
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def open_handle(base_url: str, session_id: str) -> str:
    response = request(
        f"{base_url}/v1/stream/open",
        "POST",
        json.dumps({"session_id": session_id}).encode(),
        {"Content-Type": "application/json"},
    )
    return response["handle"]


def close_handle(base_url: str, handle: str) -> None:
    request(
        f"{base_url}/v1/stream/close",
        "POST",
        json.dumps({"handle": handle}).encode(),
        {"Content-Type": "application/json"},
    )


def push(base_url: str, handle: str, seq_end: int, audio: bytes) -> tuple[float, float]:
    started = time.perf_counter()
    response = request(
        f"{base_url}/v1/stream/push",
        "POST",
        audio,
        {
            "Content-Type": "application/octet-stream",
            "X-Handle": handle,
            "X-Seq-End": str(seq_end),
            "X-Expected-Generation": "0",
        },
    )
    request_ms = (time.perf_counter() - started) * 1000
    return float(response["inference_ms"]), request_ms


def read_pcm(path: Path) -> bytes:
    with wave.open(str(path), "rb") as wav:
        spec = (wav.getframerate(), wav.getnchannels(), wav.getsampwidth())
        if spec != (SAMPLE_RATE, 1, 2):
            raise ValueError(f"{path}: expected 16kHz mono s16le, got {spec}")
        return wav.readframes(wav.getnframes())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-url", default="http://localhost:18000")
    parser.add_argument("--clip", default="corpus/06_medium_sentence.wav")
    parser.add_argument("--max-seconds", type=float, default=8.0)
    parser.add_argument("--sample-ms", type=int, default=1000)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    pcm = read_pcm(Path(args.clip))
    max_bytes = min(len(pcm), int(args.max_seconds * SAMPLE_RATE * 2))
    chunk_bytes = SAMPLE_RATE * 2 * CHUNK_MS // 1000
    sample_bytes = SAMPLE_RATE * 2 * args.sample_ms // 1000
    if max_bytes < chunk_bytes:
        raise ValueError("clip is shorter than one benchmark chunk")

    rows: list[dict[str, float | int | str]] = []
    handle = open_handle(args.worker_url, "bench-cache")
    next_sample = sample_bytes
    try:
        for end in range(chunk_bytes, max_bytes + 1, chunk_bytes):
            inference_ms, request_ms = push(args.worker_url, handle, end // chunk_bytes, pcm[end - chunk_bytes : end])
            # 160 ms inference chunks do not evenly divide the default
            # one-second chart sample. Record the first completed chunk at
            # or after every sample boundary rather than only their 4 s LCM.
            if end >= next_sample or end + chunk_bytes > max_bytes:
                rows.append({"mode": "cached", "elapsed_audio_ms": end * 1000 // (SAMPLE_RATE * 2), "inference_ms": inference_ms, "request_ms": request_ms})
                next_sample += sample_bytes
    finally:
        close_handle(args.worker_url, handle)

    for end in range(sample_bytes, max_bytes + 1, sample_bytes):
        handle = open_handle(args.worker_url, f"bench-rebuild-{end}")
        try:
            inference_ms, request_ms = push(args.worker_url, handle, 1, pcm[:end])
            rows.append({"mode": "rebuild", "elapsed_audio_ms": end * 1000 // (SAMPLE_RATE * 2), "inference_ms": inference_ms, "request_ms": request_ms})
        finally:
            close_handle(args.worker_url, handle)

    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["mode", "elapsed_audio_ms", "inference_ms", "request_ms"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {output} ({len(rows)} samples)")


if __name__ == "__main__":
    main()
