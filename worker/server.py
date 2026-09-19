"""ASR Stress Gym worker process: the open/push/flush/restore/checkpoint/
close/health HTTP surface from docs/PROTOCOL.md, hosting whichever
Adapter ADAPTER selects (worker/adapters/registry.py). One env var swaps
the model and touches no Go code — see docs/implementation-plan.md,
"Adapter registry and conformance".

`push` reads a raw binary body (PCM bytes) with metadata in headers, not
JSON — implementation-plan.md defect #5. Every other endpoint is JSON.

Request-level fault injection (slow/blackhole/429/corrupt) lives here, as
module state toggled by /admin/*. Process-level fault injection (die/
restore) does NOT: this process can't resurrect itself after os._exit(),
so that needs a separate, surviving parent — see supervisor.py, which
spawns this file as a child and is what the worker's Docker CMD actually
runs.
"""

from __future__ import annotations

import asyncio
import base64
import io
import os
import random
import time
import uuid
import wave
from collections import deque
from statistics import median

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, PlainTextResponse

from adapters import pcm
from adapters.base import NotSupported
from adapters.registry import build as build_adapter
from state import HandleNotFound, StaleGeneration, StateStore

WORKER_ID = os.environ.get("WORKER_ID", "worker-unknown")
MODEL = os.environ.get("MODEL", "unset")
ADAPTER_NAME = os.environ.get("ADAPTER", "mock")
PORT = int(os.environ.get("HEALTH_PORT", "9000"))

# MODEL doubles as the mock adapter's identity (see adapters/mock.py) —
# worker-a/worker-b share a MODEL value and so share a compatibility key;
# worker-c's differs. No separate env var: compose already assigns MODEL
# per fleet member for display purposes, so this reuses it rather than
# adding a second knob that could drift from the first.
adapter = build_adapter(ADAPTER_NAME, model_id=None if MODEL == "unset" else MODEL)
store = StateStore()
STARTED_AT = time.time()

# Rolling RTF window (inference wall-seconds / audio-seconds), reported at
# /health as rtf_p50. Bounded like the router's own health windows
# (internal/router: windowSize=50) so a long-lived worker cannot grow it
# without limit. Real numbers from M5 onward — before the real adapters
# landed this was hardcoded None, which would have made the per-adapter RTF
# table in docs/RTF.md a fiction.
_RTF_WINDOW = 64
_rtf_samples: deque[float] = deque(maxlen=_RTF_WINDOW)


def _record_rtf(audio: bytes, elapsed_s: float) -> None:
    seconds = pcm.duration_s(audio)
    if seconds > 0:
        _rtf_samples.append(elapsed_s / seconds)


def _rtf_p50() -> float | None:
    if not _rtf_samples:
        return None
    return round(median(_rtf_samples), 4)

app = FastAPI()

# --- fault injection state (module-level; single worker process) ---
_fault_slow_ms = 0
_fault_blackhole = False
_fault_429_rate = 0.0
_fault_corrupt = False


@app.get("/health")
def health() -> dict:
    return {
        "worker_id": WORKER_ID,
        "status": "READY",
        "model": MODEL,
        "compatibility_key_hash": adapter.compatibility_key().hash(),
        "capabilities": adapter.capabilities().to_json(),
        "active_sessions": store.count(),
        # Still 0, and deliberately not invented at M5: a real adapter's
        # state is an onnxruntime-owned C++ object
        # (sherpa_onnx.OnlineStream) whose footprint Python cannot measure
        # without guessing, and a guessed number feeding the router's
        # memory-headroom filter (M7) would be worse than an honest zero.
        "state_bytes": 0,
        "queue_depth": 0,
        "rtf_p50": _rtf_p50(),
        "last_heartbeat_ms": int(time.time() * 1000),
    }


@app.post("/v1/stream/open")
async def stream_open(request: Request) -> dict:
    body = await request.json()
    session_id = body["session_id"]
    model_state = adapter.create_state(session_id)
    rec = store.open(session_id, model_state)
    return {
        "handle": rec.handle,
        "compatibility_key_hash": adapter.compatibility_key().hash(),
        "capabilities": adapter.capabilities().to_json(),
        "generation": rec.generation,
    }


@app.post("/v1/stream/push")
async def stream_push(request: Request):
    if _fault_blackhole:
        await asyncio.sleep(3600)  # accept the connection, never respond
    if _fault_slow_ms:
        await asyncio.sleep(_fault_slow_ms / 1000)
    if _fault_429_rate and random.random() < _fault_429_rate:
        return JSONResponse(status_code=429, headers={"Retry-After": "1"}, content={"error": "rate_limited"})

    handle = request.headers.get("X-Handle", "")
    seq_end = int(request.headers.get("X-Seq-End", "0"))
    expected_generation = int(request.headers.get("X-Expected-Generation", "0"))
    audio = await request.body()

    try:
        rec = store.get(handle)
    except HandleNotFound:
        raise HTTPException(status_code=404, detail="unknown handle") from None

    # Idempotent replay (docs/PROTOCOL.md "last_seq_applied"): audio at or
    # below what's already applied is a no-op, not a re-infer. This is
    # what makes the coordinator's replay-on-failover safe to call freely.
    if seq_end <= rec.last_seq_applied:
        return {
            "text": rec.last_text,
            "last_seq_applied": rec.last_seq_applied,
            "generation": rec.generation,
        }

    # Off the event loop. With the mock adapter this was merely tidy; with
    # the real ones it is required. sherpa-onnx and ctranslate2 both block
    # in C++ for the whole inference, so calling infer() inline would stall
    # every other session's HTTP handler in this process for the duration
    # — turning a concurrency measurement (M8) into a measurement of how
    # long one decode takes, serialized. Each session owns its own stream,
    # so distinct sessions decoding at once is the adapters' intended
    # usage; the two non-streaming ones additionally serialize themselves
    # (adapters/buffered.py).
    started = time.perf_counter()
    delta, next_state = await asyncio.to_thread(adapter.infer, audio, rec.model_state)
    _record_rtf(audio, time.perf_counter() - started)

    try:
        rec = store.compare_and_commit(
            handle,
            expected_generation,
            last_seq_applied=seq_end,
            model_state=next_state,
            last_text=delta.text,
        )
    except StaleGeneration:
        return JSONResponse(status_code=409, content={"error": "stale_generation"})

    return {"text": rec.last_text, "last_seq_applied": rec.last_seq_applied, "generation": rec.generation}


@app.post("/v1/stream/flush")
async def stream_flush(request: Request) -> dict:
    body = await request.json()
    try:
        rec = store.get(body["handle"])
    except HandleNotFound:
        raise HTTPException(status_code=404, detail="unknown handle") from None
    # Off the event loop for the same reason as push, and more so: on the
    # non-streaming adapters finalize IS the inference (adapters/buffered.py),
    # so this is the single most expensive call the worker makes.
    delta = await asyncio.to_thread(adapter.finalize, rec.model_state)
    return {"text": delta.text, "final": True}


# --- OpenAI-compatible transcription (the Bifrost boundary, M7) ---


@app.post("/v1/audio/transcriptions")
async def audio_transcriptions(
    file: UploadFile = File(...),
    model: str = Form(default=""),
    response_format: str = Form(default="json"),
):
    """OpenAI's transcription shape, so Bifrost can route to this worker as
    an ordinary provider (docs/build-plan.md, "The Bifrost boundary").

    **Stateless by construction, and that is the whole point.** Every other
    endpoint on this worker is part of a stateful session: `push` carries a
    handle and an expected generation, and the worker holds inference state
    keyed by that handle. Bifrost load-balances and fails over between
    providers, so a stateful call routed through it could land chunk N on
    one worker and chunk N+1 on another — the second holding no state, and
    the gateway never learning the model changed underneath it. That is
    precisely the silent corruption this project exists to prevent, which
    is why only complete-utterance requests come through here.

    So this creates fresh state, runs the whole buffer through it, finalizes
    and throws the state away. Nothing survives the request. Every adapter
    supports it, including the non-streaming ones — for those, `finalize`
    IS the inference (adapters/buffered.py).
    """
    raw = await file.read()
    try:
        pcm_bytes = _to_pcm(raw)
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

    def run() -> str:
        st = adapter.create_state(f"oai-{uuid.uuid4().hex[:8]}")
        _, st = adapter.infer(pcm_bytes, st)
        return adapter.finalize(st).text

    started = time.perf_counter()
    text = await asyncio.to_thread(run)
    _record_rtf(pcm_bytes, time.perf_counter() - started)

    if response_format == "text":
        return PlainTextResponse(text)
    # OpenAI returns {"text": ...}; the extra fields are additive and let a
    # caller see WHICH backend answered, which matters when Bifrost is the
    # thing that chose it.
    return {"text": text, "model": model or MODEL, "worker_id": WORKER_ID}


def _to_pcm(raw: bytes) -> bytes:
    """Accept either a WAV file or bare s16le PCM.

    A WAV header must be stripped rather than fed through: adapters treat
    their input as raw samples (adapters/pcm.py), so 44 bytes of "RIFF...."
    would be decoded as ~22 samples of noise at the head of every
    utterance. Clients posting to an OpenAI-shaped endpoint send a
    container, so this is the common case, not the edge one.
    """
    if raw[:4] == b"RIFF":
        with wave.open(io.BytesIO(raw)) as w:
            if (w.getframerate(), w.getnchannels(), w.getsampwidth()) != (16000, 1, 2):
                raise ValueError(
                    f"expected mono 16kHz s16le (docs/FAQ.md), got "
                    f"{w.getframerate()}Hz {w.getnchannels()}ch {w.getsampwidth() * 8}bit"
                )
            return w.readframes(w.getnframes())
    if len(raw) % 2 != 0:
        raise ValueError("bare PCM payload has an odd byte count; s16le needs 2 bytes per sample")
    return raw


@app.post("/v1/stream/restore")
async def stream_restore(request: Request):
    caps = adapter.capabilities()
    if not caps.serializable:
        return JSONResponse(status_code=501, content={"error": "not_supported"})

    body = await request.json()
    blob = base64.b64decode(body["checkpoint_blob"])
    try:
        model_state = adapter.deserialize(blob)
    except NotSupported:
        return JSONResponse(status_code=501, content={"error": "not_supported"})
    except Exception:
        # A corrupted/malformed blob (whether corrupted here via
        # /admin/corrupt or gateway-side) must fail validation, not crash
        # the worker or silently coerce — build-plan.md's checkpoint
        # design: "Validation failure means audio replay. Never
        # partial-restore, never coerce."
        return JSONResponse(status_code=422, content={"error": "invalid_checkpoint"})

    # session_id isn't known from a checkpoint blob alone; the
    # coordinator's restore path supplies the real session. last_seq_applied
    # comes from the checkpoint itself (the coordinator read it back from
    # /v1/stream/checkpoint when it took the checkpoint) so this fresh
    # record correctly reflects how much audio the restored state already
    # accounts for.
    last_seq_applied = int(body.get("last_seq_applied", 0))
    rec = store.open("restored", model_state, last_seq_applied=last_seq_applied)
    return {"handle": rec.handle, "generation": rec.generation, "last_seq_applied": rec.last_seq_applied}


@app.post("/v1/stream/checkpoint")
async def stream_checkpoint(request: Request):
    """Serialize handle's current state for the gateway to hold onto.
    Checkpoints live gateway-side (internal/coord), not here — see
    internal/backend.CheckpointResp's doc comment for why. This endpoint
    is the worker's half of that: produce the blob on request, own
    nothing about its storage or lifetime afterward.
    """
    caps = adapter.capabilities()
    if not caps.serializable:
        return JSONResponse(status_code=501, content={"error": "not_supported"})

    body = await request.json()
    try:
        rec = store.get(body["handle"])
    except HandleNotFound:
        raise HTTPException(status_code=404, detail="unknown handle") from None

    blob = adapter.serialize(rec.model_state)
    if _fault_corrupt:
        blob = bytes([b ^ 0xFF for b in blob])  # deterministically corrupt every byte

    return {
        "checkpoint_blob": base64.b64encode(blob).decode(),
        "generation": rec.generation,
        "last_seq_applied": rec.last_seq_applied,
    }


@app.post("/v1/stream/close")
async def stream_close(request: Request) -> dict:
    body = await request.json()
    store.close(body["handle"])
    return {}


# --- fault injection admin API (request-level only; see module docstring) ---


@app.post("/admin/slow")
async def admin_slow(request: Request) -> dict:
    global _fault_slow_ms
    body = await request.json()
    _fault_slow_ms = int(body.get("ms", 0))
    return {"slow_ms": _fault_slow_ms}


@app.post("/admin/blackhole")
async def admin_blackhole(request: Request) -> dict:
    global _fault_blackhole
    body = await request.json()
    _fault_blackhole = bool(body.get("on", False))
    return {"blackhole": _fault_blackhole}


@app.post("/admin/429")
async def admin_429(request: Request) -> dict:
    global _fault_429_rate
    body = await request.json()
    _fault_429_rate = float(body.get("rate", 0.0))
    return {"rate": _fault_429_rate}


@app.post("/admin/corrupt")
async def admin_corrupt(request: Request) -> dict:
    global _fault_corrupt
    body = await request.json()
    _fault_corrupt = bool(body.get("on", False))
    return {"corrupt": _fault_corrupt}


@app.post("/admin/reset")
async def admin_reset() -> dict:
    global _fault_slow_ms, _fault_blackhole, _fault_429_rate, _fault_corrupt
    _fault_slow_ms = 0
    _fault_blackhole = False
    _fault_429_rate = 0.0
    _fault_corrupt = False
    return {"reset": True}


def main() -> None:
    import uvicorn

    print(f"worker[{WORKER_ID}]: adapter={ADAPTER_NAME} model={MODEL} listening on :{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
