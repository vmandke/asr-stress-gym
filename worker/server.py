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
import binascii
import io
import json
import os
import random
import time
import uuid
import wave
from collections import deque
from statistics import median

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, PlainTextResponse

import resources
from adapters import pcm
from adapters.base import NotSupported
from adapters.registry import build as build_adapter
from kvtier import KVTier, TierMiss, TierUnavailable
from state import HandleNotFound, StaleGeneration, StateStore

WORKER_ID = os.environ.get("WORKER_ID", "worker-unknown")
MODEL = os.environ.get("MODEL", "unset")
ADAPTER_NAME = os.environ.get("ADAPTER", "zipformer_kv")
PORT = int(os.environ.get("HEALTH_PORT", "9000"))

# MODEL doubles as the mock adapter's identity (see adapters/mock.py) —
# worker-a/worker-b share a MODEL value and so share a compatibility key;
# worker-c's differs. No separate env var: compose already assigns MODEL
# per fleet member for display purposes, so this reuses it rather than
# adding a second knob that could drift from the first.
adapter = build_adapter(ADAPTER_NAME, model_id=None if MODEL == "unset" else MODEL)
store = StateStore()

# The shared KV tier (cmd/kvtier), plus this worker's local hot cache in
# front of it. Disabled and inert unless KVTIER_URL is set, so the default
# stateful/pinned path is untouched — see worker/kvtier.py.
tier = KVTier()

STARTED_AT = time.time()

# Rolling RTF window (inference wall-seconds / audio-seconds), reported at
# /health as rtf_p50. Bounded like the router's own health windows
# (internal/router: windowSize=50) so a long-lived worker cannot grow it
# without limit. Real numbers from M5 onward — before the real adapters
# landed this was hardcoded None, which would have made the per-adapter RTF
# table in docs/RTF.md a fiction.
_RTF_WINDOW = 64
_rtf_samples: deque[float] = deque(maxlen=_RTF_WINDOW)

# Real queue accounting, reported at /health as queue_depth and inflight.
#
# Both numbers exist because they answer different questions and diverge in
# exactly the case that matters. `inflight` is inference requests accepted
# and not yet answered; `running` is those actually executing inside
# asyncio.to_thread's executor. Their difference is work that has arrived
# and is WAITING, which is the only one of the three that means the worker
# is oversubscribed rather than merely busy.
#
# Before M9 both of these were a hardcoded 0 in /health, which made the
# router's queue-depth signal a constant and the dashboard's queue graph a
# flat line. Unlike state_bytes — still honestly 0, because a real
# adapter's state is an onnxruntime-owned C++ object Python cannot size
# without guessing — this one is measurable from here, so it is measured.
_inflight = 0
_running = 0
_inflight_high_water = 0


class _Inflight:
    """Counts one inference request across its whole handler.

    A context manager rather than manual increments because every exit
    path — a StaleGeneration 409, an HTTPException, a cancelled request
    when the client disconnects mid-inference — must decrement. A leaked
    increment here would make the worker report a queue that never drains
    and get it ejected by the router for a bookkeeping bug.
    """

    def __enter__(self) -> "_Inflight":
        global _inflight, _inflight_high_water
        _inflight += 1
        _inflight_high_water = max(_inflight_high_water, _inflight)
        return self

    def __exit__(self, *exc) -> None:
        global _inflight
        _inflight -= 1
        return None


def _run_counted(fn, *args):
    """Wrap the callable handed to asyncio.to_thread so `running` counts
    time in the executor, not time in the handler."""

    def inner():
        global _running
        _running += 1
        try:
            return fn(*args)
        finally:
            _running -= 1

    return inner


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

# Bifrost v1.5 normalizes multipart transcription requests and discards
# unknown form fields. The gateway therefore places KV *references* in the
# standard OpenAI `prompt` field. This worker unwraps them before inference;
# Bifrost never reads, writes, or carries KV bytes.
_KV_PROMPT_PREFIX = "asr-stress-gym-kv:v1:"


def _kv_prompt(prompt: str) -> tuple[str, str, str] | None:
    if not prompt.startswith(_KV_PROMPT_PREFIX):
        return None
    try:
        encoded = prompt[len(_KV_PROMPT_PREFIX):]
        padded = encoded + "=" * (-len(encoded) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
        mode = payload["m"]
        ref = payload.get("r", "")
        sink = payload.get("s", "")
        if mode not in ("stream", "final") or not all(isinstance(v, str) for v in (mode, ref, sink)):
            raise ValueError("invalid fields")
        return mode, ref, sink
    except (binascii.Error, KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
        raise ValueError("invalid KV prompt envelope") from e


def _state_bytes() -> int:
    """Total live inference state across this worker's sessions.

    Was a hardcoded 0 through M10, honestly: a real adapter's state was an
    onnxruntime-owned C++ object Python could not size, and a guessed
    number feeding the router would be worse than a truthful zero. M11's
    zipformer_kv owns its state as numpy arrays, so it can answer — and
    adapters that still cannot simply do not implement state_bytes and
    continue to report 0.
    """
    measure = getattr(adapter, "state_bytes", None)
    if measure is None:
        return 0
    total = 0
    for rec in store.all():
        try:
            total += int(measure(rec.model_state))
        except Exception:
            # Never let telemetry break a health check: an adapter that
            # raises here would take the worker out of the fleet.
            pass
    return total


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
        "state_bytes": _state_bytes(),
        # Accepted but not yet executing: the backlog. See _Inflight.
        "queue_depth": max(0, _inflight - _running),
        "inflight": _inflight,
        "running": _running,
        "inflight_high_water": _inflight_high_water,
        "rtf_p50": _rtf_p50(),
        "uptime_s": round(time.time() - STARTED_AT, 1),
        # Shared-tier locality, cumulative. This is where the gateway and
        # the dashboard read the hit rate from, because Bifrost strips
        # unknown fields out of the per-request response and cannot carry
        # it. local_hits vs tier_hits is the measurement that says whether
        # affinity is still worth preferring; see worker/kvtier.py.
        # `url` is advertised, not configured gateway-side, for the same
        # reason the compatibility key is: the gateway must never hold a
        # second copy of the family->tier mapping that could drift from
        # the fleet's own. Each family pool has its OWN tier, so that a
        # burst in one family cannot evict another family's state.
        "kv_tier": {"enabled": tier.enabled, "url": tier.base_url, **tier.stats},
        **resources.snapshot(),
        "last_heartbeat_ms": int(time.time() * 1000),
    }


@app.get("/v1/kv/layout")
def kv_layout() -> dict:
    """What this worker's KV cache actually contains, tensor by tensor.

    Exists so a UI can show the cache rather than describe it: the answer
    is derived from the ONNX graph (kvcache.StateLayout.describe), so it
    cannot claim a tensor the model does not have or a size it does not
    occupy. Adapters that own no such state simply do not implement
    kv_layout and this reports `supported: false`.
    """
    describe = getattr(adapter, "kv_layout", None)
    if describe is None:
        return {"supported": False, "worker_id": WORKER_ID, "model": MODEL}
    out = describe()
    out.update({
        "supported": True,
        "worker_id": WORKER_ID,
        "model": MODEL,
        "compatibility_key_hash": adapter.compatibility_key().hash(),
        "cache_schema_version": adapter.compatibility_key().cache_schema_version,
        "live_state_bytes": _state_bytes(),
        "active_sessions": store.count(),
    })
    return out


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
    #
    # Counted BEFORE the inflight guard on purpose: a replayed push does no
    # inference and must not appear in the queue depth, or a failover storm
    # would read as backlog on a worker that is doing nothing.
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
    with _Inflight():
        started = time.perf_counter()
        delta, next_state = await asyncio.to_thread(_run_counted(adapter.infer, audio, rec.model_state))
        inference_s = time.perf_counter() - started
    _record_rtf(audio, inference_s)

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

    # Additive timing evidence for M8. The gateway deliberately ignores
    # unknown backend fields, so this does not put a benchmark concern on
    # the streaming contract; direct benchmark probes can nevertheless
    # distinguish model service time from their own HTTP timing.
    return {
        "text": rec.last_text,
        "last_seq_applied": rec.last_seq_applied,
        "generation": rec.generation,
        "inference_ms": round(inference_s * 1000, 3),
    }


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
    with _Inflight():
        delta = await asyncio.to_thread(_run_counted(adapter.finalize, rec.model_state))
    return {"text": delta.text, "final": True}


# --- OpenAI-compatible transcription (the Bifrost boundary, M7) ---


@app.post("/v1/audio/transcriptions")
async def audio_transcriptions(
    file: UploadFile = File(...),
    model: str = Form(default=""),
    response_format: str = Form(default="json"),
    prompt: str = Form(default=""),
    state_ref: str = Form(default=""),
    state_sink: str = Form(default=""),
    kv_mode: str = Form(default=""),
):
    """OpenAI's transcription shape, so Bifrost can route to this worker as
    an ordinary provider (docs/build-plan.md, "The Bifrost boundary").

    Two behaviours share this one endpoint, because this is the ONLY shape
    Bifrost will route. Which one runs depends on `kv_mode`.

    **kv_mode absent — one-shot, stateless.** Creates fresh state, runs the
    whole buffer through it, finalizes, throws the state away. Nothing
    survives the request. This is the offline-final path.

    **kv_mode set — shared-tier streaming.** The request carries a
    *reference* to state held in the shared KV tier (cmd/kvtier) rather
    than the state itself, and names the reference its output should be
    published under. The worker resolves `state_ref` (locally if it wrote
    it last, otherwise fetching from the tier), runs one chunk, and
    publishes the result under `state_sink`.

    That is what lets a load balancer route a session's chunks to ANY
    worker in the model family: nothing the session needs lives inside a
    particular worker process any more. See worker/kvtier.py for why a
    local hot cache is part of this rather than an optimization bolted on
    after, and docs/KVCACHE-ALTERNATIVES.md for the measurement that rules out
    the obvious alternative of shipping the tensors in the request.

    **Why `prompt` carries the references.** Bifrost v1.5 strips unknown
    multipart fields. `prompt` is part of the standard OpenAI transcription
    shape and survives its normalization, so the gateway places a tiny,
    versioned reference envelope there. It contains names only; the worker
    still exchanges every KV byte directly with its own KVTier.

    That same stripping is why a cache miss is signalled as an HTTP STATUS
    rather than a response field. Status codes survive, and a miss genuinely
    means this provider cannot serve the request — so letting Bifrost try
    the next one, and ultimately surfacing the failure to the gateway, is
    the correct behaviour rather than a workaround.
    """
    raw = await file.read()
    try:
        pcm_bytes = _to_pcm(raw)
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

    if not kv_mode:
        try:
            forwarded = _kv_prompt(prompt)
        except ValueError as e:
            return JSONResponse(status_code=400, content={"error": str(e)})
        if forwarded is not None:
            kv_mode, state_ref, state_sink = forwarded

    if not kv_mode:
        def run() -> str:
            st = adapter.create_state(f"oai-{uuid.uuid4().hex[:8]}")
            _, st = adapter.infer(pcm_bytes, st)
            return adapter.finalize(st).text

        with _Inflight():
            started = time.perf_counter()
            text = await asyncio.to_thread(_run_counted(run))
            _record_rtf(pcm_bytes, time.perf_counter() - started)

        if response_format == "text":
            return PlainTextResponse(text)
        # OpenAI returns {"text": ...}; the extra fields are additive and let
        # a caller see WHICH backend answered, which matters when Bifrost is
        # the thing that chose it. (Bifrost strips them; a direct call sees
        # them, and the chaos/bench scripts call directly for that reason.)
        return {"text": text, "model": model or MODEL, "worker_id": WORKER_ID}

    if kv_mode not in ("stream", "final"):
        return JSONResponse(status_code=400, content={"error": f"unknown kv_mode {kv_mode!r}"})
    if not tier.enabled:
        return JSONResponse(status_code=503, content={"error": "kv tier not configured"})
    if not adapter.capabilities().serializable:
        return JSONResponse(status_code=501, content={"error": "not_supported"})

    compat = adapter.compatibility_key().hash()
    outcome: dict = {}

    def run_stateful() -> str:
        if state_ref:
            st, outcome["hit"] = tier.load(state_ref, adapter, compat)
        else:
            # No predecessor: the first chunk of a session.
            st, outcome["hit"] = adapter.create_state(f"kv-{uuid.uuid4().hex[:8]}"), "fresh"
        delta, st = adapter.infer(pcm_bytes, st)
        text = delta.text
        if kv_mode == "final":
            text = adapter.finalize(st).text
        if state_sink:
            outcome["bytes"] = tier.store(state_sink, st, adapter, compat)
        return text

    try:
        with _Inflight():
            started = time.perf_counter()
            text = await asyncio.to_thread(_run_counted(run_stateful))
            _record_rtf(pcm_bytes, time.perf_counter() - started)
    except TierMiss:
        # The reference resolved nowhere: evicted, expired, or the tier
        # lost it. Serving this chunk against fresh state would silently
        # drop the session's accumulated context and produce a plausible
        # but wrong transcript. Refusing is what lets the caller replay.
        return JSONResponse(
            status_code=424,
            content={"error": "state_ref_miss", "state_ref": state_ref, "worker_id": WORKER_ID},
        )
    except ValueError as e:
        # Compatibility-key mismatch — a blob from another model family.
        # 422 for the same reason /v1/stream/restore uses it: refuse,
        # never coerce.
        return JSONResponse(
            status_code=422,
            content={"error": "incompatible_state", "detail": str(e), "worker_id": WORKER_ID},
        )
    except TierUnavailable as e:
        return JSONResponse(status_code=503, content={"error": "kv_tier_unavailable", "detail": str(e)})

    if response_format == "text":
        return PlainTextResponse(text)
    return {
        "text": text,
        "model": model or MODEL,
        "worker_id": WORKER_ID,
        # Stripped by Bifrost, visible on a direct call. Cumulative
        # equivalents are on /health, which is how the gateway and the
        # dashboard actually observe locality.
        "kv_hit": outcome.get("hit", ""),
        "kv_bytes": outcome.get("bytes", 0),
    }


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
