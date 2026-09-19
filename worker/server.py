"""ASR Stress Gym worker process: the open/push/flush/restore/close/health
HTTP surface from docs/PROTOCOL.md, hosting whichever Adapter ADAPTER
selects (worker/adapters/registry.py). One env var swaps the model and
touches no Go code — see docs/implementation-plan.md, "Adapter registry
and conformance".

`push` reads a raw binary body (PCM bytes) with metadata in headers, not
JSON — implementation-plan.md defect #5. Every other endpoint is JSON.
"""

from __future__ import annotations

import base64
import os
import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from adapters.base import NotSupported
from adapters.registry import build as build_adapter
from state import HandleNotFound, StaleGeneration, StateStore

WORKER_ID = os.environ.get("WORKER_ID", "worker-unknown")
MODEL = os.environ.get("MODEL", "unset")
ADAPTER_NAME = os.environ.get("ADAPTER", "mock")
PORT = int(os.environ.get("HEALTH_PORT", "9000"))

adapter = build_adapter(ADAPTER_NAME)
store = StateStore()
STARTED_AT = time.time()

app = FastAPI()


@app.get("/health")
def health() -> dict:
    return {
        "worker_id": WORKER_ID,
        "status": "READY",
        "model": MODEL,
        "compatibility_key_hash": adapter.compatibility_key().hash(),
        "active_sessions": store.count(),
        "state_bytes": 0,  # real accounting lands with the real adapters (M5)
        "queue_depth": 0,
        "rtf_p50": None,
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

    delta, next_state = adapter.infer(audio, rec.model_state)
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
    delta = adapter.finalize(rec.model_state)
    return {"text": delta.text, "final": True}


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

    # session_id isn't known from a checkpoint blob alone at M1; the
    # coordinator supplies the real session on the M3 restore path, which
    # also carries the seq the checkpoint was taken at.
    rec = store.open("restored", model_state)
    return {"handle": rec.handle, "generation": rec.generation, "last_seq_applied": 0}


@app.post("/v1/stream/close")
async def stream_close(request: Request) -> dict:
    body = await request.json()
    store.close(body["handle"])
    return {}


def main() -> None:
    import uvicorn

    print(f"worker[{WORKER_ID}]: adapter={ADAPTER_NAME} listening on :{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
