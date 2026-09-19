"""Integration tests over the real HTTP surface (docs/PROTOCOL.md),
against server.app with the mock adapter — no network, no subprocess.
"""

from __future__ import annotations

import base64

from fastapi.testclient import TestClient

import server

client = TestClient(server.app)

AUDIO_HEADERS_CT = {"Content-Type": "application/octet-stream"}


def _open(session_id: str) -> dict:
    r = client.post("/v1/stream/open", json={"session_id": session_id, "sample_rate_hz": 16000, "mode": "online"})
    assert r.status_code == 200
    return r.json()


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "READY"
    assert body["compatibility_key_hash"].startswith("sha256:")


def test_open_reports_capabilities_and_generation_zero():
    body = _open("s1")
    assert body["generation"] == 0
    assert body["capabilities"]["streaming"] is True
    assert body["capabilities"]["serializable"] is True
    assert "handle" in body and body["handle"]


def test_open_push_flush_close_happy_path():
    handle = _open("s2")["handle"]
    audio = b"\x00\x01" * 320  # 640 bytes = 320 samples @ s16le

    r = client.post(
        "/v1/stream/push",
        content=audio,
        headers={**AUDIO_HEADERS_CT, "X-Handle": handle, "X-Seq-Start": "0", "X-Seq-End": "10", "X-Expected-Generation": "0"},
    )
    assert r.status_code == 200
    push = r.json()
    assert push["generation"] == 1
    assert push["last_seq_applied"] == 10
    assert push["text"] == "mock1"

    r = client.post("/v1/stream/flush", json={"handle": handle})
    assert r.status_code == 200
    assert r.json()["final"] is True

    r = client.post("/v1/stream/close", json={"handle": handle})
    assert r.status_code == 200


def test_push_idempotent_replay_is_a_noop():
    """The property docs/PROTOCOL.md calls out as carrying the correctness
    weight: replaying an already-applied seq range must not re-infer."""
    handle = _open("s3")["handle"]
    audio = b"\x00\x01" * 320
    headers = {**AUDIO_HEADERS_CT, "X-Handle": handle, "X-Seq-Start": "0", "X-Seq-End": "10", "X-Expected-Generation": "0"}

    first = client.post("/v1/stream/push", content=audio, headers=headers).json()
    replayed = client.post("/v1/stream/push", content=audio, headers=headers).json()

    assert replayed == first, "replaying an applied seq range must be a byte-identical no-op"
    assert replayed["generation"] == first["generation"], "a replay must not bump generation (no re-infer happened)"


def test_push_stale_generation_returns_409():
    handle = _open("s4")["handle"]
    audio = b"\x00\x01" * 320
    r = client.post(
        "/v1/stream/push",
        content=audio,
        headers={**AUDIO_HEADERS_CT, "X-Handle": handle, "X-Seq-Start": "0", "X-Seq-End": "10", "X-Expected-Generation": "999"},
    )
    assert r.status_code == 409
    assert r.json()["error"] == "stale_generation"


def test_push_unknown_handle_returns_404():
    r = client.post(
        "/v1/stream/push",
        content=b"",
        headers={**AUDIO_HEADERS_CT, "X-Handle": "does-not-exist", "X-Seq-Start": "0", "X-Seq-End": "1", "X-Expected-Generation": "0"},
    )
    assert r.status_code == 404


def test_restore_round_trips_with_serializable_adapter():
    from adapters.mock import MockState

    st = MockState(session_id="orig", chunks_seen=3, tokens=["a", "b", "c"])
    blob = server.adapter.serialize(st)

    r = client.post("/v1/stream/restore", json={"checkpoint_blob": base64.b64encode(blob).decode()})
    assert r.status_code == 200
    body = r.json()
    assert body["generation"] == 0
    assert body["handle"]


def test_restore_returns_501_when_adapter_is_not_serializable(monkeypatch):
    from adapters.base import Capabilities

    monkeypatch.setattr(
        server.adapter,
        "capabilities",
        lambda: Capabilities(
            streaming=True, serializable=False, endpointing=False,
            modes=frozenset({"online"}), min_chunk_ms=20, max_chunk_ms=5000,
        ),
    )
    r = client.post("/v1/stream/restore", json={"checkpoint_blob": "aGVsbG8="})
    assert r.status_code == 501
    assert r.json()["error"] == "not_supported"
