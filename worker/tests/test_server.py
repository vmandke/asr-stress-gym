"""Integration tests over the real HTTP surface (docs/PROTOCOL.md),
against server.app with the mock adapter — no network, no subprocess.
"""

from __future__ import annotations

import base64
import json

import pytest

from fastapi.testclient import TestClient

import server

client = TestClient(server.app)

AUDIO_HEADERS_CT = {"Content-Type": "application/octet-stream"}


def test_kv_prompt_round_trips_the_gateway_reference_envelope():
    payload = {"m": "stream", "r": "kv:s1:100", "s": "kv:s1:200"}
    encoded = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode().rstrip("=")
    assert server._kv_prompt("asr-stress-gym-kv:v1:" + encoded) == ("stream", "kv:s1:100", "kv:s1:200")


def test_kv_prompt_rejects_a_malformed_envelope():
    with pytest.raises(ValueError, match="invalid KV prompt envelope"):
        server._kv_prompt("asr-stress-gym-kv:v1:not-base64")


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
    # No text assertion: 320 samples of a constant byte pattern is not
    # speech, and a real adapter correctly returns nothing for it. What
    # this test is about is the PROTOCOL — generation advanced, the seq
    # was applied — not transcription quality.
    assert "text" in push

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

    # Compare the fields that DEFINE idempotency, not the whole body.
    # `inference_ms` is a timing observation attached to work actually
    # done; a replayed push does no inference, so it correctly has none —
    # and asserting dict equality made a truthful response look like a
    # violation. The property under test is that no state moved and the
    # same transcript comes back.
    for field in ("text", "last_seq_applied", "generation"):
        assert replayed[field] == first[field], (
            f"replaying an applied seq range changed {field}: "
            f"{first[field]!r} -> {replayed[field]!r}"
        )
    assert "inference_ms" not in replayed, "a replayed push must not re-run inference"
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
    # Built by the adapter rather than by hand: a real KV state is 35
    # tensors, not a dataclass anyone can construct in a test. That is the
    # point — the blob under test is the one production would produce.
    st = server.adapter.create_state("orig")
    blob = server.adapter.serialize(st)

    r = client.post("/v1/stream/restore", json={"checkpoint_blob": base64.b64encode(blob).decode()})
    assert r.status_code == 200
    body = r.json()
    assert body["generation"] == 0
    assert body["handle"]


def test_restore_carries_last_seq_applied_from_the_checkpoint():
    st = server.adapter.create_state("orig")
    blob = server.adapter.serialize(st)

    r = client.post(
        "/v1/stream/restore",
        json={"checkpoint_blob": base64.b64encode(blob).decode(), "last_seq_applied": 1180},
    )
    assert r.status_code == 200
    assert r.json()["last_seq_applied"] == 1180


def test_restore_rejects_a_corrupted_blob():
    # docs/build-plan.md's checkpoint design: "Validation failure means
    # audio replay. Never partial-restore, never coerce." — a blob that
    # fails to deserialize must be a clean, distinguishable error, not a
    # 500 or a silently-wrong partial state.
    r = client.post("/v1/stream/restore", json={"checkpoint_blob": base64.b64encode(b"not a real pickle").decode()})
    assert r.status_code == 422
    assert r.json()["error"] == "invalid_checkpoint"


def test_checkpoint_round_trips_through_push_and_restore():
    """The full M3 loop: open, push (real state), checkpoint (serialize
    it), restore that exact blob into a FRESH handle, and confirm the new
    handle's state reflects the checkpointed history — not a fresh/empty
    one."""
    handle = _open("checkpoint-loop")["handle"]
    audio = b"\x00\x01" * 320
    push = client.post(
        "/v1/stream/push",
        content=audio,
        headers={**AUDIO_HEADERS_CT, "X-Handle": handle, "X-Seq-Start": "0", "X-Seq-End": "10", "X-Expected-Generation": "0"},
    ).json()
    # No text assertion: 320 samples of a constant byte pattern is not
    # speech, and a real adapter correctly returns nothing for it. What
    # this test is about is the PROTOCOL — generation advanced, the seq
    # was applied — not transcription quality.
    assert "text" in push

    cp = client.post("/v1/stream/checkpoint", json={"handle": handle})
    assert cp.status_code == 200
    cp_body = cp.json()
    assert cp_body["generation"] == push["generation"]
    assert cp_body["last_seq_applied"] == 10

    restored = client.post(
        "/v1/stream/restore",
        json={"checkpoint_blob": cp_body["checkpoint_blob"], "last_seq_applied": cp_body["last_seq_applied"]},
    )
    assert restored.status_code == 200
    restored_body = restored.json()
    assert restored_body["last_seq_applied"] == 10
    assert restored_body["handle"] != handle  # a genuinely new handle, not the same session

    # The restored handle FLUSHES cleanly — that is what this test can
    # honestly claim now. Its text is not asserted because the input is a
    # constant byte pattern, not speech: a real adapter correctly
    # transcribes nothing from it, where the mock returned a synthetic
    # "mock1". Transcript identity ACROSS a restore is proven where it can
    # be proven honestly — on real corpus audio, in
    # tests/test_kvcache.py::test_restore_continues_identically.
    final = client.post("/v1/stream/flush", json={"handle": restored_body["handle"]})
    assert final.status_code == 200
    assert "text" in final.json()


def test_checkpoint_unknown_handle_returns_404():
    r = client.post("/v1/stream/checkpoint", json={"handle": "does-not-exist"})
    assert r.status_code == 404


def test_checkpoint_returns_501_when_adapter_is_not_serializable(monkeypatch):
    from adapters.base import Capabilities

    monkeypatch.setattr(
        server.adapter,
        "capabilities",
        lambda: Capabilities(
            streaming=True, serializable=False, endpointing=False,
            modes=frozenset({"online"}), min_chunk_ms=20, max_chunk_ms=5000,
        ),
    )
    handle = _open("s-not-serializable")["handle"]
    r = client.post("/v1/stream/checkpoint", json={"handle": handle})
    assert r.status_code == 501
    assert r.json()["error"] == "not_supported"


def test_admin_corrupt_flag_makes_checkpoint_unrestorable():
    """The worker-side half of scenario 5's fault: /admin/corrupt flips
    every byte of what /v1/stream/checkpoint returns, so a blob taken
    while it's on fails to deserialize on restore — the same
    invalid_checkpoint path a gateway-side corruption would hit."""
    handle = _open("s-corrupt")["handle"]
    client.post(
        "/v1/stream/push",
        content=b"\x00\x01" * 320,
        headers={**AUDIO_HEADERS_CT, "X-Handle": handle, "X-Seq-Start": "0", "X-Seq-End": "10", "X-Expected-Generation": "0"},
    )
    try:
        r = client.post("/admin/corrupt", json={"on": True})
        assert r.json()["corrupt"] is True

        cp = client.post("/v1/stream/checkpoint", json={"handle": handle}).json()
        restored = client.post("/v1/stream/restore", json={"checkpoint_blob": cp["checkpoint_blob"]})
        assert restored.status_code == 422
    finally:
        client.post("/admin/reset")  # never leak fault state into other tests


def test_admin_slow_and_429_and_reset():
    try:
        r = client.post("/admin/slow", json={"ms": 5})
        assert r.json()["slow_ms"] == 5

        r = client.post("/admin/429", json={"rate": 1.0})  # always trip, deterministic for the test
        assert r.json()["rate"] == 1.0

        handle = _open("s-fault")["handle"]
        pushed = client.post(
            "/v1/stream/push",
            content=b"\x00\x01" * 320,
            headers={**AUDIO_HEADERS_CT, "X-Handle": handle, "X-Seq-Start": "0", "X-Seq-End": "10", "X-Expected-Generation": "0"},
        )
        assert pushed.status_code == 429
        assert pushed.headers.get("retry-after") == "1"
    finally:
        r = client.post("/admin/reset")
        assert r.json() == {"reset": True}

    # After reset, push must succeed normally again.
    handle = _open("s-after-reset")["handle"]
    pushed = client.post(
        "/v1/stream/push",
        content=b"\x00\x01" * 320,
        headers={**AUDIO_HEADERS_CT, "X-Handle": handle, "X-Seq-Start": "0", "X-Seq-End": "10", "X-Expected-Generation": "0"},
    )
    assert pushed.status_code == 200


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
