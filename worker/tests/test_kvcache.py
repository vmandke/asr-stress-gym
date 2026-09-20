"""Tests for the KV cache (worker/kvcache) and the adapter that owns one.

The two that carry the weight are `test_restore_continues_identically` and
`test_restore_into_a_second_adapter_is_identical`: if either ever fails, a
failover silently changes the transcript, which is precisely the failure
this whole repository exists to prevent.

The rejection tests matter almost as much, in the other direction — a
checkpoint that is accepted when it should not be is worse than one that is
refused, because a refusal degrades to audio replay and an acceptance
corrupts the output.
"""

from __future__ import annotations

import wave
from pathlib import Path

import numpy as np
import pytest

from adapters import weights
from adapters.registry import build
from kvcache import LayoutError, StateBank, deserialize, serialize
from kvcache.quant import dequantize_bank, quantize_bank

pytestmark = pytest.mark.skipif(
    not weights.available(), reason="no model weights on disk — run models/fetch.sh"
)

CORPUS = Path(__file__).resolve().parents[2] / "corpus"


@pytest.fixture(scope="module")
def adapter():
    return build("zipformer_kv")


@pytest.fixture(scope="module")
def speech() -> bytes:
    clips = sorted(CORPUS.glob("*.wav"))
    if not clips:
        pytest.skip(f"no clips in {CORPUS}")
    # A medium clip: long enough to span several encoder windows, so a
    # mid-utterance checkpoint has real state behind it.
    clip = next((c for c in clips if "medium" in c.name), clips[-1])
    with wave.open(str(clip), "rb") as w:
        return w.readframes(w.getnframes())


def _halves(pcm: bytes) -> tuple[bytes, bytes]:
    mid = (len(pcm) // 2) & ~1  # keep s16le sample alignment
    return pcm[:mid], pcm[mid:]


# --- the load-bearing tests ------------------------------------------------


def test_restore_continues_identically(adapter, speech):
    """Snapshot mid-utterance, restore, finish — same transcript.

    This is a failover, in miniature.
    """
    first, second = _halves(speech)

    st = adapter.create_state("uninterrupted")
    _, st = adapter.infer(first, st)
    _, st = adapter.infer(second, st)
    expected = adapter.finalize(st).text

    st = adapter.create_state("killed")
    _, st = adapter.infer(first, st)
    blob = adapter.serialize(st)

    restored = adapter.deserialize(blob)
    _, restored = adapter.infer(second, restored)
    got = adapter.finalize(restored).text

    assert got == expected, f"restore changed the transcript:\n  {expected!r}\n  {got!r}"


def test_restore_into_a_second_adapter_is_identical(speech):
    """Cross-worker transfer: serialize on one adapter instance, restore on
    another with its own ONNX sessions. This is what a failover to a
    different container does."""
    a, b = build("zipformer_kv"), build("zipformer_kv")
    first, second = _halves(speech)

    st = a.create_state("s")
    _, st = a.infer(first, st)
    _, st = a.infer(second, st)
    expected = a.finalize(st).text

    st = a.create_state("s")
    _, st = a.infer(first, st)
    blob = a.serialize(st)

    st2 = b.deserialize(blob)  # <- different adapter instance
    _, st2 = b.infer(second, st2)
    assert b.finalize(st2).text == expected


def test_state_bytes_is_real(adapter, speech):
    """`state_bytes` was an honest 0 for every real adapter because nothing
    could size an onnxruntime-owned object. It is a real number now."""
    st = adapter.create_state("s")
    _, st = adapter.infer(speech, st)
    assert st.bank.nbytes > 1_000_000, "expected ~1.09 MB of encoder state"
    # The KV tensors proper are a strict subset: conflating them with total
    # session state would overstate the cache.
    assert 0 < st.bank.kv_nbytes < st.bank.nbytes


def test_serialized_blob_is_not_a_pickle(adapter):
    """safetensors, never pickle: these blobs cross a process boundary via
    the gateway, so deserialization must not be a code-execution
    primitive."""
    blob = adapter.serialize(adapter.create_state("s"))
    assert not blob.startswith(b"\x80"), "looks like a pickle protocol marker"
    # safetensors: 8-byte little-endian header length, then JSON.
    header_len = int.from_bytes(blob[:8], "little")
    assert 0 < header_len < len(blob)
    assert blob[8:9] == b"{"


# --- refusal, which must be as reliable as acceptance ----------------------


def test_truncated_blob_is_refused(adapter):
    blob = adapter.serialize(adapter.create_state("s"))
    with pytest.raises(ValueError):
        adapter.deserialize(blob[: len(blob) // 2])


def test_blob_from_a_different_compatibility_key_is_refused(adapter, speech):
    """build-plan.md: 'validation failure means audio replay. Never
    partial-restore, never coerce.'"""
    st = adapter.create_state("s")
    _, st = adapter.infer(speech, st)
    blob = serialize(st.bank, adapter._layout, "sha256:some-other-model")
    with pytest.raises(LayoutError):
        deserialize(blob, adapter._layout, adapter.compatibility_key().hash())


def test_blob_with_a_wrong_tensor_shape_is_refused(adapter):
    st = adapter.create_state("s")
    key = next(k for k in st.bank.tensors if k.startswith("cached_key"))
    st.bank.tensors[key] = np.zeros((1, 1, 1, 1), dtype=np.float32)
    blob = serialize(st.bank, adapter._layout, adapter.compatibility_key().hash())
    with pytest.raises(LayoutError):
        deserialize(blob, adapter._layout, adapter.compatibility_key().hash())


def test_garbage_is_refused_not_crashed(adapter):
    with pytest.raises(ValueError):
        adapter.deserialize(b"not a checkpoint at all")


# --- quantization ----------------------------------------------------------


@pytest.mark.parametrize("dtype", ["fp32", "fp16", "int8"])
def test_quantization_round_trips_within_tolerance(adapter, dtype):
    st = adapter.create_state("s")
    bank = st.bank
    # Populate with something non-zero, or every width round-trips trivially.
    for k, v in bank.tensors.items():
        if v.dtype == np.float32:
            bank.tensors[k] = np.random.default_rng(0).normal(0, 0.5, v.shape).astype(np.float32)

    encoded, scales = quantize_bank(bank.tensors, dtype)
    decoded = dequantize_bank(encoded, dtype, scales)

    tol = {"fp32": 0.0, "fp16": 1e-2, "int8": 5e-2}[dtype]
    for k, original in bank.tensors.items():
        if original.dtype != np.float32:
            assert np.array_equal(decoded[k], original), f"{k}: integer state was altered"
        else:
            assert np.abs(decoded[k] - original).max() <= tol + 1e-6, k


def test_integer_state_is_never_quantized(adapter):
    """`cached_len_*` are int64 counters, not activations. Scaling them
    would corrupt the encoder's bookkeeping rather than blur it."""
    bank = adapter.create_state("s").bank
    encoded, _ = quantize_bank(bank.tensors, "int8")
    for name, arr in bank.tensors.items():
        if arr.dtype == np.int64:
            assert encoded[name].dtype == np.int64, f"{name} was quantized"


def test_quantized_blob_is_smaller(adapter, speech):
    st = adapter.create_state("s")
    _, st = adapter.infer(speech, st)
    key = adapter.compatibility_key().hash()
    sizes = {
        d: len(serialize(st.bank, adapter._layout, key, dtype=d))
        for d in ("fp32", "fp16", "int8")
    }
    assert sizes["fp16"] < sizes["fp32"]
    assert sizes["int8"] < sizes["fp16"]


# --- the seam, which is the subtle part ------------------------------------


def test_pending_frames_survive_serialization(adapter, speech):
    """The KV tensors are not the whole state: up to `window - 1` feature
    frames are buffered and unconsumed. Dropping them loses ~380ms of audio
    and changed the transcript across a restore — 2/10 clips matched before
    this was carried, 22/22 after (docs/STATUS.md, M11)."""
    st = adapter.create_state("s")
    _, st = adapter.infer(speech[: len(speech) // 3], st)
    blob = adapter.serialize(st)
    restored = adapter.deserialize(blob)
    assert restored.bank.pending.shape == st.bank.pending.shape
    assert np.array_equal(restored.bank.pending, st.bank.pending)


def test_state_survives_a_second_utterance(adapter, speech):
    """A session outlives an utterance. M5 found the sherpa adapters made
    every second utterance fail because `input_finished()` is terminal;
    this loop has no terminal call, and the guard belongs here too."""
    st = adapter.create_state("s")
    _, st = adapter.infer(speech, st)
    first = adapter.finalize(st).text
    _, st = adapter.infer(speech, st)
    second = adapter.finalize(st).text
    assert first, "first utterance produced nothing"
    assert second, "second utterance produced nothing — state was left terminal"
