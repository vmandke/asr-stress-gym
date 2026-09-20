"""Serialize a StateBank to bytes, and back, safely.

**safetensors, never pickle.** These blobs leave this process: the adapter
hands them to the worker, the worker to the gateway, the gateway to a
*different* worker on failover. `pickle.loads` and `torch.load` execute
arbitrary code by design, so using either here would turn "restore a
checkpoint" into a remote code execution primitive reachable from anything
that can write to the checkpoint store. safetensors is a length-prefixed
header plus raw tensor bytes: it cannot execute anything.

The header travels with the tensors and is validated field by field before
a single tensor is trusted. build-plan.md's rule for this tier is
"validation failure means audio replay. Never partial-restore, never
coerce" — so every mismatch raises and the caller degrades to replay,
which is always available because the gateway holds the audio journal.
"""

from __future__ import annotations

import json

import numpy as np
from safetensors.numpy import load as st_load
from safetensors.numpy import save as st_save

from .quant import dequantize_bank, quantize_bank
from .state import CACHE_SCHEMA_VERSION, LayoutError, StateBank, StateLayout

# safetensors stores str->str metadata alongside the tensors, which is
# exactly the right place for the header: it cannot be separated from the
# payload it describes.
_HEADER_KEY = "kv_header"

# The feature seam rides along as a tensor rather than in the header, so it
# gets the same zero-copy treatment as everything else. See state.py for
# why dropping it corrupts the transcript across a restore.
_PENDING_KEY = "__pending__"

# The transducer hypothesis. Small (a few dozen int64s) but load-bearing:
# without it a restore produces a transcript missing everything decoded
# before the failover. See StateBank.
_HYP_KEY = "__hypothesis__"


def serialize(
    bank: StateBank,
    layout: StateLayout,
    compat_key_hash: str,
    *,
    dtype: str = "fp32",
) -> bytes:
    """Encode a session's KV cache. `dtype` selects quantization."""
    tensors, scales = quantize_bank(bank.tensors, dtype)
    tensors = dict(tensors)
    tensors[_PENDING_KEY] = bank.pending
    tensors[_HYP_KEY] = np.asarray(bank.hypothesis, dtype=np.int64)

    header = {
        "schema": CACHE_SCHEMA_VERSION,
        "compat_key": compat_key_hash,
        "spec": layout.spec_digest,
        "dtype": dtype,
        "scales": scales,
        "frames_consumed": bank.frames_consumed,
        "pending_frames": int(bank.pending.shape[0]),
    }
    return st_save(tensors, metadata={_HEADER_KEY: json.dumps(header)})


def deserialize(
    blob: bytes,
    layout: StateLayout,
    compat_key_hash: str,
) -> StateBank:
    """Decode and validate. Raises LayoutError on any mismatch."""
    try:
        tensors = dict(st_load(blob))
    except Exception as e:  # malformed, truncated, or not safetensors at all
        raise LayoutError(f"unreadable checkpoint blob: {e}") from None

    header = _read_header(blob)

    # Cheapest, most decisive checks first: a blob from a different model or
    # a different cache layout is rejected before any tensor is examined.
    if header.get("schema") != CACHE_SCHEMA_VERSION:
        raise LayoutError(
            f"cache schema {header.get('schema')!r}, this worker speaks {CACHE_SCHEMA_VERSION!r}"
        )
    if header.get("compat_key") != compat_key_hash:
        raise LayoutError("checkpoint was taken against a different compatibility key")
    if header.get("spec") != layout.spec_digest:
        raise LayoutError("checkpoint tensor geometry does not match this model")

    pending = tensors.pop(_PENDING_KEY, np.zeros((0, 80), np.float32))
    hypothesis = tensors.pop(_HYP_KEY, np.zeros((0,), np.int64))
    tensors = dequantize_bank(tensors, header.get("dtype", "fp32"), header.get("scales", {}))

    # Only now, with provenance established, check the tensors themselves.
    layout.validate(tensors)

    return StateBank(
        tensors=tensors,
        pending=np.asarray(pending, dtype=np.float32),
        frames_consumed=int(header.get("frames_consumed", 0)),
        hypothesis=[int(t) for t in hypothesis],
    )


def _read_header(blob: bytes) -> dict:
    """safetensors' metadata lives in its JSON header; read it directly
    rather than depending on a loader API that has churned."""
    try:
        n = int.from_bytes(blob[:8], "little")
        raw = json.loads(blob[8 : 8 + n].decode())
        return json.loads(raw.get("__metadata__", {}).get(_HEADER_KEY, "{}"))
    except Exception as e:
        raise LayoutError(f"checkpoint header unreadable: {e}") from None
