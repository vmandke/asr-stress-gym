"""Quantize the KV cache for transfer.

Three supported widths, and the trade is not the obvious one. The memory
saving is real but modest in absolute terms — 1.09 MB to 273 KB — and the
number that actually matters here is **transfer cost on failover**, because
this blob crosses the network to a different worker while a session waits.

    fp32   1.09 MB   exact
    fp16    545 KB   ~7 significant bits of mantissa
    int8    273 KB   per-tensor symmetric scale

Two things this deliberately does NOT do:

**Integer state is never quantized.** `cached_len_*` are int64 counters,
not activations; scaling them would corrupt the encoder's bookkeeping
rather than merely blur it. They pass through untouched.

**A quantized blob cannot be restored into a worker expecting another
width.** The dtype is part of the header and checked on import, and
`cache_schema_version` covers the policy — so a fleet running mixed widths
correctly refuses the restore and degrades to audio replay instead of
silently importing rescaled garbage.
"""

from __future__ import annotations

import numpy as np

SUPPORTED = ("fp32", "fp16", "int8")


class UnsupportedDtype(Exception):
    pass


def _is_float_state(name: str, arr: np.ndarray) -> bool:
    return arr.dtype == np.float32


def quantize_bank(
    tensors: dict[str, np.ndarray], dtype: str
) -> tuple[dict[str, np.ndarray], dict[str, float]]:
    """Returns (encoded tensors, per-tensor scales). Scales are empty for
    everything but int8."""
    if dtype not in SUPPORTED:
        raise UnsupportedDtype(f"{dtype!r}; supported: {SUPPORTED}")
    if dtype == "fp32":
        return tensors, {}

    out: dict[str, np.ndarray] = {}
    scales: dict[str, float] = {}
    for name, arr in tensors.items():
        if not _is_float_state(name, arr):
            out[name] = arr  # int64 bookkeeping — see module docstring
            continue
        if dtype == "fp16":
            out[name] = arr.astype(np.float16)
        else:
            # Symmetric per-tensor scale. Per-tensor rather than per-channel
            # because these tensors are small and the header would otherwise
            # carry more scale metadata than the saving is worth.
            peak = float(np.abs(arr).max())
            scale = peak / 127.0 if peak > 0 else 1.0
            out[name] = np.clip(np.rint(arr / scale), -127, 127).astype(np.int8)
            scales[name] = scale
    return out, scales


def dequantize_bank(
    tensors: dict[str, np.ndarray], dtype: str, scales: dict[str, float]
) -> dict[str, np.ndarray]:
    """Restore float32 tensors. The encoder's inputs are float32, so this
    always returns float32 regardless of the transport width."""
    if dtype not in SUPPORTED:
        raise UnsupportedDtype(f"{dtype!r}; supported: {SUPPORTED}")
    if dtype == "fp32":
        return tensors

    out: dict[str, np.ndarray] = {}
    for name, arr in tensors.items():
        if arr.dtype == np.int64:
            out[name] = arr
        elif dtype == "fp16":
            out[name] = arr.astype(np.float32)
        else:
            out[name] = arr.astype(np.float32) * scales.get(name, 1.0)
    return out
