"""Name -> Adapter constructor, selected by the ADAPTER env var. Adding a
model is one entry here and one file — see docs/implementation-plan.md,
"Adapter registry and conformance". M5 adds zipformer, zipformer_ctc,
whisper_ct2, whisper_onnx; each must pass
worker/tests/test_adapter_conformance.py before it's wired in here.
"""

from __future__ import annotations

from .base import Adapter
from .mock import MockAdapter

_REGISTRY: dict[str, type] = {
    "mock": MockAdapter,
}


def build(name: str, *, model_id: str | None = None) -> Adapter:
    """model_id is forwarded to every adapter's constructor — see
    mock.MockAdapter for why. A real adapter (M5) is free to ignore it
    (its identity comes from which weights it loaded), but should still
    accept the kwarg for this call site to stay uniform across the
    registry."""
    try:
        cls = _REGISTRY[name]
    except KeyError:
        raise ValueError(f"unknown ADAPTER {name!r}; available: {sorted(_REGISTRY)}") from None
    return cls(model_id=model_id)
