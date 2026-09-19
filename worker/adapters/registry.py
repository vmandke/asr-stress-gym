"""Name -> Adapter constructor, selected by the ADAPTER env var. Adding a
model is one entry here and one file — see docs/implementation-plan.md,
"Adapter registry and conformance". Every entry must pass
worker/tests/test_adapter_conformance.py, which parametrizes over this
dict, so a new name is covered the moment it appears here.

Imports are LAZY (a module path, resolved on first build) rather than
top-level. The real adapters pull in sherpa-onnx, ctranslate2 and their
weights; importing all four eagerly would make every worker pay ~300MB of
model load for the one adapter it actually hosts, and would make
`import registry` fail outright on a checkout where models/fetch.sh has
never run — including in the mock-only tests that have no business
needing weights.
"""

from __future__ import annotations

import importlib

from .base import Adapter

# name -> (module, class). The mock is listed first because it is the
# fleet's only serializable adapter and therefore the only place the warm-
# checkpoint tier is real (docs/DECISIONS.md, "Checkpoint tier").
_REGISTRY: dict[str, tuple[str, str]] = {
    "mock": ("adapters.mock", "MockAdapter"),
    "zipformer": ("adapters.zipformer", "ZipformerAdapter"),
    "conformer_ctc": ("adapters.conformer_ctc", "ConformerCtcAdapter"),
    "whisper_ct2": ("adapters.whisper_ct2", "WhisperCT2Adapter"),
    "whisper_onnx": ("adapters.whisper_onnx", "WhisperOnnxAdapter"),
}

# The adapters that load baked-in weights (models/fetch.sh). The
# conformance suite skips these when no weights root exists, rather than
# reporting a fresh clone as a broken fleet.
NEEDS_WEIGHTS = frozenset({"zipformer", "conformer_ctc", "whisper_ct2", "whisper_onnx"})


def build(name: str, *, model_id: str | None = None) -> Adapter:
    """model_id is forwarded to every adapter's constructor — see
    mock.MockAdapter for why. The real adapters accept and ignore it: their
    identity comes from the weights they loaded, not from an env var that
    could disagree with them."""
    try:
        module_name, class_name = _REGISTRY[name]
    except KeyError:
        raise ValueError(f"unknown ADAPTER {name!r}; available: {sorted(_REGISTRY)}") from None
    cls = getattr(importlib.import_module(module_name), class_name)
    return cls(model_id=model_id)
