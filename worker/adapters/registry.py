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

# name -> (module, class). Every adapter here owns a real, serializable
# KV cache (worker/kvcache). The sherpa-onnx-wrapped adapters and the mock
# were removed once every deployed worker had one: they could not
# serialize state, so they only ever exercised the DEGRADATION path, and
# keeping them in the registry implied a choice the fleet no longer
# offers. They remain in git history if a comparison ever needs them.
_REGISTRY: dict[str, tuple[str, str]] = {
    # streaming transducer — 35 state tensors incl. cached_key/cached_val
    "zipformer_kv": ("adapters.zipformer_kv", "ZipformerKVAdapter"),
    # streaming CTC — NeMo cache_last_channel / cache_last_time
    "conformer_ctc_kv": ("adapters.conformer_ctc_kv", "ConformerCtcKVAdapter"),
    # offline encoder-decoder — a textbook self-attention KV cache
    "whisper_kv": ("adapters.whisper_kv", "WhisperKVAdapter"),
}

# The adapters that load baked-in weights (models/fetch.sh). The
# conformance suite skips these when no weights root exists, rather than
# reporting a fresh clone as a broken fleet.
NEEDS_WEIGHTS = frozenset({"zipformer_kv", "conformer_ctc_kv", "whisper_kv"})


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
