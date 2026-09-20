"""The KV cache the rest of this repository has always described.

See state.py for what is actually cached and why sherpa-onnx could not
give it to us, serde.py for how it crosses a process boundary safely, and
quant.py for the transfer-width trade.

`docs/KVCACHE-PLAN.md` records the survey of alternatives, and
`docs/KVCACHE-DEEPDIVE.md` §10 the scorecard this package exists to change.
"""

from .quant import SUPPORTED as SUPPORTED_DTYPES
from .quant import UnsupportedDtype
from .serde import deserialize, serialize
from .state import CACHE_SCHEMA_VERSION, LayoutError, StateBank, StateLayout

__all__ = [
    "CACHE_SCHEMA_VERSION",
    "LayoutError",
    "StateBank",
    "StateLayout",
    "SUPPORTED_DTYPES",
    "UnsupportedDtype",
    "deserialize",
    "serialize",
]
