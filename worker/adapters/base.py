"""The Adapter Protocol and Capabilities contract every model plugin
implements. Never crosses a network — see docs/implementation-plan.md,
"Interface contracts".

`audio` on infer() is the raw opaque payload the worker received (whatever
decoding the adapter itself needs is its own business — the black-box
boundary this project keeps extends to here too, not just the Go side's
internal/audio). No adapter here reaches back across the HTTP boundary or
knows about sessions, handles, or generations; that is worker/state.py's
job.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, FrozenSet, Protocol


class NotSupported(Exception):
    """Raised by serialize/deserialize when Capabilities.serializable is
    False. Every real adapter (M5) raises this honestly rather than
    faking a checkpoint — see docs/DECISIONS.md: the warm-checkpoint tier
    is real only on the mock adapter."""


@dataclass(frozen=True)
class CompatKey:
    """The full composition of what makes two backends cache-compatible.
    Lives ONLY here, worker-side — the Go gateway only ever sees the
    resulting hash as an opaque token (internal/session.CacheCompatibilityKey),
    never these fields. That split is what keeps build-plan.md's own rule
    2 ("coordinator never knows a backend is a model") actually true.
    """

    model_family: str
    model_id: str
    model_revision: str
    runtime: str
    runtime_version: str
    cache_schema_version: str
    dtype: str

    def hash(self) -> str:
        raw = "|".join(
            [
                self.model_family,
                self.model_id,
                self.model_revision,
                self.runtime,
                self.runtime_version,
                self.cache_schema_version,
                self.dtype,
            ]
        )
        return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()


@dataclass(frozen=True)
class Capabilities:
    """What the router (M3+) filters candidates on before it ever scores
    one — implementation-plan.md "Capability-aware routing". A backend
    that is healthy but the wrong shape for the traffic (e.g. non-streaming
    for an online session) must be filtered out here, not discovered by
    calling it."""

    streaming: bool
    serializable: bool
    endpointing: bool
    modes: FrozenSet[str]  # subset of {"online", "offline"}
    min_chunk_ms: int
    max_chunk_ms: int

    def to_json(self) -> dict:
        return {
            "streaming": self.streaming,
            "serializable": self.serializable,
            "endpointing": self.endpointing,
            "modes": sorted(self.modes),
            "min_chunk_ms": self.min_chunk_ms,
            "max_chunk_ms": self.max_chunk_ms,
        }


@dataclass
class Delta:
    text: str


class Adapter(Protocol):
    def capabilities(self) -> Capabilities: ...
    def compatibility_key(self) -> CompatKey: ...
    def create_state(self, session_id: str) -> Any: ...
    def infer(self, audio: bytes, st: Any) -> tuple[Delta, Any]: ...
    def finalize(self, st: Any) -> Delta: ...
    def serialize(self, st: Any) -> bytes: ...  # raises NotSupported
    def deserialize(self, blob: bytes) -> Any: ...  # raises NotSupported
