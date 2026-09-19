"""Deterministic, dependency-free adapter. Sleeps on the RTF datum from
build-plan.md's interview notes (~1-2s audio processed in ~50ms, RTF ~
0.025-0.05) rather than modeling anything real — transcription quality is
explicitly not graded. This is the only adapter with serializable=True:
the sole place the checkpoint tier is real (docs/DECISIONS.md); every
real adapter (M5) reports False and exercises the degradation path
instead.
"""

from __future__ import annotations

import pickle
import time
from dataclasses import dataclass, field

from .base import Adapter, Capabilities, CompatKey, Delta

_RTF = 0.03
_BYTES_PER_SAMPLE = 2  # s16le, mono — docs/DECISIONS.md
_SAMPLE_RATE_HZ = 16000


@dataclass
class MockState:
    session_id: str
    chunks_seen: int = 0
    tokens: list[str] = field(default_factory=list)


class MockAdapter:
    """model_id lets one adapter simulate several distinct "models" for
    the fleet's compatibility-key story (docs/implementation-plan.md,
    "Model fleet and deployments") without writing separate classes for
    each — M3 needs at least two mock deployments that share a key and
    one that doesn't (worker-a/worker-b vs worker-c), and this is what
    makes that possible from one adapter. Driven by the worker's existing
    MODEL env var (server.py), not a new one: worker-a and worker-b are
    already configured with the same MODEL value, worker-c a different
    one, for the fleet identities compose already assigns.
    """

    def __init__(self, model_id: str | None = None) -> None:
        self._model_id = model_id or "mock-v1"

    def capabilities(self) -> Capabilities:
        return Capabilities(
            streaming=True,
            serializable=True,
            endpointing=False,
            modes=frozenset({"online", "offline"}),
            min_chunk_ms=20,
            max_chunk_ms=5000,
        )

    def compatibility_key(self) -> CompatKey:
        return CompatKey(
            model_family="mock",
            model_id=self._model_id,
            model_revision="v1",
            runtime="mock",
            runtime_version="1",
            cache_schema_version="1",
            dtype="n/a",
        )

    def create_state(self, session_id: str) -> MockState:
        return MockState(session_id=session_id)

    def infer(self, audio: bytes, st: MockState) -> tuple[Delta, MockState]:
        num_samples = len(audio) // _BYTES_PER_SAMPLE
        duration_s = num_samples / _SAMPLE_RATE_HZ
        time.sleep(duration_s * _RTF)  # simulate the RTF datum, not real inference

        st.chunks_seen += 1
        st.tokens.append(f"mock{st.chunks_seen}")
        return Delta(text=" ".join(st.tokens)), st

    def finalize(self, st: MockState) -> Delta:
        return Delta(text=" ".join(st.tokens))

    def serialize(self, st: MockState) -> bytes:
        return pickle.dumps(st)

    def deserialize(self, blob: bytes) -> MockState:
        return pickle.loads(blob)  # noqa: S301 — worker-internal blob, not external input
