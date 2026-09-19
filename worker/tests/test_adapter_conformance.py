"""Every adapter in the registry must pass this suite — written before the
real adapters land (M5), per docs/implementation-plan.md "Adapter registry
and conformance". Parametrized over the registry itself, so a new entry in
adapters/registry.py is covered automatically with no edit here.

Idempotent replay (pushing the same seq range twice is a no-op) is
deliberately NOT tested here: that property is implemented in
server.py's last_seq_applied check, not inside any individual adapter —
see test_server.py::test_push_idempotent_replay.
"""

from __future__ import annotations

import pytest

from adapters.base import NotSupported
from adapters.registry import _REGISTRY, build

ADAPTER_NAMES = sorted(_REGISTRY.keys())


@pytest.mark.parametrize("name", ADAPTER_NAMES)
class TestAdapterConformance:
    def test_compatibility_key_is_stable_across_calls(self, name):
        a = build(name)
        k1, k2 = a.compatibility_key(), a.compatibility_key()
        assert k1 == k2
        assert k1.hash() == k2.hash()

    def test_state_is_isolated_between_sessions(self, name):
        a = build(name)
        s1 = a.create_state("session-1")
        s2 = a.create_state("session-2")
        assert s1 is not s2

        silence = b"\x00\x00" * 320
        _, s1_after = a.infer(silence, s1)
        d1 = a.finalize(s1_after)
        d2 = a.finalize(s2)  # must reflect s2's own (empty) history, not s1's
        assert d1.text != d2.text, "mutating one session's state affected another's"

    def test_finalize_after_infer(self, name):
        a = build(name)
        st = a.create_state("s1")
        silence = b"\x00\x00" * 320
        _, st = a.infer(silence, st)
        delta = a.finalize(st)
        assert isinstance(delta.text, str)

    def test_serializable_flag_is_honest(self, name):
        """Capabilities.serializable must match what serialize/deserialize
        actually do — a real adapter (M5) that declares False must raise
        NotSupported, never silently no-op or fake a checkpoint."""
        a = build(name)
        caps = a.capabilities()
        st = a.create_state("s1")

        if caps.serializable:
            blob = a.serialize(st)
            assert isinstance(blob, (bytes, bytearray))
            assert a.deserialize(blob) is not None
        else:
            with pytest.raises(NotSupported):
                a.serialize(st)
            with pytest.raises(NotSupported):
                a.deserialize(b"")

    def test_capabilities_are_well_formed(self, name):
        a = build(name)
        caps = a.capabilities()
        assert caps.modes, "an adapter must declare at least one supported mode"
        assert caps.modes <= {"online", "offline"}
        assert caps.min_chunk_ms <= caps.max_chunk_ms
        if not caps.streaming:
            assert "online" not in caps.modes, (
                "a non-streaming adapter cannot serve online partials — "
                "the router (M6) filters on exactly this"
            )
