"""MockAdapter-specific behavior not covered by the generic conformance
suite (test_adapter_conformance.py): model_id parametrization, which is
what lets one adapter simulate the fleet's multiple mock "deployments"
(docs/implementation-plan.md, "Model fleet and deployments") for M3's
same-model vs cross-model failover scenarios.
"""

from __future__ import annotations

from adapters.mock import MockAdapter


def test_same_model_id_produces_the_same_key():
    a = MockAdapter(model_id="shared")
    b = MockAdapter(model_id="shared")
    assert a.compatibility_key().hash() == b.compatibility_key().hash()


def test_different_model_id_produces_a_different_key():
    a = MockAdapter(model_id="worker-a-identity")
    c = MockAdapter(model_id="worker-c-identity")
    assert a.compatibility_key().hash() != c.compatibility_key().hash()


def test_default_model_id_is_stable():
    # No model_id given (server.py's ADAPTER=mock with no MODEL env set) —
    # must still produce a well-defined, repeatable key, not vary run to run.
    assert MockAdapter().compatibility_key().hash() == MockAdapter().compatibility_key().hash()
    assert MockAdapter(model_id=None).compatibility_key().hash() == MockAdapter().compatibility_key().hash()
