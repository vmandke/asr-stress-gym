"""Adapter registry and implementations.

worker/adapters/base.py defines the Adapter Protocol and Capabilities
dataclass (docs/implementation-plan.md, "Interface contracts"). Filled in
at M1 (base + mock). registry.py and the four real adapters
(zipformer, zipformer_ctc, whisper_ct2, whisper_onnx) land at M5, built
against tests/test_adapter_conformance.py.
"""
