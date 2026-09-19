"""worker-a / worker-b: streaming zipformer transducer, sherpa-onnx, int8.

The fleet's online primary and its same-key failover target
(docs/implementation-plan.md, "The fleet"). Two workers running this
adapter advertise an identical compatibility key, which is what makes
chaos scenario 2's same-model failover a same-model failover — and, since
this adapter is not serializable, what makes that path visibly degrade to
tail replay instead of a checkpoint restore. That degradation IS the
finding, not a gap: see docs/DECISIONS.md, "Checkpoint tier".

A transducer holds encoder state plus predictor/joiner state. Nothing
above worker/ ever learns that; the gateway sees only the key's hash.
"""

from __future__ import annotations

from typing import Any

from . import weights
from .base import CompatKey
from .sherpa_online import StreamingSherpaAdapter

SLUG = "zipformer-en-20M"


class ZipformerAdapter(StreamingSherpaAdapter):
    def _build_recognizer(self) -> Any:
        import sherpa_onnx

        self._runtime_version = sherpa_onnx.__version__
        return sherpa_onnx.OnlineRecognizer.from_transducer(
            tokens=weights.file(SLUG, "tokens.txt"),
            encoder=weights.file(SLUG, "encoder-epoch-99-avg-1.int8.onnx"),
            decoder=weights.file(SLUG, "decoder-epoch-99-avg-1.onnx"),
            joiner=weights.file(SLUG, "joiner-epoch-99-avg-1.int8.onnx"),
            # One thread per worker process: the fleet is CPU-pinned in
            # docker-compose.yml (1.5 vCPU each), and letting onnxruntime
            # spawn its own pool underneath that limit makes concurrency
            # numbers unreproducible — the exact thing the pinned limits
            # exist to prevent (docs/build-plan.md, "Reproducible capacity
            # numbers").
            num_threads=1,
            provider="cpu",
        )

    def _key(self) -> CompatKey:
        return CompatKey(
            model_family="transducer",
            model_id="zipformer-en-20M",
            model_revision="icefall-2023-02-17",
            runtime="sherpa-onnx",
            runtime_version=self._runtime_version,
            cache_schema_version="1",
            dtype="int8",
        )
