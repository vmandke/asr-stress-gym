"""worker-c: streaming fast-conformer CTC, sherpa-onnx, int8.

The fleet's cross-family failover target
(docs/implementation-plan.md, "The fleet"). Its job is to be a healthy,
streaming-capable backend whose inference state is *structurally
meaningless* to the transducer in zipformer.py: CTC carries encoder
hidden state and no predictor or attention cache at all. Failing a session
from worker-a to worker-c therefore cannot restore anything, must build
fresh state and replay the audio, and must emit partial.reset — which is
exactly what chaos scenario 3 asserts.

**Deviation from docs/implementation-plan.md's fleet table**, recorded
here and in docs/DECISIONS.md: the plan names this worker
`zipformer-ctc-en`. No English streaming zipformer-CTC export is published
— the streaming zipformer-CTC models upstream are Chinese
(`sherpa-onnx-streaming-zipformer-ctc-multi-zh-hans-*`), and pointing an
English corpus at a Chinese model would make every transcript assertion in
the test suite meaningless. NVIDIA's fast-conformer CTC, exported for
sherpa-onnx in English, fills the identical architectural role: streaming,
CTC, a different family from the transducer, a different compatibility
key. The plan's requirement was a second state shape, not that particular
checkpoint.
"""

from __future__ import annotations

from typing import Any

from . import weights
from .base import CompatKey
from .sherpa_online import StreamingSherpaAdapter

SLUG = "conformer-ctc-en"


class ConformerCtcAdapter(StreamingSherpaAdapter):
    def _build_recognizer(self) -> Any:
        import sherpa_onnx

        self._runtime_version = sherpa_onnx.__version__
        return sherpa_onnx.OnlineRecognizer.from_nemo_ctc(
            tokens=weights.file(SLUG, "tokens.txt"),
            model=weights.file(SLUG, "model.int8.onnx"),
            num_threads=1,  # see zipformer.py for why this is pinned
            provider="cpu",
        )

    def _key(self) -> CompatKey:
        return CompatKey(
            model_family="ctc",
            model_id="nemo-fast-conformer-ctc-en-480ms",
            model_revision="nemo-stt_en_fastconformer_hybrid_large_streaming_multi",
            runtime="sherpa-onnx",
            runtime_version=self._runtime_version,
            cache_schema_version="1",
            dtype="int8",
        )
