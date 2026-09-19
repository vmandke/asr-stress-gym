"""worker-e: whisper-tiny.en under ONNX Runtime (via sherpa-onnx), int8.

The other half of the same-weights/different-runtime pair — see
whisper_ct2.py for the full argument. The two keys differ in `runtime` and
`runtime_version` and in nothing else.

The pair is worth more than a table row: the two runtimes genuinely
disagree on identical weights (sherpa-onnx issue #2900), so failing a
session from d to e visibly changes the transcript. Since transcription
quality is explicitly out of grading scope, that is an asset — it makes
`partial.reset` mean something a reviewer can see, rather than an event
they have to take on faith.

worker-e itself is opt-in (`--profile models`) per
docs/implementation-plan.md; this adapter is registered unconditionally so
the conformance suite covers it either way.
"""

from __future__ import annotations

import numpy as np

from . import pcm, weights
from .base import CompatKey
from .buffered import BufferedAdapter

SLUG = "whisper-tiny.en-onnx"


class WhisperOnnxAdapter(BufferedAdapter):
    def _load(self) -> None:
        import sherpa_onnx

        self._runtime_version = f"onnxruntime-via-sherpa-onnx-{sherpa_onnx.__version__}"
        self._recognizer = sherpa_onnx.OfflineRecognizer.from_whisper(
            encoder=weights.file(SLUG, "tiny.en-encoder.int8.onnx"),
            decoder=weights.file(SLUG, "tiny.en-decoder.int8.onnx"),
            tokens=weights.file(SLUG, "tiny.en-tokens.txt"),
            language="en",
            task="transcribe",
            num_threads=1,  # see zipformer.py
            provider="cpu",
        )

    def _transcribe(self, samples: np.ndarray) -> str:
        # BufferedAdapter.finalize's MIN_UTTERANCE_S guard is what keeps a
        # zero-length buffer from reaching this call — it SIGSEGVs the
        # worker process rather than raising. Do not remove it.
        stream = self._recognizer.create_stream()
        stream.accept_waveform(pcm.SAMPLE_RATE_HZ, samples)
        self._recognizer.decode_stream(stream)
        return stream.result.text.strip()

    def _key(self) -> CompatKey:
        return CompatKey(
            model_family="encdec",
            model_id="whisper-tiny.en",
            model_revision="openai/whisper-tiny.en",
            runtime="onnxruntime",
            runtime_version=self._runtime_version,
            cache_schema_version="1",
            dtype="int8",
        )
