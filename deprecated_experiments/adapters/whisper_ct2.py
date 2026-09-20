"""worker-d: whisper-tiny.en under CTranslate2 (faster-whisper), int8.

Half of the fleet's subtlest pair. worker-d and worker-e load the *same
weights* — openai/whisper-tiny.en — and are still cache-incompatible,
because their runtimes serialize inference state differently. That is the
case docs/build-plan.md calls out as the one people get wrong, and
compatibility_key() below differs from whisper_onnx.py's in exactly two
fields: runtime and runtime_version. Everything else is identical, on
purpose, so the reason for the mismatch is unambiguous when
`make matrix` prints it.

It is also the fleet's only non-streaming member in the default compose
profile, which makes it the a→d case: a healthy backend the router must
refuse for an online session rather than mis-serve (M6).
"""

from __future__ import annotations

import numpy as np

from . import weights
from .base import CompatKey
from .buffered import BufferedAdapter

SLUG = "whisper-tiny.en-ct2"


class WhisperCT2Adapter(BufferedAdapter):
    def _load(self) -> None:
        import ctranslate2
        from faster_whisper import WhisperModel

        self._runtime_version = ctranslate2.__version__
        self._model = WhisperModel(
            str(weights.resolve(SLUG)),
            device="cpu",
            compute_type="int8",
            cpu_threads=1,  # see zipformer.py: pinned so capacity numbers reproduce
        )

    def _transcribe(self, samples: np.ndarray) -> str:
        # beam_size=1 (greedy): this fleet is graded on failover behaviour,
        # not transcription quality (docs/DECISIONS.md), and a beam search
        # would only make RTF worse for no measured benefit.
        segments, _ = self._model.transcribe(samples, language="en", beam_size=1)
        return "".join(s.text for s in segments).strip()

    def _key(self) -> CompatKey:
        return CompatKey(
            model_family="encdec",
            # Identical to whisper_onnx.py's: the same weights, stated as
            # the same weights. The mismatch below must come from the
            # runtime, not from an incidental naming difference.
            model_id="whisper-tiny.en",
            model_revision="openai/whisper-tiny.en",
            runtime="ctranslate2",
            runtime_version=self._runtime_version,
            cache_schema_version="1",
            dtype="int8",
        )
