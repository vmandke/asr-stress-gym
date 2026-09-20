"""Shared machinery for the two streaming sherpa-onnx adapters
(zipformer.py, conformer_ctc.py).

They differ in exactly two things — which recognizer factory builds them,
and what compatibility key they advertise — so everything else lives here
rather than being copy-pasted twice and drifting.

Two behaviours below were established empirically against the real models,
not assumed:

1. **Tail padding on finalize.** A streaming encoder has not "seen" the
   last few hundred milliseconds of audio until enough right-context
   follows it. Calling input_finished() with nothing after the last real
   sample truncates the final: "...near the river" for zipformer and
   "...lazy dog ne" for the CTC model on corpus/06_medium_sentence.wav.
   0.6s of silence recovers the full text on both; 0.3s recovers only part
   of it. sherpa-onnx's own examples pad the same way.

2. **finalize rolls the stream.** input_finished() is terminal for a
   sherpa OnlineStream — pushing to it afterwards is an error. But a
   session outlives an utterance (docs/PROTOCOL.md, M2's utterance
   lifecycle), so finalize hands back a fresh stream in place of the
   spent one. Without this the second utterance of any session would fail
   on a live model while passing every mock test.

Neither adapter is serializable: sherpa-onnx's Python bindings expose no
save/restore/clone on OnlineStream (verified against upstream during
planning — see docs/implementation-plan.md, "Findings that change the
design"). They raise NotSupported honestly, which is what makes them the
fleet's demonstration of the checkpoint-degradation path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from . import pcm
from .base import Capabilities, CompatKey, Delta, NotSupported

# See point 1 above. Measured, not guessed; reported rather than defended,
# same as the VAD thresholds (docs/implementation-plan.md, "The audio
# boundary").
TAIL_PADDING_S = 0.6


@dataclass
class OnlineState:
    session_id: str
    stream: Any  # sherpa_onnx.OnlineStream — opaque here on purpose
    text: str = ""


class StreamingSherpaAdapter:
    """Base class; subclasses provide _build_recognizer() and _key()."""

    def __init__(self, model_id: str | None = None) -> None:
        # model_id is accepted for registry uniformity (registry.build
        # forwards it to every adapter) but deliberately ignored: a real
        # adapter's identity comes from the weights it loaded, not from an
        # env var that could disagree with them.
        self._recognizer = self._build_recognizer()
        self._silence = np.zeros(int(pcm.SAMPLE_RATE_HZ * TAIL_PADDING_S), dtype=np.float32)

    # --- subclass hooks -------------------------------------------------
    def _build_recognizer(self) -> Any:
        raise NotImplementedError

    def _key(self) -> CompatKey:
        raise NotImplementedError

    # --- Adapter protocol -----------------------------------------------
    def capabilities(self) -> Capabilities:
        return Capabilities(
            streaming=True,
            serializable=False,
            endpointing=False,  # endpointing is the gateway's VAD (internal/audio), not the model's
            modes=frozenset({"online", "offline"}),
            min_chunk_ms=20,
            max_chunk_ms=5000,
        )

    def compatibility_key(self) -> CompatKey:
        return self._key()

    def create_state(self, session_id: str) -> OnlineState:
        return OnlineState(session_id=session_id, stream=self._recognizer.create_stream())

    def infer(self, audio: bytes, st: OnlineState) -> tuple[Delta, OnlineState]:
        samples = pcm.to_float32(audio)
        if samples.size == 0:
            return Delta(text=st.text), st
        st.stream.accept_waveform(pcm.SAMPLE_RATE_HZ, samples)
        self._drain(st.stream)
        st.text = self._recognizer.get_result(st.stream)
        return Delta(text=st.text), st

    def finalize(self, st: OnlineState) -> Delta:
        st.stream.accept_waveform(pcm.SAMPLE_RATE_HZ, self._silence)
        st.stream.input_finished()
        self._drain(st.stream)
        text = self._recognizer.get_result(st.stream)

        # Roll the spent stream so the next utterance in this session
        # starts clean — see point 2 in the module docstring.
        st.stream = self._recognizer.create_stream()
        st.text = ""
        return Delta(text=text)

    def serialize(self, st: OnlineState) -> bytes:
        raise NotSupported(
            f"{type(self).__name__}: sherpa-onnx exposes no way to serialize an "
            "OnlineStream's inference state; recovery is audio replay"
        )

    def deserialize(self, blob: bytes) -> OnlineState:
        raise NotSupported(
            f"{type(self).__name__}: sherpa-onnx exposes no way to restore an "
            "OnlineStream's inference state; recovery is audio replay"
        )

    # --- internals ------------------------------------------------------
    def _drain(self, stream: Any) -> None:
        while self._recognizer.is_ready(stream):
            self._recognizer.decode_stream(stream)
