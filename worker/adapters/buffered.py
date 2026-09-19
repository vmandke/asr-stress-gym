"""Shared machinery for the two NON-streaming adapters (whisper_ct2.py,
whisper_onnx.py).

An encoder-decoder like Whisper consumes a whole utterance at once; there
is no meaningful mid-utterance state to carry, and no partial to emit
before the end. That is not a limitation being worked around here — it is
the fleet's whole point that a backend can be perfectly healthy and simply
the wrong shape for online traffic (docs/implementation-plan.md, defect
#11). So these adapters declare `streaming=False` and `modes={"offline"}`,
and the router filters them out of online sessions before it ever scores
one. `infer` buffers and returns the text unchanged; `finalize` is where
the model actually runs.

**The minimum-length guard is not defensive tidiness.** sherpa-onnx's
Whisper recognizer SIGSEGVs — kills the whole worker process, no Python
exception — when handed a zero-length buffer. Found by feeding one during
the M5 spike, and worth stating plainly: without this guard, a session
that ended with nothing buffered would look exactly like a crashed worker,
which is precisely the signal chaos testing depends on being real. Both
adapters share the guard so the two runtimes stay behaviourally
comparable, which is the entire point of the d/e pair.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

import numpy as np

from . import pcm
from .base import Capabilities, CompatKey, Delta, NotSupported

# Below this, finalize returns empty text without touching the model.
# See the module docstring.
MIN_UTTERANCE_S = 0.1


@dataclass
class BufferedState:
    session_id: str
    buf: bytearray = field(default_factory=bytearray)
    text: str = ""


class BufferedAdapter:
    """Base class; subclasses provide _transcribe() and _key()."""

    def __init__(self, model_id: str | None = None) -> None:
        # model_id ignored on purpose — see sherpa_online.StreamingSherpaAdapter.
        #
        # server.py runs inference off the event loop (asyncio.to_thread),
        # so two sessions finalizing at once reach _transcribe
        # concurrently. One decoder instance is shared by every session in
        # this process, and neither faster-whisper's WhisperModel nor a
        # sherpa OfflineRecognizer promises re-entrancy on a single
        # instance, so calls are serialized here. Offline work is not the
        # realtime path (docs/build-plan.md invariant 14) and a lock costs
        # nothing it needs.
        self._lock = threading.Lock()
        self._load()

    # --- subclass hooks -------------------------------------------------
    def _load(self) -> None:
        raise NotImplementedError

    def _transcribe(self, samples: np.ndarray) -> str:
        raise NotImplementedError

    def _key(self) -> CompatKey:
        raise NotImplementedError

    # --- Adapter protocol -----------------------------------------------
    def capabilities(self) -> Capabilities:
        return Capabilities(
            streaming=False,
            serializable=False,
            endpointing=False,
            modes=frozenset({"offline"}),
            # An offline backend is handed whole utterances, not the 20ms
            # slivers a streaming one accepts. max is Whisper's own 30s
            # receptive field; past it the model truncates anyway.
            min_chunk_ms=100,
            max_chunk_ms=30_000,
        )

    def compatibility_key(self) -> CompatKey:
        return self._key()

    def create_state(self, session_id: str) -> BufferedState:
        return BufferedState(session_id=session_id)

    def infer(self, audio: bytes, st: BufferedState) -> tuple[Delta, BufferedState]:
        # No partial. `streaming=False` means exactly this, and pretending
        # otherwise — by re-transcribing the buffer on every push — would
        # be both a lie about the capability and quadratic in utterance
        # length.
        st.buf.extend(audio)
        return Delta(text=st.text), st

    def finalize(self, st: BufferedState) -> Delta:
        samples = pcm.to_float32(bytes(st.buf))
        st.buf = bytearray()  # roll for the next utterance in this session
        if samples.size < int(pcm.SAMPLE_RATE_HZ * MIN_UTTERANCE_S):
            st.text = ""
            return Delta(text="")
        with self._lock:
            st.text = self._transcribe(samples)
        return Delta(text=st.text)

    def serialize(self, st: BufferedState) -> bytes:
        raise NotSupported(
            f"{type(self).__name__}: an encoder-decoder holds no mid-utterance "
            "state worth checkpointing; recovery is audio replay"
        )

    def deserialize(self, blob: bytes) -> BufferedState:
        raise NotSupported(
            f"{type(self).__name__}: an encoder-decoder holds no mid-utterance "
            "state worth checkpointing; recovery is audio replay"
        )
