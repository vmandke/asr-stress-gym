"""Whisper, driven against its ONNX graphs so the decoder's KV cache is ours.

The third KV model in the fleet, and the only one whose cache is a
*textbook* transformer KV cache — the graph even names it that way:

    in_n_layer_self_k_cache   [4, 1, 448, 384]   decoder self-attention K
    in_n_layer_self_v_cache   [4, 1, 448, 384]   decoder self-attention V
    offset                    [1]                where the next token writes
    n_layer_cross_k / _v      [4, 1, T, 384]     cross-attention to the audio

4 decoder layers x 448 max tokens x 384 dims. `offset` is the write cursor:
exactly the "append K and V for token t, attend over 0..t" mechanism from
docs/KVCACHE-DEEPDIVE.md §1, exposed as a tensor.

**Offline only, and that is not a limitation being hidden.** Whisper is not
a streaming model: its encoder consumes a fixed 30-second window and there
is no cache-aware chunking of the kind zipformer and FastConformer have.
Advertising `streaming: true` would make `router.Pick` hand it live
sessions it cannot serve at a sane latency. So it declares
`modes: {"offline"}`, which has two consequences worth stating:

  - `Pick` filters it out of every online session, structurally.
  - Offline work is exactly what the gateway routes through Bifrost, so
    this pair is the reason Bifrost has real traffic to carry.

Its cache is genuinely serializable and transferable — that is what makes
it a KV worker — but the honest reuse story is weaker than the streaming
models': re-encoding new audio changes the cross-attention, which strictly
invalidates the self-attention states computed against the old encoding.
Within one decode pass the reuse is exact and is what makes decoding
linear rather than quadratic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import numpy as np
import onnxruntime as ort

from kvcache import LayoutError, StateBank, StateLayout, deserialize, serialize
from kvcache.state import BATCH, CACHE_SCHEMA_VERSION

from . import pcm, weights
from .base import Capabilities, CompatKey, Delta

# whisper-tiny.en offsets. The export's metadata gives translate=50357,
# which pins these to the ENGLISH-ONLY table (the multilingual one is one
# higher throughout). Derived rather than copied from a blog post.
_EOT = 50256
_SOT = 50257
_NO_TIMESTAMPS = 50362
_MAX_TOKENS = 448
_N_MELS = 80
_WINDOW_S = 30  # whisper's fixed encoder window


@dataclass
class WhisperKVState:
    session_id: str
    bank: StateBank
    audio: bytearray = field(default_factory=bytearray)
    text: str = ""


class WhisperKVAdapter:
    def __init__(self, model_id: str | None = None) -> None:
        d = weights.resolve("whisper-tiny.en-onnx")
        self._enc = self._session(weights.file(d, "tiny.en-encoder.int8.onnx"))
        self._dec = self._session(weights.file(d, "tiny.en-decoder.int8.onnx"))
        self._tokens = self._read_tokens(weights.file(d, "tiny.en-tokens.txt"))

        meta = self._enc.get_modelmeta().custom_metadata_map
        self._layers = int(meta["n_audio_layer"])
        self._dim = int(meta["n_audio_state"])

        # Only the SELF-attention cache is checkpointed state. Cross-attention
        # is a pure function of the audio and is recomputed on every encode,
        # so carrying it would quadruple the blob for something we can derive.
        self._layout = StateLayout(
            specs={
                "in_n_layer_self_k_cache": ((self._layers, BATCH, _MAX_TOKENS, self._dim), np.float32),
                "in_n_layer_self_v_cache": ((self._layers, BATCH, _MAX_TOKENS, self._dim), np.float32),
            },
            out_to_in={
                "out_n_layer_self_k_cache": "in_n_layer_self_k_cache",
                "out_n_layer_self_v_cache": "in_n_layer_self_v_cache",
            },
        )
        self._runtime_version = ort.__version__
        self._dtype = os.environ.get("KV_DTYPE", "fp32")

    @staticmethod
    def _session(path: str) -> ort.InferenceSession:
        opts = ort.SessionOptions()
        opts.inter_op_num_threads = 1
        opts.intra_op_num_threads = 1
        return ort.InferenceSession(path, opts, providers=["CPUExecutionProvider"])

    @staticmethod
    def _read_tokens(path: str) -> dict[int, str]:
        """k2-fsa ships Whisper's vocabulary BASE64-ENCODED, unlike the
        zipformer and conformer token files which are plain text. Reading it
        the same way produces output like 'IEk=J20=IG5vdA==' — valid-looking
        and completely wrong, which is how this was noticed."""
        import base64

        out: dict[int, str] = {}
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                parts = line.rstrip("\n").rsplit(" ", 1)
                if len(parts) != 2:
                    continue
                try:
                    out[int(parts[1])] = base64.b64decode(parts[0]).decode("utf-8", "replace")
                except Exception:
                    out[int(parts[1])] = parts[0]
        return out

    # --- adapter contract -------------------------------------------------

    def capabilities(self) -> Capabilities:
        return Capabilities(
            streaming=False,  # see the module docstring: not a streaming model
            serializable=True,
            endpointing=False,
            modes=frozenset({"offline"}),
            min_chunk_ms=20,
            max_chunk_ms=30000,
        )

    def compatibility_key(self) -> CompatKey:
        return CompatKey(
            model_family="encoder-decoder",
            model_id="whisper-tiny.en",
            model_revision="k2-fsa-onnx",
            runtime="onnxruntime-direct",
            runtime_version=self._runtime_version,
            cache_schema_version=f"{CACHE_SCHEMA_VERSION}/{self._dtype}",
            dtype="int8",
        )

    def create_state(self, session_id: str) -> WhisperKVState:
        return WhisperKVState(session_id=session_id, bank=self._layout.new_bank())

    def infer(self, audio: bytes, st: WhisperKVState) -> tuple[Delta, WhisperKVState]:
        # Offline: accumulate. Whisper cannot usefully decode a 160ms chunk,
        # and pretending otherwise would burn a 30s encode per chunk.
        st.audio.extend(audio)
        return Delta(text=st.text), st

    def finalize(self, st: WhisperKVState) -> Delta:
        samples = pcm.to_float32(bytes(st.audio))
        if samples.size >= 1600:  # below 0.1s there is nothing to transcribe
            st.text = self._transcribe(samples, st)
        text = st.text
        st.audio = bytearray()
        st.text = ""
        st.bank = self._layout.new_bank()
        return Delta(text=text)

    def state_bytes(self, st: WhisperKVState) -> int:
        return st.bank.nbytes

    def kv_layout(self) -> dict:
        """See ZipformerKVAdapter.kv_layout.

        Whisper's is the only cache here that is genuinely decoder
        self-attention K/V in the textbook sense — and also the only one
        that is offline-only, so it never appears on a live streaming
        session's panel.
        """
        tensors = self._layout.describe()
        return {
            "model_family": self.compatibility_key().model_family,
            "runtime": "onnxruntime",
            "dtype": self._dtype,
            "tensors": tensors,
            "tensor_count": len(tensors),
            "total_bytes": sum(t["bytes"] for t in tensors),
            "kv_bytes": sum(t["bytes"] for t in tensors if t["role"].startswith("attention")),
            "offline_only": True,
        }

    def serialize(self, st: WhisperKVState) -> bytes:
        return serialize(st.bank, self._layout, self.compatibility_key().hash(), dtype=self._dtype)

    def deserialize(self, blob: bytes) -> WhisperKVState:
        try:
            bank = deserialize(blob, self._layout, self.compatibility_key().hash())
        except LayoutError as e:
            raise ValueError(f"incompatible checkpoint: {e}") from None
        return WhisperKVState(session_id="restored", bank=bank)

    # --- inference --------------------------------------------------------

    def _mel(self, samples: np.ndarray) -> np.ndarray:
        """Whisper's log-mel over a fixed 30s window, zero-padded."""
        import kaldi_native_fbank as knf

        want = 16000 * _WINDOW_S
        if samples.size < want:
            samples = np.pad(samples, (0, want - samples.size))
        else:
            samples = samples[:want]

        opts = knf.WhisperFeatureOptions()
        opts.dim = _N_MELS
        fb = knf.OnlineWhisperFbank(opts)
        fb.accept_waveform(16000, samples.tolist())
        fb.input_finished()
        frames = [fb.get_frame(i) for i in range(fb.num_frames_ready)]
        mel = np.array(frames, dtype=np.float32).T  # [80, T]

        # kaldi-native-fbank returns RAW mel energies here (measured: 0..34),
        # not log-mel. Whisper's encoder expects its own normalisation, and
        # feeding the raw energies produces confident hallucination rather
        # than an error — "I'm not a good one." for a fox-and-dog sentence,
        # which is how this was caught. The three lines below are Whisper's
        # own log_mel_spectrogram tail.
        mel = np.log10(np.maximum(mel, 1e-10))
        mel = np.maximum(mel, mel.max() - 8.0)
        mel = (mel + 4.0) / 4.0
        return mel[None, :, :]

    def _transcribe(self, samples: np.ndarray, st: WhisperKVState) -> str:
        cross_k, cross_v = self._enc.run(None, {"mel": self._mel(samples)})

        tokens = [_SOT, _NO_TIMESTAMPS]
        out: list[int] = []
        offset = 0
        # First pass primes the cache with the prompt; every pass after it
        # feeds ONE token and attends over everything already cached. That
        # is the whole point of the KV cache, and it is why this loop is
        # linear in output length rather than quadratic.
        feed = np.array([tokens], dtype=np.int64)

        for _ in range(_MAX_TOKENS - len(tokens)):
            logits, new_k, new_v = self._dec.run(
                None,
                {
                    "tokens": feed,
                    "in_n_layer_self_k_cache": st.bank.tensors["in_n_layer_self_k_cache"],
                    "in_n_layer_self_v_cache": st.bank.tensors["in_n_layer_self_v_cache"],
                    "n_layer_cross_k": cross_k,
                    "n_layer_cross_v": cross_v,
                    "offset": np.array([offset], dtype=np.int64),
                },
            )
            st.bank.tensors["in_n_layer_self_k_cache"] = new_k
            st.bank.tensors["in_n_layer_self_v_cache"] = new_v
            offset += feed.shape[1]

            nxt = int(logits[0, -1].argmax())
            if nxt == _EOT:
                break
            out.append(nxt)
            feed = np.array([[nxt]], dtype=np.int64)

        st.bank.hypothesis = out
        return "".join(self._tokens.get(t, "") for t in out).replace("▁", " ").strip()
