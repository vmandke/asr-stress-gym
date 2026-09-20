"""NeMo cache-aware streaming FastConformer, driven against its ONNX graph
so its cache is ours.

The second real KV cache in this fleet, and a deliberately *different*
shape from `zipformer_kv` — which is the point. Where zipformer is a
transducer with 35 state tensors and a stateful decoder search, this is a
**CTC** model with three:

    cache_last_channel      [1, 17, 70, 512]   self-attention context
    cache_last_time         [1, 17, 512,  8]   convolution context
    cache_last_channel_len  [1]                how much of it is valid

`cache_last_channel` is the attention cache NVIDIA's cache-aware streaming
papers describe: 17 layers × 70 frames of retained context × 512 dims.
`cache_last_time` is the depthwise-convolution history, which is not
attention but is exactly as load-bearing for continuity.

Three consequences of being CTC rather than a transducer, all of which
make this adapter simpler than `zipformer_kv`:

1. **No decoder, no joiner, no beam.** One graph, one call, argmax per
   frame, collapse repeats, drop blanks.
2. **No hypothesis to checkpoint.** A transducer's emitted tokens are
   state (zipformer_kv had to carry them, found via a failing test). CTC
   decodes each frame independently given the encoder output, so the only
   carry-over is the last emitted token id, for repeat collapsing across a
   chunk boundary.
3. **Bigger cache, fewer tensors.** ~2.7 MB against zipformer's 1.09 MB,
   in 3 tensors instead of 35.

Weights: `stt_en_fastconformer_hybrid_large_streaming_480ms`, CTC branch
only (the export's own comment). Its compatibility key is distinct from
every other adapter's, so a failover between this and anything else is
correctly cross-model.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import numpy as np
import onnxruntime as ort

from kvcache import LayoutError, StateBank, StateLayout, deserialize, serialize
from kvcache.state import BATCH, CACHE_SCHEMA_VERSION

from . import pcm, weights
from .base import Capabilities, CompatKey, Delta
from .zipformer_kv import OnlineFeatures

_BLANK = 1024  # vocab_size per the export's metadata; CTC blank is the last id


@dataclass
class CTCKVState:
    session_id: str
    bank: StateBank
    feats: "OnlineFeatures" = None  # type: ignore[assignment]
    text: str = ""
    # Only carry-over needed for CTC: the last token emitted, so a token
    # repeated across a chunk boundary collapses the same way it would
    # mid-chunk. Not "state" in the KV sense, but it is per-session and it
    # must survive a restore, so it rides in the bank's hypothesis slot.
    last_token: int = -1


class ConformerCtcKVAdapter:
    def __init__(self, model_id: str | None = None) -> None:
        d = weights.resolve("conformer-ctc-en")
        self._sess = self._session(weights.file(d, "model.int8.onnx"))
        meta = self._sess.get_modelmeta().custom_metadata_map

        self._window = int(meta["window_size"])          # 65 frames per call
        self._shift = int(meta["chunk_shift"])           # 56 frames consumed
        self._subsampling = int(meta["subsampling_factor"])
        self._vocab = int(meta["vocab_size"])
        self._cache_len = int(meta["cache_last_channel_dim2"])
        self._tokens = self._read_tokens(weights.file(d, "tokens.txt"))
        # The graph's cache axes are dynamic (named, not sized), so their
        # real dimensions come from the model's OWN metadata rather than
        # being guessed. A wrong value here produces a silently mis-shaped
        # cache that still runs, which is the worst kind of wrong.
        self._layout = StateLayout.from_session(
            self._sess,
            non_state_inputs=("audio_signal", "length"),
            non_state_outputs=("logprobs", "encoded_lengths"),
            pair="suffix:_next",
            shape_overrides={
                "cache_last_channel": (BATCH, int(meta["cache_last_channel_dim1"]),
                                       int(meta["cache_last_channel_dim2"]),
                                       int(meta["cache_last_channel_dim3"])),
                "cache_last_time": (BATCH, int(meta["cache_last_time_dim1"]),
                                    int(meta["cache_last_time_dim2"]),
                                    int(meta["cache_last_time_dim3"])),
                "cache_last_channel_len": (BATCH,),
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
        out: dict[int, str] = {}
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                parts = line.rstrip("\n").rsplit(" ", 1)
                if len(parts) == 2:
                    out[int(parts[1])] = parts[0]
        return out

    # --- adapter contract -------------------------------------------------

    def capabilities(self) -> Capabilities:
        return Capabilities(
            streaming=True,
            serializable=True,
            endpointing=False,
            modes=frozenset({"online", "offline"}),
            min_chunk_ms=20,
            max_chunk_ms=5000,
        )

    def compatibility_key(self) -> CompatKey:
        return CompatKey(
            model_family="ctc",
            model_id="stt_en_fastconformer_hybrid_large_streaming_480ms",
            model_revision="nemo-ctc-branch",
            runtime="onnxruntime-direct",
            runtime_version=self._runtime_version,
            cache_schema_version=f"{CACHE_SCHEMA_VERSION}/{self._dtype}",
            dtype="int8",
        )

    def create_state(self, session_id: str) -> CTCKVState:
        return CTCKVState(session_id=session_id, bank=self._new_bank(), feats=OnlineFeatures())

    def _new_bank(self) -> StateBank:
        bank = self._layout.new_bank()
        # cache_last_channel_len starts at 0: nothing in the cache is valid
        # yet. Leaving it at the tensor's zero default happens to be right,
        # but it is asserted here because "how much of the cache is real"
        # is the one field whose wrong value produces plausible garbage
        # rather than a crash.
        bank.tensors["cache_last_channel_len"] = np.zeros((BATCH,), dtype=np.int64)
        return bank

    def infer(self, audio: bytes, st: CTCKVState) -> tuple[Delta, CTCKVState]:
        samples = pcm.to_float32(audio)
        frames = np.concatenate([st.bank.pending, st.feats.push(samples)])
        while frames.shape[0] >= self._window:
            self._encode(frames[: self._window], st)
            frames = frames[self._shift :]
            st.bank.frames_consumed += self._shift
        st.bank.pending = frames
        return Delta(text=st.text), st

    def finalize(self, st: CTCKVState) -> Delta:
        # Flush whatever is buffered by padding to a full window. CTC has
        # no right-context requirement of the kind the zipformer transducer
        # has, but a partial window still cannot be encoded.
        if st.bank.pending.shape[0] > 0:
            pad = np.zeros((self._window - st.bank.pending.shape[0], 80), np.float32)
            self._encode(np.concatenate([st.bank.pending, pad]), st)
        text = st.text

        # New utterance: reset the transcript, KEEP the acoustic cache —
        # it is context, not content, and discarding it would make the next
        # utterance's first chunk decode cold.
        st.text = ""
        st.last_token = -1
        st.feats = OnlineFeatures()
        st.bank.pending = np.zeros((0, 80), np.float32)
        st.bank.hypothesis = []
        return Delta(text=text)

    def state_bytes(self, st: CTCKVState) -> int:
        return st.bank.nbytes

    def kv_layout(self) -> dict:
        """See ZipformerKVAdapter.kv_layout. NeMo names its state
        cache_last_channel / cache_last_time rather than cached_key /
        cached_val, which is why the role mapping lives in StateLayout
        rather than being guessed from one family's convention."""
        tensors = self._layout.describe()
        return {
            "model_family": self.compatibility_key().model_family,
            "runtime": "onnxruntime",
            "dtype": self._dtype,
            "tensors": tensors,
            "tensor_count": len(tensors),
            "total_bytes": sum(t["bytes"] for t in tensors),
            "kv_bytes": sum(t["bytes"] for t in tensors if t["role"].startswith("attention")),
            "frame_ms": 10,
        }

    def serialize(self, st: CTCKVState) -> bytes:
        st.bank.hypothesis = [st.last_token]
        return serialize(st.bank, self._layout, self.compatibility_key().hash(), dtype=self._dtype)

    def deserialize(self, blob: bytes) -> CTCKVState:
        try:
            bank = deserialize(blob, self._layout, self.compatibility_key().hash())
        except LayoutError as e:
            raise ValueError(f"incompatible checkpoint: {e}") from None
        last = bank.hypothesis[0] if bank.hypothesis else -1
        return CTCKVState(session_id="restored", bank=bank, feats=OnlineFeatures(), last_token=last)

    # --- inference --------------------------------------------------------

    def _encode(self, window: np.ndarray, st: CTCKVState) -> None:
        # [1, 80, T]: this model wants channels-first, unlike zipformer.
        x = window.T[None, :, :].astype(np.float32)
        inputs = {
            "audio_signal": x,
            "length": np.array([window.shape[0]], dtype=np.int64),
        }
        inputs.update(st.bank.tensors)
        outputs = self._sess.run(None, inputs)

        logprobs = outputs[0]
        self._layout.absorb(st.bank, self._sess.get_outputs(), outputs)
        self._decode(logprobs, st)

    def _decode(self, logprobs: np.ndarray, st: CTCKVState) -> None:
        """Greedy CTC: argmax per frame, collapse repeats, drop blanks.

        `last_token` carries across calls so a token repeated over a chunk
        boundary collapses exactly as it would within one.
        """
        ids = logprobs[0].argmax(axis=-1)
        emitted: list[int] = []
        for t in ids:
            t = int(t)
            if t != st.last_token and t != _BLANK:
                emitted.append(t)
            st.last_token = t
        if emitted:
            st.text += "".join(self._tokens.get(t, "") for t in emitted).replace("▁", " ")
            st.text = st.text.strip()
