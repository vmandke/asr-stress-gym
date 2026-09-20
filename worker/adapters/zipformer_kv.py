"""Streaming zipformer driven directly against its ONNX graphs, so the
attention KV cache is ours to size, save, and move.

Same weights as `zipformer.py`. The difference is the whole point: that
adapter goes through sherpa-onnx, which owns the state and exposes no way
to serialize it, so it reports `serializable=False` and every failover
degrades to a full audio replay. This one drives encoder/decoder/joiner
itself and holds the 35 state tensors in a `kvcache.StateBank`, so it is
the first REAL adapter for which the warm-checkpoint tier exists.

What we take on by bypassing the wrapper — feature extraction, the greedy
transducer search, tail padding — is the cost of owning the cache. Measured
against sherpa on the committed corpus (docs/STATUS.md, M11): comparable
transcripts at **0.74x its RTF**, i.e. slightly faster, because we run one
ORT session per graph with no wrapper overhead.

Its compatibility key is deliberately DIFFERENT from `zipformer`'s despite
identical weights: `runtime` and `cache_schema_version` differ, and a
checkpoint from this adapter must never be importable by one that cannot
interpret it. Two `zipformer_kv` workers share a key with each other, which
is what makes a same-model, checkpoint-restoring failover real.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import numpy as np
import onnxruntime as ort

from kvcache import LayoutError, StateBank, StateLayout, deserialize, serialize
from kvcache.state import CACHE_SCHEMA_VERSION

from . import pcm, weights
from .base import Capabilities, CompatKey, Delta, NotSupported

# Same constant, same reason as sherpa_online.py: a streaming encoder has
# not "seen" the last few hundred milliseconds until right-context follows
# it, so finalizing without padding truncates the final word. Measured
# there across 0/0.3/0.6/1.0s; 0.6 is what recovers the full text. Our loop
# inherits the property because it is a property of the model.
TAIL_PADDING_S = 0.6

_MAX_SYMBOLS_PER_FRAME = 4  # greedy transducer guard against a blank-less loop


@dataclass
class KVState:
    session_id: str
    bank: StateBank
    hyp: list[int]
    decoder_out: np.ndarray
    feats: "OnlineFeatures"
    text: str = ""
    finished: bool = False
    audio_tail: bytes = field(default=b"")


class OnlineFeatures:
    """Fbank with our own read cursor.

    `OnlineFbank.get_frame(i)` indexes ABSOLUTELY, and `pop()` shifts those
    indices, so the obvious loop (`get_frame(0)` then `pop(1)`) raises
    `IndexError: deque` on the second chunk. Keeping the cursor here was the
    fix; see docs/STATUS.md M11.
    """

    def __init__(self) -> None:
        import kaldi_native_fbank as knf

        opts = knf.FbankOptions()
        opts.frame_opts.dither = 0.0
        opts.frame_opts.snip_edges = False
        opts.frame_opts.samp_freq = 16000
        opts.mel_opts.num_bins = 80
        self._fb = knf.OnlineFbank(opts)
        self._read = 0

    def push(self, samples: np.ndarray) -> np.ndarray:
        if samples.size:
            self._fb.accept_waveform(16000, samples.tolist())
        out = []
        while self._read < self._fb.num_frames_ready:
            out.append(self._fb.get_frame(self._read))
            self._read += 1
        return np.array(out, dtype=np.float32) if out else np.zeros((0, 80), np.float32)


class ZipformerKVAdapter:
    def __init__(self, model_id: str | None = None) -> None:
        # model_id ignored: identity comes from the weights, not an env var
        # that could disagree with them (see registry.build).
        d = weights.resolve("zipformer-en-20M")
        self._enc = self._session(weights.file(d, "encoder-epoch-99-avg-1.int8.onnx"))
        self._dec = self._session(weights.file(d, "decoder-epoch-99-avg-1.onnx"))
        self._join = self._session(weights.file(d, "joiner-epoch-99-avg-1.int8.onnx"))

        meta = self._enc.get_modelmeta().custom_metadata_map
        self._window = int(meta["T"])                      # frames per encoder call
        self._advance = int(meta["decode_chunk_len"])      # frames consumed per call
        self._context = int(self._dec.get_modelmeta().custom_metadata_map["context_size"])
        self._blank = 0
        self._tokens = self._read_tokens(weights.file(d, "tokens.txt"))
        # raises LayoutError at startup on an export that breaks the convention
        self._layout = StateLayout.from_session(self._enc, pair="prefix:new_")
        self._runtime_version = ort.__version__
        self._dtype = os.environ.get("KV_DTYPE", "fp32")

    @staticmethod
    def _session(path: str) -> ort.InferenceSession:
        opts = ort.SessionOptions()
        # One thread per session: the worker already runs inference off the
        # event loop and serves several sessions concurrently, so letting
        # ORT fan out internally would oversubscribe the container's CPU
        # limit and make concurrency measurements meaningless.
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

    # --- adapter contract ------------------------------------------------

    def capabilities(self) -> Capabilities:
        return Capabilities(
            streaming=True,
            serializable=True,  # the first REAL adapter for which this is true
            endpointing=False,  # endpointing is the gateway's VAD, not the model's
            modes=frozenset({"online", "offline"}),
            min_chunk_ms=20,
            max_chunk_ms=5000,
        )

    def compatibility_key(self) -> CompatKey:
        return CompatKey(
            model_family="transducer",
            model_id="zipformer-en-20M",
            model_revision="icefall-2023-02-17",
            # NOT "sherpa-onnx": same weights, different runtime, and a
            # checkpoint taken here is meaningless to that adapter. This is
            # the same distinction whisper_ct2/whisper_onnx already make.
            runtime="onnxruntime-direct",
            runtime_version=self._runtime_version,
            cache_schema_version=f"{CACHE_SCHEMA_VERSION}/{self._dtype}",
            dtype="int8",  # the WEIGHTS are int8; KV transport width is in cache_schema_version
        )

    def create_state(self, session_id: str) -> KVState:
        hyp = [self._blank] * self._context
        return KVState(
            session_id=session_id,
            bank=self._layout.new_bank(),
            hyp=hyp,
            decoder_out=self._decoder_out(hyp),
            feats=OnlineFeatures(),
        )

    def infer(self, audio: bytes, st: KVState) -> tuple[Delta, KVState]:
        samples = pcm.to_float32(audio)
        st.audio_tail = audio
        frames = np.concatenate([st.bank.pending, st.feats.push(samples)])
        st = self._consume(frames, st)
        return Delta(text=st.text), st

    def finalize(self, st: KVState) -> Delta:
        """Flush with tail padding, then leave the state usable.

        A session outlives an utterance (docs/PROTOCOL.md), so this must not
        leave the adapter in a terminal state — the bug M5 found in the
        sherpa adapters, where `input_finished()` made every second
        utterance fail. Here the encoder has no terminal call at all; we
        pad, drain, and reset only the hypothesis.
        """
        pad = np.zeros(int(16000 * TAIL_PADDING_S), dtype=np.float32)
        frames = np.concatenate([st.bank.pending, st.feats.push(pad)])
        st = self._consume(frames, st, drain=True)
        text = st.text

        # Fresh utterance: new hypothesis and features, but the ENCODER
        # state is deliberately kept — it is acoustic context, not
        # transcript, and discarding it would make the first chunk of the
        # next utterance decode without left context.
        st.hyp = [self._blank] * self._context
        st.bank.hypothesis = st.hyp
        st.decoder_out = self._decoder_out(st.hyp)
        st.text = ""
        st.feats = OnlineFeatures()
        st.bank.pending = np.zeros((0, 80), np.float32)
        return Delta(text=text)

    def state_bytes(self, st: KVState) -> int:
        """Real resident bytes. Every other real adapter reports 0 because
        it genuinely cannot measure an onnxruntime-owned C++ object; this
        one owns numpy arrays and can."""
        return st.bank.nbytes

    def kv_layout(self) -> dict:
        """What is actually in this model's KV cache, for display.

        Read from the ONNX graph via StateLayout, never described by hand,
        so a UI showing it cannot claim a tensor the model does not have.
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
            # The encoder consumes whole windows and holds the remainder as
            # the feature seam; both are part of what a checkpoint carries.
            "encoder_window_frames": self._window,
            "encoder_advance_frames": self._advance,
            "frame_ms": 10,
        }

    def serialize(self, st: KVState) -> bytes:
        return serialize(
            st.bank, self._layout, self.compatibility_key().hash(), dtype=self._dtype
        )

    def deserialize(self, blob: bytes) -> KVState:
        try:
            bank = deserialize(blob, self._layout, self.compatibility_key().hash())
        except LayoutError as e:
            # Surfaced as a normal exception so worker/server.py answers 422
            # ("invalid_checkpoint") and the gateway degrades to audio
            # replay. NotSupported would wrongly mean "this adapter cannot
            # checkpoint at all", which is the opposite of true here.
            raise ValueError(f"incompatible checkpoint: {e}") from None
        # Resume the hypothesis, not just the encoder state. Without this
        # the restored session decodes correctly but its final text is
        # missing everything emitted before the failover.
        hyp = bank.hypothesis or [self._blank] * self._context
        return KVState(
            session_id="restored",
            bank=bank,
            hyp=list(hyp),
            decoder_out=self._decoder_out(hyp),
            feats=OnlineFeatures(),
            text=self._text(hyp),
        )

    # --- the decode loop --------------------------------------------------

    def _consume(self, frames: np.ndarray, st: KVState, *, drain: bool = False) -> KVState:
        """Feed whole encoder windows; retain the remainder as the seam.

        The remainder matters: up to `window - 1` frames (~380ms) are held
        back, and a checkpoint that dropped them changed the transcript
        across a restore. StateBank carries them for exactly that reason.
        """
        while frames.shape[0] >= self._window:
            self._encode(frames[: self._window], st)
            frames = frames[self._advance :]
            st.bank.frames_consumed += self._advance
        st.bank.pending = frames
        return st

    def _encode(self, window: np.ndarray, st: KVState) -> None:
        # window[None] is the batch axis, fixed at 1 — see kvcache.BATCH.
        # It is not a batching hook; nothing here ever puts two sessions in
        # one call.
        inputs = {"x": window[None, :, :].astype(np.float32)}
        inputs.update(st.bank.tensors)
        outputs = self._enc.run(None, inputs)
        self._layout.absorb(st.bank, self._enc.get_outputs(), outputs)
        self._search(outputs[0], st)

    def _search(self, encoder_out: np.ndarray, st: KVState) -> None:
        """Greedy transducer search. The decoder is stateless with
        context_size=2, so the 'decoder state' is just the last two tokens —
        which is why none of it needs to be in the checkpoint."""
        for t in range(encoder_out.shape[1]):
            frame = encoder_out[:, t, :]
            for _ in range(_MAX_SYMBOLS_PER_FRAME):
                logit = self._join.run(
                    None, {"encoder_out": frame, "decoder_out": st.decoder_out}
                )[0]
                token = int(logit[0].argmax())
                if token == self._blank:
                    break
                st.hyp.append(token)
                st.decoder_out = self._decoder_out(st.hyp)
        # The hypothesis is checkpointed state, not a local: mirror it into
        # the bank so serialize() captures the transcript as well as the
        # acoustic cache.
        st.bank.hypothesis = st.hyp
        st.text = self._text(st.hyp)

    def _decoder_out(self, hyp: list[int]) -> np.ndarray:
        y = np.array([hyp[-self._context :]], dtype=np.int64)
        return self._dec.run(None, {"y": y})[0]

    def _text(self, hyp: list[int]) -> str:
        return (
            "".join(self._tokens.get(t, "") for t in hyp[self._context :])
            .replace("▁", " ")
            .strip()
        )
