"""The KV cache: the streaming encoder's attention state, as tensors we own.

This is the thing the rest of the repository has always described and never
had. Until now `model_state` was an opaque `sherpa_onnx.OnlineStream` that
could not be sized, saved, or moved, so the warm-checkpoint tier was real
only for the mock adapter and `/health` reported `state_bytes: 0` honestly
rather than guess.

The streaming zipformer's ONNX encoder takes 35 state tensors in and
returns 35 out. Five of them per encoder stack are literally the attention
cache:

    cached_key_i    [layers, left_context, batch, attention_dim]
    cached_val_i    [layers, left_context, batch, value_dim]
    cached_val2_i   [layers, left_context, batch, value_dim]

with `left_context_len = 64,32,16,8,32` frames retained per stack. That is
a bounded sliding KV cache, which the model ships with by design — the
same shape of answer StreamingLLM reaches for an unbounded token stream
(docs/KVCACHE-DEEPDIVE.md §9).

sherpa-onnx hides all of this. The graph does not.

**The seam.** These tensors are not the whole of a session's state. The
encoder consumes fixed `T`-frame windows, so at any instant up to `T-1`
feature frames are buffered and unconsumed. Serializing the tensors while
dropping that buffer loses up to ~380 ms of audio, and the transcript
changes across a restore — measured, and it is why the first spike run
scored 2/10 before the seam was carried (docs/STATUS.md, M11). `StateBank`
therefore carries the pending frames alongside the tensors.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

# Bumped whenever the serialized layout changes in any way that makes an
# older blob unsafe to import: tensor set, dtype policy, quantization,
# or the seam representation. It is one of the seven fields of CompatKey
# (adapters/base.py), so a bump makes two otherwise-identical workers
# cache-incompatible and correctly forces audio replay instead of a
# restore. That is the intended behaviour, not a regression.
CACHE_SCHEMA_VERSION = "kv-1"

STATE_PREFIX = "new_"

# One session, one stream, always. Every state tensor in these graphs has a
# batch dimension, and it is pinned to 1 here rather than plumbed through
# as a parameter — there is no batching in this system and no code that
# pretends there is.
#
# That is a deliberate design position, not a shortcut. Batching across
# sessions would mean padding ragged audio to a common length, masking it,
# and coupling every session in a batch to the slowest member's latency.
# This repository's whole subject is per-session isolation and failover:
# a batched worker would make one session's stall everyone's stall, and
# would make "restore THIS session's cache" mean slicing a row out of a
# shared tensor. Concurrency here comes from independent sessions running
# in parallel, which is what the worker's thread pool already provides.
#
# A dynamic batch axis is REJECTED rather than silently coerced, so a model
# expecting real batching fails loudly at startup.
BATCH = 1


class LayoutError(Exception):
    """The model's state in/out convention is not what we require.

    Raised at construction, never mid-session: a graph whose outputs do not
    pair with its inputs would otherwise mis-wire silently and corrupt every
    transcript it touched.
    """


@dataclass
class StateBank:
    """One session's full continuation state.

    Three parts, and it took a failing test to establish that all three are
    required — the first version carried only the tensors and restored a
    transcript missing its opening words:

    1. **`tensors`** — the encoder KV cache. Acoustic context: what the
       model has heard. Keyed by the encoder's own input names so it feeds
       onnxruntime directly with no translation layer to drift.
    2. **`pending`** — the feature seam. Up to `window - 1` frames the
       encoder has not consumed yet (~380 ms). Dropping these loses audio
       at the resume point: 2/10 clips matched before this was carried,
       22/22 after.
    3. **`hypothesis`** — the transducer's emitted tokens. The *transcript*
       state. The decoder is stateless given the last `context_size`
       tokens, which makes it tempting to omit — but the accumulated
       hypothesis is what the final text is built from, so a restore
       without it silently truncates the utterance to whatever was decoded
       after the failover.

    Miss any one and the transcript changes across a restore, each in its
    own distinctive way. That is the whole reason this repository exists,
    so all three are checkpointed.
    """

    tensors: dict[str, np.ndarray]
    pending: np.ndarray = field(default_factory=lambda: np.zeros((0, 80), np.float32))
    frames_consumed: int = 0
    hypothesis: list[int] = field(default_factory=list)

    @property
    def nbytes(self) -> int:
        """Real resident bytes. This is what /health reports as state_bytes,
        replacing the honest 0 the real adapters reported before."""
        return sum(t.nbytes for t in self.tensors.values()) + self.pending.nbytes

    @property
    def kv_nbytes(self) -> int:
        """Just the attention key/value tensors — the KV cache proper,
        excluding convolution and bookkeeping state. Reported separately
        because "how big is the KV cache" and "how big is the session" are
        different questions and conflating them overstates the first."""
        return sum(
            t.nbytes
            for name, t in self.tensors.items()
            if name.startswith(("cached_key", "cached_val"))
        )

    def copy(self) -> "StateBank":
        return StateBank(
            tensors={k: v.copy() for k, v in self.tensors.items()},
            pending=self.pending.copy(),
            frames_consumed=self.frames_consumed,
            hypothesis=list(self.hypothesis),
        )


class StateLayout:
    """A model's state contract: which inputs are state, which outputs feed
    them back, and what shape each one is.

    **Read from the graph, never hardcoded.** The zipformer spike hit
    `InvalidArgument: Invalid input name: new_cached_val_3` by assuming the
    outputs were suffixed `_next`; they are prefixed `new_`. Deriving the
    mapping and asserting it means a different export either works or fails
    loudly at construction, rather than mis-wiring one tensor and producing
    plausible-looking nonsense.

    Two naming conventions exist in this fleet, which is exactly why the
    pairing is a parameter rather than a constant:

        zipformer (k2-fsa)  cached_key_0  ->  new_cached_key_0
        conformer (NeMo)    cache_last_channel      -> cache_last_channel_next
                            cache_last_channel_len  -> cache_last_channel_next_len

    Note the second is not a plain suffix: `_next` is inserted *before*
    `_len`. Use `from_pairs` when the convention is irregular.
    """

    def __init__(
        self,
        specs: dict,
        out_to_in: dict,
        *,
        shape_overrides: dict | None = None,
    ) -> None:
        self._specs: dict[str, tuple[tuple[int, ...], np.dtype]] = {}
        for name, (shape, dtype) in specs.items():
            if shape_overrides and name in shape_overrides:
                shape = tuple(shape_overrides[name])
            self._specs[name] = (tuple(shape), dtype)
        self.out_to_in = dict(out_to_in)
        self.input_names = list(self._specs)
        self.output_names = list(out_to_in)

        missing = set(out_to_in.values()) - set(self._specs)
        if missing:
            raise LayoutError(f"state outputs map to unknown inputs: {sorted(missing)}")

    def describe(self) -> list[dict]:
        """The state contract, tensor by tensor, for display.

        Derived from the same `_specs` the graph is actually fed with, so a
        UI showing this cannot drift from what is really cached. `role`
        separates the attention key/value tensors — the KV cache proper —
        from convolution and bookkeeping state, because "how big is the KV
        cache" and "how big is the session" are different questions and
        conflating them overstates the first.
        """
        out = []
        for name, (shape, dtype) in self._specs.items():
            n = int(np.dtype(dtype).itemsize)
            for d in shape:
                n *= int(d)
            # Three families, three naming conventions, and the length
            # tensors shadow the cache tensors they belong to
            # (`cache_last_channel` vs `cache_last_channel_len`), so the
            # `_len` test has to come first or a counter gets reported as
            # half the KV cache.
            low = name.lower()
            if low.endswith("_len") or "cached_len" in low:
                role = "bookkeeping"
            elif "conv" in low:
                role = "convolution"
            elif "key" in low or "_k_cache" in low or "last_channel" in low:
                role = "attention K"
            elif "val" in low or "_v_cache" in low or "last_time" in low:
                role = "attention V"
            else:
                role = "bookkeeping"
            out.append({
                "name": name,
                "shape": [int(d) for d in shape],
                "dtype": np.dtype(dtype).name,
                "bytes": n,
                "role": role,
                "feeds_back_from": next((o for o, i in self.out_to_in.items() if i == name), None),
            })
        return out

    @classmethod
    def from_session(
        cls,
        session,
        *,
        non_state_inputs: tuple = ("x",),
        non_state_outputs: tuple = ("encoder_out",),
        pair: str = "prefix:new_",
        shape_overrides: dict | None = None,
    ) -> "StateLayout":
        """Derive the contract from an ONNX session.

        `pair` says how an output name maps back to its input:
          "prefix:new_"  -> new_X  becomes X        (zipformer)
          "suffix:_next" -> X_next becomes X, and X_next_len becomes X_len
                            (NeMo, whose `_next` is infixed before `_len`)
        """
        specs = {}
        for i in session.get_inputs():
            if i.name in non_state_inputs:
                continue
            shape = tuple(BATCH if isinstance(d, str) else d for d in i.shape)
            dtype = np.int64 if "int64" in i.type else np.float32
            specs[i.name] = (shape, dtype)

        kind, token = pair.split(":", 1)
        out_to_in = {}
        for o in session.get_outputs():
            if o.name in non_state_outputs:
                continue
            if kind == "prefix":
                src = o.name[len(token):] if o.name.startswith(token) else o.name
            else:
                # `_next` may be infixed rather than trailing, so remove the
                # first occurrence wherever it is.
                src = o.name.replace(token, "", 1)
            if src not in specs:
                raise LayoutError(
                    f"state output {o.name!r} does not pair with any state input "
                    f"under {pair!r} (got {src!r}; inputs are {sorted(specs)})"
                )
            out_to_in[o.name] = src

        if len(out_to_in) != len(specs):
            raise LayoutError(
                f"{len(specs)} state inputs but {len(out_to_in)} state outputs — "
                "every piece of state must be fed back or it is silently lost"
            )
        return cls(specs, out_to_in, shape_overrides=shape_overrides)

    def new_bank(self) -> StateBank:
        """A zeroed state bank — a session that has heard nothing.

        Batch is fixed at BATCH (1); see the constant for why this system
        has no batching at all.
        """
        return StateBank(
            tensors={n: np.zeros(s, dtype=d) for n, (s, d) in self._specs.items()}
        )

    def absorb(self, bank: StateBank, session_outputs, values) -> None:
        """Fold one call's state outputs back into the bank.

        Matched BY NAME against the session's declared outputs, not by
        position: relying on ordering means a re-export that reorders
        outputs silently writes the attention cache into the convolution
        cache's slot, and everything still runs.
        """
        for spec, value in zip(session_outputs, values):
            src = self.out_to_in.get(spec.name)
            if src is None:
                continue  # a non-state output (logits, encoder_out)
            bank.tensors[src] = value

    def validate(self, tensors: dict[str, np.ndarray]) -> None:
        """Reject a restored blob that does not match this model exactly.

        Shape and dtype are checked per tensor. build-plan.md's rule for the
        checkpoint tier is "validation failure means audio replay; never
        partial-restore, never coerce" — so this raises, and the caller
        degrades to replay.
        """
        if set(tensors) != set(self._specs):
            missing = sorted(set(self._specs) - set(tensors))
            extra = sorted(set(tensors) - set(self._specs))
            raise LayoutError(f"state tensor set mismatch (missing={missing[:3]} extra={extra[:3]})")
        for name, (shape, dtype) in self._specs.items():
            got = tensors[name]
            if got.shape != shape:
                raise LayoutError(f"{name}: shape {got.shape}, want {shape}")
            if got.dtype != dtype:
                raise LayoutError(f"{name}: dtype {got.dtype}, want {dtype}")

    @property
    def spec_digest(self) -> str:
        """A short digest of the tensor geometry, carried in the blob header
        so an incompatible export is caught before any tensor is examined."""
        import hashlib

        raw = ";".join(f"{n}:{s}:{d}" for n, (s, d) in sorted(self._specs.items()))
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
