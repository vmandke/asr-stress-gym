# Considered and rejected

Every design this repository looked at and did not take, with the reason
and the number that settled it. Kept so the decisions stay *re-decidable*
rather than inherited, and so none of them is rediscovered later as novel.

What was built is in [KVCACHE.md](KVCACHE.md). How other serving stacks
solve the same problems is in [KVCACHE-DEEPDIVE.md](KVCACHE-DEEPDIVE.md).

---

## 1. Bifrost owning the live KV cache

**Rejected, and still rejected — but the reasoning changed, and the change
matters more than the conclusion.**

### The four things it is easy to call "cache"

| Thing | Owner | Meaning | Safe as a live stream cache? |
|---|---|---|---|
| **ASR inference/KV cache** | One worker, keyed by an opaque `handle` | Encoder/predictor/attention state after an ordered sequence of chunks | **No.** Mutable, runtime-specific, bound to a worker-local handle |
| **Gateway checkpoint** | `coord.CheckpointStore` | A validated serialized snapshot for a compatible target | Only where the adapter supports serialization |
| **Bifrost response/semantic cache** | Bifrost | A completed HTTP response, keyed by request content or similarity | Not a substitute |
| **Prefix cache** | One inference runtime | Reuse of common immutable input prefixes | Not session continuation, not transferable by a gateway |

A response cache answers *"have I already answered this request?"* A KV
cache answers *"what state is the model in after this exact ordered
history?"* Those are different problems, and no amount of configuration
turns the first into the second.

Bifrost's semantic caching documents itself as caching completed LLM
responses, by vector similarity or exact hash. It does not export or
rehydrate a provider's attention state. For live ASR, similarity matching
is also the wrong correctness model for audio.

### What breaks if Bifrost routes `Push` blindly

```text
chunk N   -> Bifrost -> worker-a  (creates/updates handle H)
chunk N+1 -> Bifrost -> worker-b  (H is unknown; b has no state)
```

Only bad outcomes: `worker-b` rejects `H` and a request router now has to
learn the gateway's recovery protocol; or it creates fresh state and
silently drops chunk N's context; or Bifrost falls back to an incompatible
worker without the gateway emitting `partial.reset` or replaying. The last
two are transcript-corruption bugs, not latency failures. Passing `handle`
through does not help — it names a worker-local map entry, not a portable
object.

The same applies at an online final. A direct `Flush(handle)` finalizes the
hot state that produced the partials; Bifrost's
`POST /v1/audio/transcriptions` receives a complete WAV, builds fresh
state, infers and discards it. That is a **new transcription, not a
continuation** — measured here at 265.3 ms against 9.2 ms for the direct
flush.

### What changed

The argument above is about a gateway **carrying or recreating** state. It
says nothing about a request that merely **references** state living
somewhere both workers can reach.

That is the loophole [KVCACHE.md](KVCACHE.md) walks through, and it
upgrades the verdict from *structurally impossible* to *possible, priced,
and now built*. Bifrost still never owns, transfers or understands a cache.
It routes a control request within a compatibility cohort; the bytes move
on a side channel it cannot see. Affinity stops being a correctness
requirement and becomes an optimization worth 1.88×.

So the honest current boundary:

| Work | Route | Reason |
|---|---|---|
| Online partials, pinned mode | Direct to the session's worker | Requires handle affinity and mutable state |
| Online partials, `STATELESS_STREAM` | **Bifrost, within the family pool** | State is referenced, not carried — any peer can serve |
| Default online final | Direct `Flush(handle)` | Reuses the hot state that produced the partials |
| Online final with `BIFROST_FINALS=1` | Bifrost, direct fallback on failure | An intentional re-transcription / second opinion |
| Offline transcription | Bifrost | No partials or live handle; retry and fallback are correct |
| Streaming worker failure | Gateway coordinator | Needs compatibility checks, checkpoint validation, reset, replay |

**Invariants that survive all of it:** no component silently substitutes a
model or runtime while a stream is active; a stateful failure always
produces gateway-managed recovery and a Bifrost retry never substitutes for
it; a Bifrost outage cannot lose a final, because the gateway takes the
direct path.

---

## 2. Recompute-style statelessness

**Rejected. It is quadratic, and the derivation is short enough to check.**

This is the *other* thing "make the online path stateless" can mean: no
cache anywhere, every partial re-transcribes the utterance so far. It is
worth separating from §1's reference-carrying statelessness, because the
two share a name and nothing else.

With a 160 ms chunk policy, an utterance of `T` seconds is `N = T / 0.16`
chunks. Chunk `k` must re-transcribe `k · 0.16` seconds:

```text
audio reprocessed  =  0.16 · N(N+1)/2
multiplier vs stateful  =  (N+1)/2  ≈  N/2
```

**Linear in utterance length, not a constant factor.** It cannot be bought
out with faster hardware; it grows with how long someone talks. Against
`zipformer`'s measured RTF of 0.020 ([RTF.md](RTF.md)):

| utterance | chunks `N` | multiplier | compute | effective RTF |
|---|---|---|---|---|
| 5 s | 31 | 16× | 1.6 s | 0.32 |
| 15 s | 94 | 47× | 14.2 s | **0.95** |
| 30 s | 188 | 94× | 56.6 s | **1.89** |
| 120 s | 750 | 375× | 901.2 s | **7.51** |

Effective RTF ≥ 1.0 means the stream falls behind realtime and never
catches up — the lag grows without bound. That threshold is
`N = 2 / RTF_model`:

| adapter | RTF | breaks at |
|---|---|---|
| `zipformer` | 0.020 | **16.0 s** of continuous speech |
| `conformer_ctc` | 0.041 | **7.8 s** |
| `whisper_ct2` | 0.154 | **2.1 s** |

The corpus's `long_form` clips are 60–120 s. A stateless online path does
not degrade there — it **diverges**.

Two further costs are independent of compute:

- **Partial latency scales with utterance length.** The last partial of a
  `T`-second utterance must transcribe `T` seconds. Against the locked
  p95 ≤ 250 ms target ([DECISIONS.md](DECISIONS.md)), zipformer breaks that
  around 12.5 s of speech.
- **Partials stop being monotone.** A streaming decoder commits a prefix.
  An independent re-transcription may revise any earlier word, so displayed
  text churns rather than grows.

The journal does **not** disappear either: audio must still be retained to
re-transcribe from. Statelessness removes the cache, not the storage.

### The trade curve

Two middle grounds, recorded so they are not proposed again as new:

- **Bounded re-transcription window.** Re-transcribe only the last `W`
  seconds. Cost becomes constant per chunk (`W / 0.16`) rather than `N/2`.
  The price is lost context across the window boundary, and a coherent
  final still needs either a full pass or retained state.
- **Coarser partial cadence.** At 2 s instead of 160 ms, the 15 s case
  falls from 47× to roughly 3.75×, which is affordable. But partial latency
  becomes 2 s, far past the 250 ms target — this trades away the
  responsiveness partials exist to provide.

Both trade partial quality or latency for compute. Neither recovers the
cache's benefit; they only make its absence cheaper.

### Why the reference-carrying path is not this

Carrying a *reference* costs bytes, not compute, and the bytes are
constant per chunk rather than growing with the utterance. Priced before it
was built, at a 160 ms chunk (5,120 B of audio):

```
fp32:  round trip 2,185,120 B  =  427× the audio
int8:  round trip   550,120 B  =  107× the audio
```

427× sounds worse than 16×, and is not: one is a **byte** multiplier on a
5 KB payload over a local network, the other is a **compute** multiplier
that ends in divergence. [KVCACHE.md](KVCACHE.md) §8 measures the real
thing at 141× for 500 ms chunks, against 137× predicted.

---

## 3. What the machinery buys

Stated in both directions, because the cache is only worth its complexity
if the complexity is named.

The gateway's session pinning, health-for-selection, rate budget,
checkpoint store and journal replay are not incidental complexity a
general-purpose gateway could absorb. They are the machinery that makes a
per-worker, per-session cache **safe** — and they are deletable only by
deleting the cache.

| Package | Lines (non-test) | Exists because |
|---|---|---|
| `internal/router` | 820 | pin a session; prefer a compatible key on failover; eject and probe |
| `internal/coord` | 397 | restore a checkpoint, else replay audio |
| `internal/journal` | 142 | the replay floor when no checkpoint helps |
| parts of `internal/session`, `cmd/gateway` | — | generation, epochs, compare-and-commit |

That is the price. The 16×–375× compute in §2 is what it buys. A reviewer
who concludes the cache is not worth ~1,400 lines should also accept a
system that cannot transcribe a two-minute utterance in realtime on one
core.

---

## 4. Mooncake

**Not integrated. Evaluated, and the gap is in this repository, not in
Mooncake.**

Mooncake offers a Transfer Engine for high-performance data movement and a
Store for distributed, paged KV storage. In its LLM integrations a prefill
worker exports compatible KV blocks and a decode worker imports them — and
crucially, the *model runtime*, not Mooncake, knows how to allocate, name,
validate and consume those blocks.

| Mooncake concept | Nearest concept here | Crucial difference |
|---|---|---|
| KV blocks | the adapter's serialized state | Whole-state snapshots, not addressable blocks |
| Prefix hash | compatibility key + session identity | Compatibility says *may interpret*, not *which audio this is* |
| Prefill/decode transfer | worker-failure recovery | ASR also needs audio boundaries, fencing, partial-reset rules |
| Distributed Store | `cmd/kvtier` | One in-process tier: non-durable, unsharded, unreplicated |
| Cache-aware scheduler | router key preference | The router does not consult a cache directory |

What it would not fix by itself:

```text
copy a live state object to remote storage
read it on another machine
call infer() as though nothing changed
```

A real integration needs all of: adapter-owned export/import for the
entire live state (process memory is not a state format); exact
compatibility identity covering weights, runtime build, decoder config,
dtype, device ABI and cache schema; **prefix identity** proving which audio
sequence and endpointing state a snapshot represents; distributed ownership
transfer that fences the old writer before the target imports; replay
fallback for every transfer error and incompatible route; retention and
privacy policy for state derived from a caller's audio; and measured
benefit — transfer plus validation plus import costing less than replay.

Items 1, 2, 5 and 7 are now done ([KVCACHE.md](KVCACHE.md)). Items 3, 4 and
6 are not, and the tier is a single in-memory process.

Streaming ASR also does not map cleanly onto prefill/decode. Audio arrives
continuously, partials have a latency budget, and endpointing can reset an
utterance at any moment. Its natural unit is stateful session ownership,
not a blind request proxy.

---

## 5. HF Transformers Whisper as the cache vehicle

**Rejected after a survey.** An earlier plan draft chose it, on the grounds
that `EncoderDecoderCache` exposes `layers[i].keys/.values` cleanly. The
survey that killed it:

| Model / runtime | Cache exposed? | Verdict |
|---|---|---|
| **Streaming zipformer, raw ONNX** | **Yes — 35 state tensors** | **Chosen** |
| NeMo cache-aware FastConformer | Yes — `cache_last_channel`, `cache_last_time` | Strong, but pulls the whole NeMo toolkit |
| WeNet / ESPnet U2++ conformer | Yes — `att_cache`, `cnn_cache` | Viable; another framework to adopt |
| HF Transformers **Whisper** | Yes — `EncoderDecoderCache` | Works, but **not streaming**; +1.5 GB of torch |
| Kyutai Moshi / Mimi | Yes — genuinely streaming | Far from this repo's shape |
| Audio LLMs (Qwen2-Audio, Voxtral, Phi-4) | Yes | Overkill; large models |
| **sherpa-onnx Python wrapper** | **No** — hides state behind `OnlineStream` | Cannot be used |
| CTranslate2 (`whisper_ct2`) | **No** — cache kept internal | Cannot be used |
| Wav2Vec2 / plain CTC | No autoregressive KV — encoder-only | Not applicable |

The realisation that settled it: **sherpa-onnx hides the state, but the
ONNX graph underneath does not.** The wrapper was the only thing in the
way, and the weights were already in the fleet. Adding 1.5 GB of torch to
reach a cache that a model already on disk exports as plain tensors would
have been the expensive way to a worse answer.

`whisper_kv` was eventually built anyway — via ONNX, not HF — and it is the
cleanest KV cache of the three ([KVCACHE.md](KVCACHE.md) §1).

---

## 6. Token-level paging

**Deliberately not applicable, which is different from not done.**

vLLM-style block tables exist because an LLM's KV cache is a per-token,
append-only sequence whose length is unpredictable, so fixed-size blocks
stop the allocator fragmenting and let sequences share prefixes.

This cache is a **fixed-size sliding window per layer**. `left_context_len`
is 64/32/16/8/32 and never grows; the state is exactly 1.09 MB at chunk 1
and at chunk 900. There is no fragmentation to solve and no variable length
to page. A block table here would be a constant-size array of one entry —
the form of the design with none of its motivating property, which is
exactly the theatre this work exists to remove.

## 7. Cross-session prefix sharing

**Not applicable for the same kind of reason.** An LLM shares a system
prompt across thousands of requests, so a content-addressed cache gets
enormous reuse. Two ASR sessions carrying *different audio* share no
prefix; their encoder states diverge from the first frame. Sharing would
only pay for identical audio, which is a response cache (§1), not a KV
cache.

Content-addressing would still buy something real — deduplication of
retries and idempotent replays — but that is a much smaller prize than the
LLM case, and it is not built.

---

## 8. Offline sessions on the stateful path

**A known implementation gap, recorded rather than left to be tripped
over.** Not a rejected alternative — a rejected *current behaviour* that
has not yet been fixed.

An offline session today takes the same path as an online one:

1. `router.Pick` pins a worker, consuming a slot and its rate budget
2. `Open` allocates model state on that worker
3. 2000 ms chunks are pushed; the worker infers and accumulates state
4. **`partial` events are emitted**
5. at `session.end` with Bifrost enabled, all of it is discarded and the
   utterance is re-transcribed

Steps 2–4 are dead work for a class that never uses the result. Step 4 is
also a spec violation: `dispatchChunk` in
[`cmd/gateway/conn.go`](../cmd/gateway/conn.go) calls `Emitter.Partial`
with no mode guard, while [DECISIONS.md](DECISIONS.md) locks "Partial
transcripts — Yes for online mode, no for offline mode."

The intended shape, specified but **not implemented**: an offline session
journals audio, never opens a handle, never pushes, emits no partials, and
issues one stateless transcription at `session.end` — through Bifrost when
enabled, otherwise directly to a `router.Pick`ed worker. That yields one
offline code path whether or not Bifrost is deployed, with Bifrost
replacing only the choice of provider.

Note the direction of the benefit: this *protects* the cache rather than
competing with it. It stops offline traffic occupying worker state it never
reads, leaving that state for the online sessions that do.

---

## Sources

- [Bifrost transcription API](https://docs.getbifrost.ai/api-reference/audio/create-transcription) — complete multipart audio requests, optional streaming, fallbacks.
- [Bifrost retries and fallbacks](https://github.com/maximhq/bifrost/blob/dev/docs/features/retries-and-fallbacks.mdx) — fallbacks are fresh requests and re-run configured plugins.
- [Bifrost semantic cache configuration](https://github.com/maximhq/bifrost/blob/dev/docs/deployment-guides/helm/plugins.mdx) — response-cache semantics and exact-hash mode.
- Local: [the Bifrost boundary in ARCHITECTURE.md](ARCHITECTURE.md#10-the-bifrost-boundary), [gateway final routing](../cmd/gateway/conn.go), [Bifrost deployment README](../bifrost/README.md).
