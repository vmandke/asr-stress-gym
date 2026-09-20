# The reading list, read for you

[INTERVIEW-READING-LIST.md](INTERVIEW-READING-LIST.md) tells you what to
read. This document *is* the reading. Every item on that list is worked
through here — the mechanism, the numbers, the exact sentence to say, and
the sentence that gets you caught. If you read only this, you are covered
for the interview the list was written for.

Order matters and it is the list's order: this repository first, then
attention, then the KV cache, then the systems papers that exist because
the KV cache is expensive. Do not skip to Mooncake.

**How to use it.** Read Parts I–VI once for understanding. Then work
Part VII out loud — the drills are the actual deliverable, and an answer
you have only read is not an answer you have. Part VIII is the
night-before card.

| Part | Covers | Reading-list items |
|---|---|---|
| [I](#part-i--this-repository) | This repo: state, ownership, fencing, recovery, the shared tier | Part I |
| [II](#part-ii--transformers-and-attention) | Illustrated Transformer, Raschka self-attention, Vaswani | Part II |
| [III](#part-iii--the-kv-cache-itself) | Raschka KV cache, the size formula | Part III |
| [IV](#part-iv--scheduling-and-memory-management) | ORCA, Anyscale, PagedAttention, prefix caching | Part IV |
| [V](#part-v--unbounded-streams-and-attention-sinks) | StreamingLLM | Part V |
| [VI](#part-vi--disaggregation-and-mooncake) | Mooncake | Part VI |
| [VII](#part-vii--the-fifteen-drills-with-answers) | 15 drills, answered | Part VII |
| [VIII](#part-viii--traps-numbers-and-the-review-card) | Traps, numbers, review card | Final card |

---

## Part I — this repository

Everything else in this document is context for this part. An interview
about this repo goes wrong in exactly one way: claiming it is an LLM
serving stack. It is not. It is the **distributed-systems half** of one,
built on streaming ASR, where the state is real and the transfer problem
is real but the memory-management problem largely is not.

### 1.1 The one-paragraph thesis

> A KV cache is model-internal state that saves recomputing work over an
> already-processed prefix. It buys latency and compute; it costs memory,
> placement, compatibility, ownership, eviction and privacy. In this
> repository the worker-local streaming ASR state plays the same
> operational role — same placement problem, same ownership problem — but
> it is not portable transformer KV state for the four production
> adapters, so **audio replay is the correctness path** and any state
> transfer is an optimization layered above it.

Say that, then stop. Everything below is elaboration you produce on
demand.

### 1.2 What the state actually is, in bytes

Read live from `/v1/kv/layout` on each worker. Three families, three
genuinely different shapes of the same idea:

| Family | Tensors | KV bytes | Total state |
|---|---|---|---|
| `zipformer_kv` — transducer | 35 | 466,944 | **1,091,664** |
| `conformer_ctc_kv` — CTC | 3 | 2,715,648 | **2,715,656** |
| `whisper_kv` — encoder/decoder | 2 | 5,505,024 | **5,505,024** |

**zipformer** is a bounded sliding window shipped by the model —
`left_context_len` of 64/32/16/8/32 per stack, fixed, never growing. Only
467 KB of its 1.09 MB is attention K/V; **614 KB is convolution state**,
which has no LLM analogue at all. That single fact kills the lazy answer
"it's just a KV cache."

**conformer_ctc** is NeMo's cache-aware streaming pair —
`cache_last_channel` [1,17,70,512] at 2.44 MB and `cache_last_time`
[1,17,512,8] at 279 KB. Wildly asymmetric.

**whisper_kv** is the textbook case and the only symmetric one:
`in_n_layer_self_k_cache` and `in_n_layer_self_v_cache`, both
[4,1,448,384] = 2,752,512 B. 4 layers × 448 positions × 384 dims, K and V
exactly equal. This is the diagram every LLM blog draws.

Whisper's state is **5× zipformer's**, which is why the design uses one
cache tier *per family* rather than one for the fleet: a burst in one
family would otherwise evict another family's state through a shared byte
ceiling, and those sessions take a 424 and replay. Co-tenancy creating a
cross-family blast radius, for no benefit.

### 1.3 A checkpoint is four things, and three are not the tensors

The most transferable finding in the repo, arrived at through three
distinct test failures each with its own signature:

| Part | Omitting it produces | Carried? |
|---|---|---|
| **KV tensors** — acoustic context | nothing works | yes |
| **Feature seam** — up to 38 unconsumed frames (~380 ms) | corruption *at the resume point*: `FOX`→`OX` | yes |
| **Decode hypothesis** — emitted tokens | truncation *from the beginning*: the transcript starts mid-sentence | yes |
| **Extractor sample position** | one word differs on 1 of 12 clips | **no** — §1.9 |

"The KV cache" in casual usage means row one. **Three quarters of what
has to move is not it.** If an interviewer asks you to design state
transfer, this table is the answer: enumerate the state, not the tensors.

### 1.4 Ownership — who owns what, and why it is drawn there

```text
Gateway owns: session metadata, audio journal, recovery policy,
              the client event contract
Worker owns:  opaque handle → live model_state
Adapter owns: model-state format + compatibility identity + serialization

If the worker dies:
  valid compatible checkpoint? restore + replay the tail
  otherwise:                   fresh state + replay from last final
```

Three boundary rules, each load-bearing:

- **The gateway never knows a backend is a model.** It holds an opaque
  `handle` and an opaque `CacheCompatibilityKey` it only ever compares for
  equality ([`internal/session/state.go:26`](../internal/session/state.go#L26)).
  The seven fields that *compose* that key —
  `model_family, model_id, model_revision, runtime, runtime_version,
  cache_schema_version, dtype` — live only worker-side in
  [`worker/adapters/base.py`](../worker/adapters/base.py) and are hashed
  to `sha256:…` before crossing the wire.
- **Checkpoints live gateway-side, not worker-side.** A checkpoint *about*
  worker-a stored *in* worker-a dies exactly when it was needed.
- **`internal/audio` is the only package that touches a PCM sample.**
  Everything above it moves opaque byte slices tagged with sequence ranges.

Never say "the gateway holds the KV cache." It holds the *reference*, the
compatibility key, the audio needed to rebuild the state, and — on the
mock adapter only — a checkpoint blob.

### 1.5 Fencing: why sequence numbers *and* a generation

They answer different questions, and collapsing them is the classic bug.

| Counter | Question it answers | Enforced where |
|---|---|---|
| `last_seq_applied` | *How far through the audio is this state?* | worker: `seq_end <= last_seq_applied` → return cached text, do not re-infer |
| `generation` | *Which version of the state am I overwriting?* | worker: compare-and-commit, or `409` |
| `StreamEpoch` | client reconnected | gateway |
| `FailoverEpoch` | backend changed | gateway |

`compare_and_commit` in [`worker/state.py:68`](../worker/state.py#L68) is
the whole fencing story in fifteen lines: under the record's lock, if
`rec.generation != expected_generation`, raise `StaleGeneration` rather
than apply over newer state.

**Why both.** Sequence-only fencing cannot distinguish "this is a retry of
chunk 40" from "a second writer also produced a chunk 40 from different
state" — the seq matches in both cases, and only one is safe. Generation
catches the second. Generation-only fencing cannot make replay idempotent:
after a failover the gateway re-pushes audio the state may already
reflect, and without a seq comparison every replay double-applies.
Idempotency-by-seq is precisely what makes **replay safe to call freely**,
which is what makes replay usable as the universal fallback.

Note also that generations are **worker-local** and never need to match
the dead worker's numbering — a restored handle starts at generation 0 and
the coordinator adopts whatever the new worker returns.

### 1.6 What happens when a worker dies

[`internal/coord/failover.go`](../internal/coord/failover.go), a bounded
loop of at most `MaxFailoverAttempts = 3`:

```text
dispatchChunk errors
  ├─ 429? → Router.On429(worker, retryAfter): zero the bucket, never sleep
  ▼
HandleBackendFailure
  Report(dead, ok=false)
  for attempt 1..3:
      target := Pick(mode, exclude=tried, prefer=currentKey)
      sameKey ? RecoverSameModel : RecoverCrossModel
```

|  | same compatibility key | different key |
|---|---|---|
| **valid checkpoint** | `Restore` the blob, replay only the tail after `cp.Seq` | never attempted — state from one model is meaningless to another |
| **no/corrupt checkpoint** | falls through to full replay | fresh `Open`, replay from the last committed final |

Both paths then, in this order: emit **`partial.reset`** on any failover,
then re-emit the replayed text as a fresh `partial`. Without the second
step the client sees a reset with nothing after it and the transcript goes
blank.

Four details in that file worth having ready, because they are the kind of
thing an interviewer probes for:

- **Replay is bounded at the last committed final**, never from session
  start. The journal is a ring of 1500 records ≈ 30 s and **wraps rather
  than grows**.
- **Unbind late, not early.** Unbinding the dead worker on entry
  double-decrements on any failure path, leaving `outstanding` permanently
  negative — and since `Pick` scores on least-outstanding, a worker at −9
  looks maximally attractive forever. The bind transfers only once
  recovery has actually succeeded.
- **Roll back a failed candidate.** If replay into the new handle fails,
  the session's state is restored to the previous owner and the disposable
  handle is closed. Handles are opaque *within* a worker, so two workers
  may issue the same string — ownership is part of the comparison.
- **Count the two axes separately.**
  `failover_same_model_total` / `failover_cross_model_total` asks *did the
  key match?*; `checkpoint_restores_total` / `checkpoint_degraded_total`
  asks *did the warm tier pay off?* While every worker was a mock both
  questions had the same answer, and the code conflated them. A real
  same-model failover between two zipformer workers reads:

```json
{ "failover_same_model_total": 1, "failover_cross_model_total": 0,
  "checkpoint_restores_total": 0, "checkpoint_degraded_total": 1,
  "duplicate_finals_total": 0 }
```

That is the project's thesis as four counters: a compatible worker *was*
chosen, the warm tier *was* attempted, it did *not* pay off, and replay
recovered the session anyway.

### 1.7 The shared tier: reference-in-request, bytes on a side channel

The design move that upgrades "Bifrost can never route streaming chunks"
from *structurally impossible* to *possible, priced, built*.

```text
gateway ──► Bifrost ──► worker      file=<chunk>
                                    state_ref=sess:41  state_sink=sess:42
                        worker ──► kvtier  GET sess:41   ← side channel
                               ──► kvtier  PUT sess:42   ← side channel
                        worker ──► {"text": "..."}       ← only text
```

Through the load balancer: the audio plus ~64 bytes of key. Between worker
and tier: the 1.09 MB. **The control request goes through the load
balancer; the state bytes never do.** Mooncake, LMCache and NVIDIA
Dynamo/NIXL all share that shape; they differ in transport (RDMA) and
scale, not in structure.

Five invariants make it safe, and each one came from a bug:

1. **Versions are immutable.** A chunk reads version N and writes version
   M, never overwriting its input — which is what makes a retry safe,
   because the retry finds its input intact.
2. **Immutable everywhere, including the worker's hot cache.** Adapters
   mutate state *in place* and return the same object. Caching the live
   object meant a retry landing on the same worker read its own corrupted
   entry and applied the chunk twice, while the identical retry routed to
   a peer was correct — **correctness as a function of placement**,
   precisely the property the design exists to remove. Fix: `_hot_take`
   consumes on load, plus create-only `PutIfAbsent` writes.
3. **Retire version N once N+1 is durable.** Without it, a 60 s session at
   160 ms cadence writes ~375 versions and nothing reclaims them: measured
   at 30 streams, **5,682 of 6,528 versions (87%) were evicted by the
   LRU**, costing 12 sessions their state. After retirement: 0 evictions,
   peak resident 268 MB → **46 MB**.
4. **Validate before deserializing.** The compatibility key advertised by
   the blob is checked *before* any tensor is examined: mismatch is `422
   incompatible_state` naming both keys; a reference resolving nowhere is
   `424`. Never partial-restore, never coerce.
5. **safetensors, never pickle.** These blobs leave the process
   (adapter → worker → tier → a *different* worker). `pickle.loads`
   executes arbitrary code by design, which would make "restore a
   checkpoint" a remote-code-execution primitive reachable by anything
   that can write to the store. There is a test asserting the blob is not
   a pickle.

**What it costs**, measured on a 2.20 s clip, 5 × 500 ms chunks:

| | alternate workers | sticky |
|---|---|---|
| local hits / tier fetches | 0 / 4 | 4 / 0 |
| wall | 113.6 ms | 60.4 ms |
| state bytes / audio bytes | **141×** | **78×** |

Two conclusions, and they are the point:

- **Locality is worth 1.88×**, so cache-aware routing is where the
  performance is — not a nicety.
- **Both policies are correct.** They differ only in cost. Affinity stops
  being a *correctness requirement* and becomes an *optimization*:

```text
pinned session   affinity REQUIRED    a miss is data loss
shared tier      affinity PREFERRED   a miss costs a fetch
```

The prediction was made before it was built — 427× for a 160 ms fp32
chunk, scaling to 137× at 500 ms — and measured at 141×. Quote the
prediction *and* the measurement; agreeing with your own estimate is the
credible part.

### 1.8 The Bifrost boundary

Measured against Bifrost with a purpose-built echo provider:

| Direction | Unknown fields |
|---|---|
| gateway → worker | **forwarded verbatim** |
| worker → gateway | **stripped** — only `text` plus Bifrost's `extra_fields` |

That is not a bug; it is what a *semantic* gateway is for — it parses
provider responses into a normalized schema, and anything outside that
schema is by definition not part of the contract. Two consequences shape
the design: state can travel **in** but not **back**, so the caller names
the output reference (`state_sink`) up front; and a cache miss is signalled
as an **HTTP status**, because status codes survive the hop while response
fields do not.

What routes where, and why:

| Work | Route | Reason |
|---|---|---|
| Online partials, pinned | direct to the session's worker | needs handle affinity + mutable state |
| Online partials, `STATELESS_STREAM` | Bifrost, within the family pool | state is referenced, not carried |
| Online final (default) | direct `Flush(handle)` | reuses the hot state that produced the partials |
| Online final, `BIFROST_FINALS=1` | Bifrost | deliberate re-transcription / second opinion |
| Offline | Bifrost | no partials, no live handle; retry and fallback are correct |
| Worker failure | gateway coordinator | needs compat checks, validation, reset, replay |

**Why finals go direct by default.** A complete utterance looks like a
discrete stateless request — true of the audio, false of the worker. The
pinned worker is holding state built from exactly that audio; a direct
flush finalizes it, while `/v1/audio/transcriptions` is stateless by
construction and throws it away. Measured: **9.2 ms direct flush vs
265.3 ms via Bifrost**, and a different model's text.

And be ready with the honest version of Bifrost's value: of five
advertised features, key rotation and weighted balancing are **inert**
here (no keys, one provider per base_url), health tracking and retry are
**redundant** against a router that already does the harder version — and
retry-with-backoff is actively *wrong* for the online path, which must
never sleep. **Provider fallback for discrete requests is the one feature
doing non-duplicated work**, because `internal/coord` rebuilds streaming
sessions and has no equivalent for a one-shot transcription.

### 1.9 The one known gap, characterised

On one of twelve clips a restored session says `…INFERENCE CALLS SINCE…`
where an uninterrupted one says `…INFERENCE CALLED…`. Isolated by
experiment: splitting audio across two `infer()` calls is **perfectly
transparent at every split point**; only the restore differs, and only
sometimes. That isolates the cause to the one piece of state not carried —
the feature extractor's sample-level position. `OnlineFbank` is continuous
across `accept_waveform`, so a warm extractor and a cold one produce
different frame grids, and near a word boundary that can flip a token.
Priming with 10–100 ms of consumed audio does not fix it, because
`snip_edges=False` re-applies start padding to a fresh extractor.

Say it exactly like that: **11/12 clips restore identically; the twelfth
differs by one word, and the cause is characterised rather than guessed.**
A known, bounded, explained defect reads as engineering. A hidden one
reads as luck.

---

## Part II — transformers and attention

Covers: *The Illustrated Transformer*, Raschka's self-attention article,
and Vaswani et al. You need this to talk about a KV cache at all, because
a KV cache is defined entirely in terms of these objects.

### 2.1 The decoder loop — the picture to draw

```text
token ids → embeddings + positions → [ attention → MLP ] × L → logits
                                                            │
                                         choose next token ─┘
                                         append, repeat
```

A decoder-only LLM is **autoregressive**: it consumes a sequence and emits
a distribution over the next token, samples one, appends it, and runs
again. That loop is the source of every systems problem in Parts III–VI:
one request is not one forward pass, it is *hundreds*, each depending on
the last, each needing every earlier token's contribution.

Two vocabulary points that get tested:

- **Encoder–decoder vs decoder-only.** The 2017 paper is
  encoder–decoder for translation. Modern LLM serving is decoder-only.
  Whisper — relevant here — is genuinely encoder–decoder: the encoder runs
  once over the audio, the decoder autoregresses over text and
  cross-attends to the encoder output.
- **Residual stream.** Each layer reads from and writes back to a running
  hidden vector per position. "Layer" means attention plus MLP plus two
  norms, not attention alone.

### 2.2 Q, K, V — the definitions, said precisely

At each layer, every position's hidden vector `x` is projected three ways
by learned matrices:

| Term | Formal | Plain language |
|---|---|---|
| Query `Q = xW_Q` | what this position is asking for | "what do I need from the past?" |
| Key `K = xW_K` | what a position offers for matching | "here is what I am, for lookup" |
| Value `V = xW_V` | what gets blended once matched | "here is what I contribute" |

The operation itself, the one equation to be able to write:

```text
Attention(Q, K, V) = softmax( (Q Kᵀ) / √d_k + M ) V
```

- `Q Kᵀ` is every query against every key — a similarity matrix.
- `√d_k` scaling: dot products grow with dimension; without it the softmax
  saturates, gradients vanish, and training destabilises. *"It keeps the
  softmax out of its saturated regime"* is the whole answer.
- `M` is the **causal mask**: −∞ at every (i, j) with j > i, so after
  softmax those weights are exactly 0. Position i cannot see the future.
  Without it, training leaks the answer and generation becomes incoherent —
  the model would have learned to depend on tokens that do not exist yet
  at inference time.
- softmax normalises each row to sum to 1. Remember that constraint; it is
  the entire cause of attention sinks in Part V.

**Multi-head** attention runs `h` of these in parallel with `d_k =
d_model / h`, concatenates, and projects by `W_O`. Different heads
specialise — some positional, some syntactic, some copy-like. Cost is the
same as one big head; expressiveness is not.

### 2.3 The one fact the whole KV cache rests on

> Because of the causal mask, position *i*'s key and value at a given layer
> depend only on tokens 0..i. They are **final the moment they are
> computed** and never change as more tokens arrive. The query, by
> contrast, is **consumed at the step that creates it** and never used
> again.

That asymmetry is the cache. K and V are reusable *because attention is
causal*; in a bidirectional encoder (BERT) an earlier position's
representation changes when later tokens arrive, and there is nothing
cacheable. Say that sentence when asked "why K and V but not Q" — the
answer is causality, not convenience.

One refinement for credit: with **RoPE**, the rotary transform is applied
to Q and K before the dot product, so a cached K has its absolute position
baked in. That matters in Part V, where evicting from the front of a
window means positions must be reassigned.

### 2.4 Variants you will be asked to name

| Variant | What it shares | Cache effect |
|---|---|---|
| MHA | nothing — `h` Q heads, `h` KV heads | baseline |
| **GQA** | one KV head per *group* of Q heads | Llama-3-70B: 64 Q heads, 8 KV heads → **8× smaller cache** |
| MQA | one KV head for all Q heads | smallest; some quality cost |
| **MLA** (DeepSeek) | caches a low-rank *latent*, decompressed per head | reported ~70 KB/token vs 192–328 KB/token for comparable GQA models |

All three are **architecture** decisions fixed at training time. The
serving-time knob is quantization (§4.6). Knowing which levers you control
and which you inherit is a good interview beat.

---

## Part III — the KV cache itself

Covers: Raschka's *Coding the KV Cache in LLMs*, the most important single
item on the list.

### 3.1 The derivation, in your own words

```text
Without a KV cache, at generation step t:
  push the entire prefix 0..t-1 through every layer again,
  recomputing K and V for tokens whose K and V cannot have changed

With a KV cache:
  retain K/V for positions 0..t-1, per layer
  project ONLY the new token → q_t, k_t, v_t
  append k_t, v_t to the layer's cache
  attend q_t over [K_cache ; k_t], [V_cache ; v_t]
```

The saving is **repeated prefix projection and MLP work**. State it in
complexity terms, because that is what separates a memorised answer from
an understood one:

| | per decode step | over N tokens |
|---|---|---|
| no cache | O(t · d²) projections + O(t² · d) attention | **O(N³·d) attention, O(N²·d²) projection** |
| with cache | O(d²) projections + O(t · d) attention | O(N·d²) projection + **O(N²·d) attention** |

The headline: the cache turns the dominant per-step cost from *linear in
prefix length* to *constant*, leaving only the attention read over the
cache growing with context. It does **not** make generation O(1) — you
still read the whole cache every step, which is why long contexts remain
expensive even cached, and why Part V exists.

### 3.2 The size formula, and a worked number

```text
bytes_per_token = 2 × layers × kv_heads × head_dim × dtype_bytes
```

The leading 2 is K and V. Worked, for Llama-3-70B (80 layers, 8 KV heads
after GQA, head_dim 128, fp16):

```text
2 × 80 × 8 × 128 × 2  =  327,680 B  ≈  320 KB per token
8k context            ≈  2.6 GB for ONE sequence
```

On an 80 GB H100 holding a 70B model's weights, **the cache — not the
weights — is what decides how many users fit.** That is the sentence the
formula exists to support.

In an interview, **state the assumptions before quoting a number**: exact
size moves with GQA/MQA, quantization, tensor parallelism (each rank holds
its shard), sliding windows, and architecture-specific extras.

### 3.3 Prefill vs decode — the distinction everything else hangs off

| | Prefill | Decode |
|---|---|---|
| Work | whole prompt at once | one token per step, per sequence |
| Shape | big GEMMs, highly parallel | skinny GEMV-like ops |
| Bound by | **compute** | **memory bandwidth** |
| FLOPs | ≈ 2 · P · T (P params, T prompt tokens) | ≈ 2 · P per token |
| Produces | the cache | reads the cache, appends one entry |
| Metric | **TTFT** | **TPOT** / inter-token latency |

The arithmetic that makes decode bandwidth-bound is worth being able to do
aloud: a decode step must read *every weight* from HBM to produce *one*
token per sequence. A 70B model in fp16 is ~140 GB; an H100 SXM has
~3.35 TB/s, so ~24 forward passes per second — **~24 tokens/s per sequence
as a hardware ceiling, almost independent of how fast the GPU's math
is.** Batching is what fixes this: the weight read is amortised across
every sequence in the batch, so throughput scales with batch size while
per-sequence latency barely moves. That is the *entire economic motivation*
for Part IV — and the reason KV cache memory, which caps batch size,
is the binding constraint.

### 3.4 The three things called "cache", never to be confused

```text
live session cache:  state for ONE request continuing over time
prefix cache:        reusable COMPLETED prefix blocks across matching requests
response cache:      the final output, reused for a matching request
```

- A **response cache** answers *"have I already answered this exact
  request?"*
- A **KV cache** answers *"what state is the model in after this exact
  ordered history?"*

No amount of configuration turns the first into the second. This
distinction is the backbone of the repo's rejection of "just put a
semantic cache in front of it" — and of drill 7.

### 3.5 Why compatibility is stricter than "same model name"

The bytes are raw numerical state whose meaning depends on architecture,
weights **revision**, layer count and dims, precision, position scheme,
cache layout, runtime version and device/runtime ABI. A cache from the
wrong configuration is not a cache miss — **consuming it is unsafe**: it
deserialises, the shapes may even match, and you get silently wrong output
rather than an error.

This repo's evidence is the fleet table: `worker-d` and `worker-e` run
**identical weights** (`whisper-tiny.en`) under different runtimes and
hold **different compatibility keys**. Same model name, incompatible
state. That is the example to give, and it is why the key is checked
*before* deserialization.

---

## Part IV — scheduling and memory management

Covers: ORCA, the Anyscale explainer, PagedAttention/vLLM, and automatic
prefix caching.

### 4.1 ORCA (OSDI 2022) — iteration-level scheduling

**Problem.** Existing servers schedule whole requests. Since a generative
request takes many iterations, a request that finishes early cannot return
to the client, and an arriving request must wait for the whole batch.

```text
static:      [A B C] → wait for A, B AND C → [D E]
continuous:  [A B C] → C ends → [A B D] → A ends → [B D E]
```

**Two contributions, and naming both is the differentiator:**

1. **Iteration-level scheduling** — the scheduler returns control after
   *each token step*, so finished requests leave and queued ones join at
   the next iteration.
2. **Selective batching** — you cannot naively batch requests with
   different sequence lengths and different KV state. ORCA flattens tokens
   into one tensor for the shape-agnostic operations (Linear, LayerNorm,
   activation) and splits the batch to run **attention per request**, then
   merges. Batch the ops that can be batched; don't pretend attention is
   one of them.

**Result:** up to **36.9× throughput over FasterTransformer at the same
latency level**.

**What it does not solve:** memory. ORCA still reserves contiguous
per-request KV slots sized to the maximum length — the exact waste
PagedAttention attacks a year later. Knowing that ORCA *creates* the
memory-management problem by admitting more concurrent sequences is the
clean way to motivate vLLM.

**Costs it introduces:** scheduler complexity, per-request latency
variance, memory pressure, admission/fairness policy, and preemption — all
become first-class concerns.

### 4.2 The Anyscale explainer — and how to quote it

The practical companion: continuous batching plus PagedAttention gave
**up to 23× throughput vs naive static batching**, measured on OPT-13B on
a single 40 GB A100, at maximum variance in generation length (up to 1536
tokens).

**Never quote 23× as a general fact.** The caveat *is* the insight: the
benefit is a function of **variance in output length**. With uniform
lengths, static and continuous batching perform similarly — there is no
early finisher to make room. The mechanism is "requests can leave the
batch early"; if none do, there is nothing to gain. Say the mechanism,
then say results depend on workload, model, hardware and SLO.

Vocabulary to have crisp:

| Term | Meaning |
|---|---|
| Request batching | group requests, run the batch to completion |
| Continuous batching | rebuild the active batch at iteration boundaries |
| Prefill | process prompt tokens; compute-heavy, parallel |
| Decode | one token per iteration; bandwidth- and KV-sensitive |
| **Chunked prefill** | split a long prompt across scheduling iterations so it cannot stall everyone's decode — head-of-line blocking control |

### 4.3 PagedAttention / vLLM (SOSP 2023)

**The central problem is not attention math; it is allocation.** The
original sin: one contiguous buffer per sequence, sized to
`max_model_len`. A request generating 100 tokens against a 4096-token
reservation wastes 97% of it, and the leftovers fragment. Prior systems
were reported to use only a fraction of KV memory for actual token state.

Three wastes, worth naming separately:

- **Reservation waste** — space held for tokens not yet generated.
- **Internal fragmentation** — over-allocation to the maximum length that
  is never used.
- **External fragmentation** — unusable gaps between differently sized
  contiguous allocations.

**The answer is the OS answer: paging.**

```text
logical KV blocks:   L0 → L1 → L2 → L3
physical GPU blocks: P8   P2   P19   P5
block table maps logical → non-contiguous physical
```

- Fixed-size blocks, **typically 16 tokens**, each holding that block's K
  and V for every layer and head.
- **Allocated on demand** — a block is claimed when the 17th token needs
  it, not at admission.
- Physical blocks need not be adjacent, which is why this needed a
  **custom attention kernel** that gathers across the block table, not
  just a new allocator.
- **Waste is bounded by one partially filled block per sequence.**
- **Sharing becomes possible** — two sequences can point at the same
  physical block, with reference counts and copy-on-write when one
  diverges. That is what makes parallel sampling and beam search cheap.

**Result:** **2–4× throughput over FasterTransformer and ORCA at
comparable latency**, more pronounced with longer sequences, larger
models, and more complex decoding algorithms.

**Do not say:** "PagedAttention makes attention sparse" (it does not
change what is attended to — only where it lives), or "it keeps one shared
global cache for all requests."

**What it does not solve:** it does not reduce the *total* KV bytes for a
single sequence, does not make long contexts cheap to attend over, and
does not decide *which* worker a request should land on. That last one is
Part VI's problem and this repo's problem.

### 4.4 Automatic prefix caching — identity, sharing, eviction

Paging makes blocks shareable. Prefix caching decides *when* two blocks
are the same thing.

**Chained hashing.** A block's hash is computed from:

1. the **hash of the preceding block**,
2. the **token IDs in this block**, and
3. **extra keys** — LoRA ID, multimodal embedding references, and an
   optional **cache salt**. (SHA-256 is the default in current vLLM.)

The chain is the whole trick. Hashing a block's own contents would make
any two blocks with the same 16 tokens interchangeable — wrong, because
attention is over the *entire* preceding context. Chaining encodes "same
content **and** same history" in a single comparison. This is drill 1's
sibling and a favourite interview question.

**Only full blocks are cached.** A partially filled tail block has no
stable hash, so **cache-hit granularity is the block size**: with 16-token
blocks, a 15-token shared prefix gets you exactly nothing.

**Eviction is two-stage.**

1. **Reference counting** — a block becomes a candidate only at
   `ref_cnt == 0`. That is resource management, not caching.
2. **LRU over a free queue** — freed blocks keep their hash and go onto a
   doubly linked free queue (O(1) removal and reclaim); eviction takes
   from the head.

The subtle bit, and excellent material: freed blocks are appended **in
reverse order**. A later block in a sequence hashes more tokens, so it is
more specific, less likely to be reused, and should die first. Earlier
blocks — the shared system prompt — are the general ones and survive
longest. *A cache policy encoded as a list insertion order.*

**SGLang's RadixAttention** is the same job with a different structure: a
radix tree whose edges are token *sequences*, walked to find the longest
cached prefix. Hash map vs radix tree is a genuine trade — O(1) exact
lookup versus cheap longest-prefix matching and an eviction order that
follows the sharing structure by construction (only leaves are evictable,
since an interior node is by definition a prefix of something resident).

### 4.5 Prefix caching is a security decision

The part most candidates miss, and worth volunteering.

Cache hits are **faster**, and speed is observable. In a multi-tenant
deployment an attacker can measure TTFT to learn whether a guessed prefix
is already resident — i.e. whether *someone else* submitted it. This is
tracked as **CVE-2025-46570**, and published analysis reports the signal
is nearly perfectly distinguishable (**ROC AUC 0.99**) at prefix lengths
of just **8 tokens**.

The mitigation is the **cache salt**: a secret value injected into the
first block's hash, so only requests carrying the same salt can reuse
those blocks. Guidance: treat it as confidential, use cryptographically
random values (e.g. 256 bits), and scope it to your tenant boundary — per
user for isolation, per group for deliberate sharing. And state the cost
honestly: **salting reduces hit rate**, because it partitions the cache.
Single-tenant deployments should omit it.

The general lesson, which is the transferable one: **cache reuse is an
isolation decision, not only a performance feature.** Any cross-request
cache is a side channel until proven otherwise.

### 4.6 Making the cache smaller, and routing to it

Two more levers, because a strong answer names them:

**Quantization** is the serving-time knob you actually control.
`--kv-cache-dtype fp8` halves bytes per token and halves memory traffic
per attention step; with an FP8-capable attention backend the math runs in
FP8 rather than dequantizing first. The non-obvious result: 4-bit has been
reported to give a **higher hit rate than BF16 under a fixed HBM+DRAM
budget (86.8% vs 75.2%)** — because smaller entries mean more of them fit.
**Quantization is a hit-rate lever, not only a memory saving.** Caveat:
quality is model- and kernel-path-dependent; validate per model.

This repo has the matching measurement: 12 clips, checkpoint at midpoint,
restore, compare — fp32 11/12 identical at 1.10 MB, fp16 11/12 at
0.56 MB, **int8 11/12 at 0.28 MB**, encode cost 0.3–0.6 ms. The single
non-identical clip is the *same* clip at all three widths including
lossless fp32, so quantization is not the cause. **On this model the cache
quantizes to int8 for free.** Integer bookkeeping tensors are never
quantized — scaling a counter corrupts rather than blurs.

**Cache-aware routing.** Once cache lives on a specific worker, the load
balancer becomes part of the cache system. Round-robin is actively
harmful: it scatters prefix-sharing requests so every worker builds and
pays for its own copy. SGLang's router keeps an approximate radix tree of
what it believes each worker holds (reported up to **1.9× throughput and
3.8× higher hit rate**); llm-d goes exact by subscribing to **KV-block
events emitted by the engines**, scoring endpoints on true resident-block
fraction and breaking ties on queued prefill work.

**This is exactly what `internal/router` is** — pin the session to the
worker holding its state, prefer a matching compatibility key on failover.
Different domain, identical invariant: *send the work where the state
already is, because the alternative is recomputing it.*

---

## Part V — unbounded streams and attention sinks

Covers: StreamingLLM (Xiao et al., 2023).

### 5.1 The problem

Two obstacles to running an LLM over an unbounded stream (multi-round
dialogue, live captioning): the KV cache grows without bound, and models
cannot generalise beyond their training sequence length.

The obvious fix is **window attention** — keep only the most recent N
tokens. It does not degrade gracefully. **It collapses**: perplexity
explodes the moment the window slides past the *first few tokens*.

### 5.2 The observation — attention sinks

Models dump enormous attention weight onto the first few tokens
**regardless of their content**. The mechanism is the softmax constraint
from §2.2: the row must sum to 1, so the model needs somewhere to park
attention it does not want to use, and the initial tokens — visible to
every subsequent position, so always available — become that dump.

Evict them and every later softmax is renormalised over what remains,
which is garbage. That is why the failure is a cliff, not a slope.

### 5.3 The fix, and why it is so small

**Keep the first ~4 tokens permanently** as sinks, slide the window over
everything else. No fine-tuning required. Reported results: stable
language modelling on Llama-2, MPT, Falcon and Pythia over **4M+ tokens**,
and **up to 22.2× speedup** over the sliding-window-with-recomputation
baseline.

One mechanical detail worth knowing, because it shows you read past the
abstract: positions are assigned **by position within the cache, not
position in the original text**. With RoPE, the keys are cached before the
rotary transform and rotated at attention time according to their cache
index — otherwise the sinks and the window would carry a nonsensical
positional gap between them.

Extra credit: the paper also finds that **pre-training with a dedicated
sink token** improves streaming deployment further — which is why explicit
learned attention sinks now appear in production model architectures.

### 5.4 How this maps here — and the trap

```text
LLM:  trained with DENSE attention over a prefix, then a window is
      imposed at inference  →  train/serve mismatch  →  collapse
      →  needs sinks as a repair

zipformer: trained AND served with the same bounded left context
      (left_context_len 64/32/16/8/32, fixed per stack)
      →  no mismatch  →  no sink needed
```

That contrast is the sophisticated answer. The state is exactly 1.09 MB at
chunk 1 and at chunk 900, because the window is a property of the exported
model rather than a policy applied on top of it.

**The trap on the list, spelled out:** do **not** claim the audio journal
is an attention-sink implementation. The journal is a bounded ring of raw
audio for *replay*; sinks are retained KV entries that keep *softmax
normalisation* sane. Different layer, different mechanism, different
failure mode. The honest common ground is one sentence: **an unbounded
stream needs an explicit, designed retention policy, and the obvious
policy is wrong.** This repo answers it with a bounded journal plus
trim-on-final; StreamingLLM answers it with sinks plus a rolling window.

---

## Part VI — disaggregation and Mooncake

Covers: Mooncake (Qin et al., 2024), the serving platform behind Moonshot
AI's Kimi.

### 6.1 Why disaggregate at all

From §3.3: prefill is compute-bound and parallel; decode is
bandwidth-bound and serial. Co-locating them means a long prefill stalls
everyone's decode steps — head-of-line blocking that surfaces as p99
inter-token latency. **PD disaggregation** puts them in separate pools:
the prefill cluster computes the KV cache, ships it, and the decode
cluster generates. This is now the standard shape across vLLM, SGLang,
TensorRT-LLM, LMDeploy and NVIDIA Dynamo.

### 6.2 What Mooncake actually is

A **KVCache-centric disaggregated architecture**:

| Component | Job |
|---|---|
| Prefill / decode clusters | separated, scaled independently |
| **Disaggregated KVCache pool** | uses the **underutilised CPU, DRAM and SSD of the GPU cluster** as a distributed cache |
| **Transfer Engine** | high-performance movement (RDMA/InfiniBand, RoCE, TCP, NVMe-oF, object storage) |
| **KVCache-centric scheduler** | maximises effective throughput subject to latency SLOs — placement and reuse are scheduling inputs, not afterthoughts |
| **Prediction-based early rejection** | under overload, refuse a request *before* spending prefill on it, predicting whether it can meet its SLO |

Numbers: **up to 525% throughput increase in certain simulated
long-context scenarios while meeting SLOs**, and under real workloads
Kimi handles **75% more requests**.

Two details that mark a careful reader: the early-rejection policy exists
because the paper explicitly *does not* assume all requests are processed
— overload is the design point, not an edge case; and rejecting on current
load alone causes oscillation, which is why the rejection is
prediction-based.

**The crucial ownership fact:** the **model runtime**, not Mooncake, knows
how to allocate, name, validate and consume KV blocks. Mooncake moves and
stores bytes that the runtime defines. This is exactly the division this
repo draws between `cmd/kvtier` (stores opaque blobs, never parses one)
and the adapter (owns format and compatibility identity).

### 6.3 Why it is not integrated here — the answer to memorise

> Mooncake is not integrated. This repo's real ASR adapters hold
> runtime-owned streaming state that they do not serialize. The gateway
> can therefore move audio and policy, not live state. Its safe recovery
> path is replay; a Mooncake integration would first require
> adapter-owned state export/import, exact compatibility identity,
> distributed fencing, and a demonstrated transfer-cost benefit.

Then the elaboration, if they push. A real integration needs **all** of:

| # | Requirement | Status here |
|---|---|---|
| 1 | adapter-owned export/import of the *entire* live state (process memory is not a state format) | **done** for the three `_kv` adapters |
| 2 | exact compatibility identity — weights, runtime build, decoder config, dtype, device ABI, cache schema | **done** — 7-field `CompatKey`, hashed |
| 3 | **prefix identity** — proof of *which audio sequence and endpointing state* a snapshot represents | not done |
| 4 | distributed ownership transfer that **fences the old writer** before the target imports | not done — fencing is per-worker CAS, not distributed |
| 5 | replay fallback on every transfer error and incompatible route | **done** — 422/424 → replay |
| 6 | retention and privacy policy for state derived from a caller's audio | not done |
| 7 | measured benefit: transfer + validate + import < replay | **done** — §1.7 |

And the domain mismatch, which is the deeper point: **streaming ASR does
not map onto prefill/decode.** Audio arrives continuously, partials have a
latency budget, and endpointing can reset an utterance at any moment. Its
natural unit is *stateful session ownership*, not a two-phase pipeline.

Note the honest framing the repo uses: compatibility says a worker **may
interpret** these bytes; it does not say **which audio** they represent.
That gap — item 3 — is the real one, and naming it unprompted is worth
more than any amount of Mooncake trivia.

### 6.4 The honest scorecard for this repo

| Mechanism | Production LLM serving | Here |
|---|---|---|
| Paged allocation | PagedAttention blocks + block table | **none** — whole-state snapshots |
| Prefix identity | chained block hash / radix tree | **none** — no cross-session reuse |
| Eviction | ref-count → LRU free queue | tier-level LRU + explicit retirement |
| Sharing | copy-on-write blocks across requests | **none** — one session, one state |
| Offload tiers | HBM → DRAM → SSD (LMCache) | one in-memory tier + worker hot cache |
| Shrinking | GQA / MLA / FP8 / 4-bit | **int8 quantization, measured free** |
| Transfer | NIXL, Mooncake, PD disaggregation | HTTP + safetensors, 141× audio bytes |
| Cache-aware routing | SGLang router, llm-d | **yes** — pin + prefer-by-key |
| Recovery when the holder dies | recompute prefill | **yes** — audio journal replay |
| Bounded retention for long streams | attention sinks + rolling window | **yes** — bounded ring + trim on final |

**The summary sentence: this repository implements the
distributed-systems half and little of the memory-management half.** It
knows where state is, keeps work near it, detects when the holder dies,
and rebuilds deterministically. It does not allocate, share or page a
cache — and for the wrapper-based adapters it *cannot*, because
sherpa-onnx and CTranslate2 expose no handle to their internals. That is a
defensible scope, stated as a scope rather than discovered as a gap.

### 6.5 Why cross-session sharing is not applicable (and paging is not either)

Two "why didn't you do X" answers that are *structural*, not excuses:

- **Token-level paging.** vLLM block tables exist because an LLM cache is
  per-token, append-only, and of unpredictable length. This cache is a
  **fixed-size sliding window per layer** — 1.09 MB at chunk 1 and at
  chunk 900. No fragmentation to solve, no variable length to page. A
  block table here would be a constant-size array of one entry: the form
  of the design with none of its motivating property.
- **Cross-session prefix sharing.** An LLM shares a system prompt across
  thousands of requests. Two ASR sessions carry *different audio*; their
  encoder states diverge from the first frame. Sharing would only pay for
  *identical* audio — which is a response cache, not a KV cache.
  Content-addressing would still buy deduplication of retries and
  idempotent replays, but that is a much smaller prize.

---

## Part VII — the fifteen drills, with answers

Two minutes each, out loud. The answers below are the *spine*; say them in
your own words and stop when you have made the point.

**1. What are Q, K and V? Why can K/V be cached during decode?**
Per layer, each position projects its hidden vector into a query (what I
am looking for), a key (what I offer for matching) and a value (what I
contribute once matched). Attention is `softmax(QKᵀ/√d_k + mask)V`.
Because attention is causal, position i's K and V depend only on tokens
0..i, so they are final once computed and never change. The query is
consumed at the step that creates it. That asymmetry is the cache — and it
exists because of causality, which is why a bidirectional encoder has
nothing to cache.

**2. The time-versus-memory trade-off.**
You trade recomputation for residency. Without a cache, every decode step
re-projects the whole prefix: O(t·d²) per step. With it, projection is
O(d²) per step and you keep 2 × layers × kv_heads × head_dim ×
dtype_bytes per token — 320 KB/token on Llama-3-70B, ~2.6 GB for one 8k
sequence. So the cost of the cache is *batch capacity*: cache memory, not
weights, is what decides concurrency, and everything in vLLM/Mooncake is
downstream of that.

**3. Why is compatibility stricter than "same model name"?**
The bytes are raw numerical state whose meaning depends on architecture,
weights revision, layer count and dims, dtype, position scheme, cache
layout, runtime version and device ABI. A mismatch is not a miss —
consuming it can deserialize cleanly and produce silently wrong output.
Concretely, two workers here run identical `whisper-tiny.en` weights under
different runtimes and hold different compatibility keys. So we hash seven
fields into an opaque key and check it *before* deserialization; mismatch
is a 422 naming both keys, never a coercion.

**4. Prefill, decode, TTFT, TPOT, end-to-end.**
Prefill processes the prompt in one compute-bound pass and produces the
cache; TTFT is queue + prefill. Decode generates one token per iteration
per sequence, is bandwidth-bound because it re-reads all weights per step,
and TPOT (or inter-token latency) is its metric. End-to-end ≈ TTFT +
TPOT × output length. They are optimised by opposite means — batching and
chunked prefill for TTFT protection, batch size and smaller KV for TPOT —
which is why disaggregating them is attractive.

**5. Why does continuous batching increase utilization? What does it
complicate?**
It schedules at iteration granularity, so a finished request leaves the
batch immediately and a queued one joins at the next token step, instead
of the whole batch waiting for its slowest member. ORCA reports up to
36.9× over FasterTransformer; Anyscale measured up to 23× including
PagedAttention — but the gain is a function of *variance in output length*,
and with uniform lengths it is close to nil. It complicates everything
policy-shaped: admission, fairness, preemption, memory pressure, and
per-request latency variance. Also, attention cannot be batched naively
across different lengths — ORCA's selective batching flattens the
shape-agnostic ops and runs attention per request.

**6. What memory problem does PagedAttention solve, and what does it not?**
It solves allocation: contiguous per-sequence buffers sized to
`max_model_len` waste memory to reservation, internal and external
fragmentation. It carves KV memory into fixed 16-token blocks addressed by
a per-sequence block table, allocates on demand, bounds waste to one
partial block per sequence, and makes blocks shareable with ref-counting
and copy-on-write — 2–4× throughput over FasterTransformer and ORCA. It
does **not** reduce the bytes a single sequence needs, does not make
attention sparse or cheaper per token, and does not decide which *worker*
should serve a request.

**7. Prefix cache vs response cache vs live session cache.**
A response cache stores a finished output keyed by the request — it
answers "have I answered this before?". A prefix cache stores *completed,
full KV blocks* for a shared prompt prefix and is reused across different
requests that share that prefix — it answers "have I computed this
prefix before?". A live session cache is the state of one in-flight
request continuing over time. Only the last one is what a streaming
session needs, and it is why a semantic/response cache in front of a
streaming path is not a substitute for anything.

**8. How can prefix caching leak across tenants?**
A hit is faster, and timing is observable, so TTFT reveals whether a
guessed prefix is already resident — i.e. whether another tenant submitted
it. That is CVE-2025-46570; published analysis puts it near-perfectly
distinguishable (ROC AUC 0.99) at 8-token prefixes. The mitigation is a
cache salt injected into the first block's hash so only requests with the
same salt can share blocks — treated as a secret, cryptographically
random, scoped to the tenant boundary. It costs hit rate, which is the
honest trade: cache reuse is an isolation decision, not just a performance
feature.

**9. What is an attention sink, and why is a naive sliding window
unreliable?**
Softmax rows must sum to 1, so a model parks unwanted attention mass on
the first few tokens regardless of content — they are visible to every
position and semantically inert. A naive window eventually evicts them,
and every later softmax renormalises over what is left, so quality
collapses rather than degrades. StreamingLLM keeps ~4 initial tokens
permanently and slides over the rest, assigning positions by index within
the cache: stable over 4M+ tokens with no fine-tuning, up to 22.2× faster
than sliding-window recomputation.

**10. What does Mooncake disaggregate, and where does the cache live?**
It separates prefill and decode into different clusters and puts the KV
cache in a disaggregated pool built from the cluster's underutilised CPU,
DRAM and SSD, moved by a Transfer Engine over RDMA and friends. A
KVCache-centric scheduler places work with reuse and SLOs in mind, plus
prediction-based early rejection for overload. Reported up to 525%
throughput in simulated long-context scenarios and 75% more requests in
production for Kimi. Crucially the *runtime* owns block format and
validity; Mooncake moves and stores bytes it does not interpret.

**11. Why is Mooncake not a drop-in for an ASR worker's memory?**
Because "copy a live state object elsewhere and call infer() as though
nothing happened" is not a thing you can do. The four real adapters don't
serialize their state at all — it's a C++-owned object. Even with
serialization you need exact compatibility identity, *prefix* identity
proving which audio a snapshot represents, distributed fencing of the old
writer before import, a replay fallback for every transfer error, a
retention policy for state derived from a caller's audio, and a measured
win over replay. And streaming ASR doesn't map onto prefill/decode:
continuous arrival, a latency budget on partials, endpointing that can
reset an utterance at any moment. The natural unit is stateful session
ownership.

**12. Where is live state, where is replay, what survives a crash?**
Live state is in the worker process: `StateStore` maps an opaque handle to
`model_state`, and it dies with the worker — that is the point. The
gateway holds session metadata, the compatibility key, a bounded audio
journal (1500 records ≈ 30 s) and, on the mock adapter only, a checkpoint
blob. Nothing is persisted anywhere; the answer to a gateway dying is
client-side replay from its own ack ring. On a worker crash: pick a
replacement preferring the same key, try restore if a valid checkpoint
exists, otherwise open fresh and replay from the last committed final,
always emitting `partial.reset` then re-emitting the replayed text.

**13. Why both sequence numbers and a generation token?**
They answer different questions. The sequence says how far through the
audio a state is, and makes replay idempotent — `seq_end <=
last_seq_applied` returns the cached text without re-inferring, which is
what makes replay safe to call freely. The generation says which *version*
of the state you are overwriting, enforced by compare-and-commit under a
lock: mismatch is rejected rather than applied over newer state. Seq alone
cannot tell a retry from a second writer; generation alone cannot make
replay idempotent. We also keep stream and failover epochs separately so a
client reconnect is never confused with a backend change.

**14. Why must Bifrost not route individual stateful chunks?**
Because `push` names a worker-local handle. If chunk N lands on worker-a
and N+1 on worker-b, the outcomes are: b rejects the handle and the load
balancer now has to implement the gateway's recovery protocol; or b
creates fresh state and silently drops N's context; or it falls back to an
incompatible worker with no `partial.reset` and no replay. The last two
are transcript corruption, not latency failures. Passing the handle
through does not help — it names a map entry, not a portable object. What
*does* work is inverting it: the request carries a *reference*
(`state_ref`/`state_sink`) to state in a shared tier, the bytes move on a
side channel the balancer never sees, and affinity becomes an optimization
worth 1.88× rather than a correctness requirement.

**15. Design a safe state-transfer protocol — minimum validation and
fallback.**
Seven steps. (1) Enumerate the state — for us it was four things, and
three weren't the tensors: KV, feature seam, decode hypothesis, extractor
position. (2) Define a compatibility identity covering weights revision,
runtime build, dtype, device ABI and cache schema; hash it and ship it
*with* the blob. (3) Name the destination version up front so the response
carries no state, and make versions immutable so a retry finds its input
intact — create-only writes, so two attempts at one chunk can't both land.
(4) Fence: the old writer must be excluded before the new one imports;
compare-and-commit on a generation, and validate before deserializing —
mismatch is a refusal naming both keys, never a coercion, never a partial
restore. (5) Use a format that cannot execute — safetensors, not pickle,
because these bytes cross process boundaries. (6) Every failure degrades
to the deterministic rebuild path; that path must always exist and always
be cheap enough to use. (7) Measure that transfer plus validation plus
import beats the rebuild — otherwise you built a slower correctness risk.

### The design-answer structure

When asked to design or critique an inference system, answer in this
order. It reads as experience because it front-loads the constraints:

1. **Targets first** — latency SLO, throughput/concurrency, and which one
   you're willing to sacrifice.
2. **Separate the four state classes** — immutable weights, live
   state/KV, input journal, final response cache. Most confusion in this
   area is two of these being called the same word.
3. **Ownership and fencing** for live state — who may write, how a stale
   writer is excluded.
4. **Compatibility and serialization**, before any transfer is proposed.
5. **Allocation, admission, eviction and isolation** policy — including
   whether cache reuse crosses a tenant boundary.
6. **Failure behaviour and the correctness fallback** — and be explicit
   that the fallback must always be available.
7. **The measurements** — TTFT, TPOT, p50/p95/p99, cache hit rate, memory
   utilization and fragmentation, queue time, recovery time, error rate.

---

## Part VIII — traps, numbers, and the review card

### 8.1 Sentences that cost you the interview

| Don't say | Say instead |
|---|---|
| "The gateway holds the KV cache." | "The gateway holds the *reference*, the compatibility key, and the audio to rebuild it." |
| "PagedAttention makes attention sparse." | "It's virtual memory for the KV cache — blocks plus a block table, and a kernel that gathers across it." |
| "Continuous batching gives 23× throughput." | "23× was OPT-13B on one A100 at maximum length variance; the mechanism is early departure from the batch, and with uniform lengths it's ~nil." |
| "We use Mooncake / it's basically Mooncake." | "Same invariant — reference in the request, bytes on a side channel — but Mooncake is paged, sharded and RDMA; ours is whole-state, one process, HTTP." |
| "The audio journal is an attention sink." | "Both bound an unbounded stream; the mechanisms and failure modes are unrelated." |
| "A semantic cache can serve streaming chunks." | "A response cache answers 'have I answered this?'; a KV cache answers 'what state am I in after this exact history?'" |
| "`state_bytes` is 0 because we didn't measure." | "It was 0 because a C++-owned object can't be sized honestly; the `_kv` adapters now report 2.19 MB for two live sessions." |
| "Restores work." | "11 of 12 clips restore byte-identically; the twelfth differs by one word, and the cause is the extractor's sample position — characterised, not guessed." |

### 8.2 Numbers worth having cold

**External:**

| Fact | Number |
|---|---|
| Llama-3-70B KV cache | 320 KB/token → ~2.6 GB at 8k |
| ORCA vs FasterTransformer | up to 36.9× |
| vLLM vs FasterTransformer/ORCA | 2–4× |
| Anyscale end-to-end | up to 23× (OPT-13B, 1×A100-40GB, max variance) |
| vLLM block size | typically 16 tokens |
| StreamingLLM | ~4 sink tokens, 4M+ tokens stable, up to 22.2× |
| Mooncake | up to 525% simulated; 75% more requests in production |
| Prefix-cache side channel | CVE-2025-46570; ROC AUC 0.99 at 8 tokens |
| GQA on Llama-3-70B | 64 Q heads / 8 KV heads = 8× smaller cache |

**This repo:**

| Fact | Number |
|---|---|
| State per session | zipformer 1.09 MB / conformer 2.72 MB / whisper 5.51 MB |
| zipformer split | 467 KB attention K/V, 614 KB convolution state |
| Quantization | int8 = 0.28 MB, 11/12 identical, ~free |
| Stateless cost | 141× audio bytes measured vs 137× predicted |
| Locality | 1.88× (113.6 ms alternate → 60.4 ms sticky) |
| Version retirement | 87% LRU-evicted → 0; 268 MB → 46 MB peak |
| Recompute-statelessness | `N/2` multiplier; diverges past 16.0 s (zipformer), 7.8 s (conformer), 2.1 s (whisper_ct2) |
| Direct flush vs re-transcription | 9.2 ms vs 265.3 ms |
| Steady state | chunk round trip p50 20.0 ms / p95 20.9 ms; first partial 187 ms; session open 6.5 ms |
| Journal | ring of 1500 records ≈ 30 s, wraps rather than grows |
| The cache's price | ~1,400 lines across `router`, `coord`, `journal` |

### 8.3 The final card

```text
KV cache = per-layer K/V of an already-processed prefix. It removes
repeated prefix projection, and grows with active context. Causality is
why K/V are reusable and Q is not.

bytes/token = 2 × layers × kv_heads × head_dim × dtype_bytes

Prefill  compute-bound, makes the cache, measured by TTFT
Decode   bandwidth-bound, reads the cache, measured by TPOT

ORCA           schedule at iterations, not requests (+ selective batching)
PagedAttention KV in fixed blocks via a block table; virtual memory
Prefix caching chained hash over full blocks; reuse across requests
Cache salt     because a hit is observable — isolation, not just speed
StreamingLLM   keep ~4 sinks + a window; softmax must sum to 1
Mooncake       PD disaggregation + distributed KV pool; runtime owns format
Cache routing  send work where the state is, or pay to rebuild it

This repo ≠ Mooncake:
  worker owns opaque live ASR state; gateway owns journal + recovery
  compatibility checked BEFORE deserialization; refuse, never coerce
  versions immutable, retired on successor durability, safetensors only
  replay is always the safety net — every other path is an optimization
  affinity: REQUIRED when pinned, PREFERRED when state is referenced
```

---

## Sources

Primary, with the claims taken from each:

- [Vaswani et al., *Attention Is All You Need*](https://arxiv.org/abs/1706.03762) — scaled dot-product attention, multi-head, masking.
- [Alammar, *The Illustrated Transformer*](https://jalammar.github.io/illustrated-transformer/) — architecture vocabulary.
- [Raschka, *Understanding and Coding Self-Attention*](https://magazine.sebastianraschka.com/p/understanding-and-coding-self-attention) — Q/K/V mechanics.
- [Raschka, *Coding the KV Cache in LLMs*](https://magazine.sebastianraschka.com/p/coding-the-kv-cache-in-llms) — the derivation in §3.1.
- [Yu et al., *ORCA* (OSDI 2022)](https://www.usenix.org/conference/osdi22/presentation/yu) — iteration-level scheduling, selective batching, 36.9×.
- [Anyscale, *Continuous batching*](https://www.anyscale.com/blog/continuous-batching-llm-inference) — 23×, OPT-13B/A100, the variance caveat.
- [Kwon et al., *PagedAttention* (SOSP 2023)](https://arxiv.org/abs/2309.06180) — blocks, block tables, 2–4×.
- [vLLM, *Automatic Prefix Caching*](https://docs.vllm.ai/en/latest/design/prefix_caching.html) — chained hashes, full blocks only, reverse free-queue insertion.
- [vLLM, *Security*](https://github.com/vllm-project/vllm/blob/main/docs/usage/security.md) — cache salting, CVE-2025-46570.
- [Xiao et al., *StreamingLLM*](https://arxiv.org/abs/2309.17453) — attention sinks, 4M tokens, 22.2×.
- [Qin et al., *Mooncake*](https://arxiv.org/abs/2407.00079) and [docs](https://kvcache-ai.github.io/Mooncake/) — disaggregation, 525%, 75%.

Local, for everything in Part I:
[KVCACHE.md](KVCACHE.md) ·
[KVCACHE-ALTERNATIVES.md](KVCACHE-ALTERNATIVES.md) ·
[KVCACHE-DEEPDIVE.md](KVCACHE-DEEPDIVE.md) ·
[ARCHITECTURE.md](ARCHITECTURE.md) ·
[`internal/session/state.go`](../internal/session/state.go) ·
[`internal/coord/failover.go`](../internal/coord/failover.go) ·
[`worker/state.py`](../worker/state.py) ·
[`worker/adapters/base.py`](../worker/adapters/base.py)
