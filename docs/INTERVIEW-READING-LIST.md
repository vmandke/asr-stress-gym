# Interview reading list: transformers, KV cache, and inference systems

This is a preparation guide for discussing this repository, KV caches, vLLM,
Mooncake, Bifrost, and stateful inference clearly in an interview. It is
ordered: do not start with Mooncake or a systems paper before you can explain
what a key/value tensor is and why it grows.

**If you have no time to read the sources themselves**, every item below is
worked through — mechanism, numbers, and the answer to say out loud — in
[INTERVIEW-DEEPDIVE.md](INTERVIEW-DEEPDIVE.md), including model answers to
all fifteen drills in Part VII.

## Quick navigation

- [Repository state and recovery](#part-i--understand-this-repository-first-4560-min)
- [Transformer and attention fundamentals](#part-ii--transformer-and-attention-fundamentals-75120-min)
- [KV cache fundamentals](#part-iii--kv-cache-fundamentals-6090-min)
- [Continuous batching, PagedAttention, and prefix cache](#part-iv--scheduling-and-memory-management-23-hours)
- [Attention sinks / long streaming context](#part-v--long-streaming-context-4560-min)
- [Mooncake and disaggregated cache](#part-vi--disaggregated-kv-cache-and-mooncake-6090-min)
- [Interview drills](#part-vii--interview-drills-3045-min)
- [Final review card](#final-15-minute-review-card)

## Goal

By the end, you should be able to explain this in plain language:

> A KV cache is model-internal state that saves repeated computation over an
> already processed prefix. It improves latency and compute cost, but it adds
> memory, placement, compatibility, ownership, eviction, and privacy
> problems. In this ASR repository, worker-local streaming state plays the
> same operational role, but it is not portable transformer KV state; audio
> replay is therefore the correctness path.

## If time is scarce

| Time available | Read/watch in this order | Deliverable |
|---|---|---|
| 90 minutes | [Repo primer](#part-i--understand-this-repository-first-4560-min); [Illustrated Transformer](https://jalammar.github.io/illustrated-transformer/); [Raschka KV cache](https://magazine.sebastianraschka.com/p/coding-the-kv-cache-in-llms); [Anyscale continuous batching](https://www.anyscale.com/blog/continuous-batching-llm-inference) | Explain Q/K/V, cache growth, and static vs continuous batching. |
| 3–4 hours | Above + [PagedAttention paper](https://arxiv.org/abs/2309.06180) abstract/figures/§3 + [Mooncake paper](https://arxiv.org/abs/2407.00079) abstract/architecture | Explain fragmentation, logical vs physical blocks, and why a remote KV pool is not a generic cache. |
| 6–8 hours | Above + [ORCA](https://www.usenix.org/conference/osdi22/presentation/yu), [Attention Sinks](https://arxiv.org/abs/2309.17453), [vLLM prefix caching](https://docs.vllm.cc/en/latest/design/automatic_prefix_caching.html), [repo code walkthrough](#part-i--understand-this-repository-first-4560-min) | Give trade-offs, sketch a design, and defend why this repo does not use Mooncake. |

Do the **repo primer** first. It makes every external paper easier to map to
something concrete.

## Part I — understand this repository first (45–60 min)

| Priority | Read | What to learn | Interview prompt to practice |
|---|---|---|---|
| Must | [The KV cache and the stateless path](KVCACHE.md) | What the three families' caches actually contain; why a checkpoint is four things; reference-in-request with bytes on a side channel. | “What is in your KV cache, in bytes?” |
| Must | [Considered and rejected](KVCACHE-ALTERNATIVES.md) | Response caching ≠ KV cache; the `N/2` cost of recompute-statelessness; why Mooncake is not integrated. | “Why not put a proxy in front of every streaming chunk?” |
| Must | [Architecture](ARCHITECTURE.md), especially session state, journal, and Bifrost sections | Ownership boundaries: gateway owns audio/recovery; adapter owns inference state. | “Where does each kind of state live, and why?” |
| Strong | [`internal/session/state.go`](../internal/session/state.go) and [`worker/state.py`](../worker/state.py) | Handle, generation, sequence, and compare-and-commit fencing. | “How do you prevent two writers corrupting session state?” |
| Strong | [`internal/coord/failover.go`](../internal/coord/failover.go) | Same-key checkpoint attempt vs fresh-state replay; bounded failover. | “What happens when the current worker dies?” |
| Strong | [`worker/adapters/base.py`](../worker/adapters/base.py) | Compatibility keys and why same model name/weights are insufficient. | “What must match before state can be restored elsewhere?” |

### Repo mental model to memorize

```text
Gateway owns: session metadata, audio journal, recovery policy, event contract
Worker owns:  opaque handle → live model_state
Adapter owns: model-state format + compatibility identity + serialization truth

If worker dies:
  valid compatible checkpoint? restore + replay tail
  otherwise:                fresh state + replay audio
```

Do not say “the gateway holds the KV cache.” It does not. It holds the
reference (`handle`), the compatibility key, audio needed to rebuild state,
and—in the mock-only path—a checkpoint blob.

## Part II — transformer and attention fundamentals (75–120 min)

### 1. Jay Alammar — The Illustrated Transformer

[The Illustrated Transformer](https://jalammar.github.io/illustrated-transformer/)

**Read for:** intuitive architecture vocabulary: embeddings, positional
information, self-attention, multi-head attention, feed-forward layers,
encoder/decoder distinction, and autoregressive decoding.

**Do not get stuck on:** every training detail or the original
encoder–decoder translation example. For serving interviews, understand the
decoder loop and attention inputs/outputs.

**Be able to draw:**

```text
token ids → embeddings + positions → repeated attention/MLP layers → logits
                                                               │
                                            choose next token ─┘
```

**Check yourself:** Why does the next output token depend on every earlier
token? Why does that make naïve generation increasingly expensive?

### 2. Raschka — Understanding and Coding Self-Attention

[Understanding and Coding Self-Attention, Multi-Head Attention,
Causal-Attention, and Cross-Attention in LLMs](https://magazine.sebastianraschka.com/p/understanding-and-coding-self-attention)

**Read for:** the mechanics, preferably with a notebook open. This is the
bridge between an architecture picture and cache tensors.

Learn these terms precisely:

| Term | Plain-language definition |
|---|---|
| Query (Q) | What the current position is looking for. |
| Key (K) | What each prior position offers for matching. |
| Value (V) | Information blended after the match weights are known. |
| Attention score | Similarity of a query with keys; softmax turns it into weights. |
| Causal mask | Prevents a token from attending to future tokens during generation. |
| Head | One independent learned attention projection; heads are combined. |

**Check yourself:** Why are K and V reusable during generation, but the new
token's Q must still be computed? What would go wrong without a causal mask?

### 3. Optional but high value — Attention Is All You Need

[Vaswani et al., Attention Is All You Need](https://arxiv.org/abs/1706.03762)

Read the [abstract](https://arxiv.org/abs/1706.03762),
[Figure 1 and §3.2](https://arxiv.org/pdf/1706.03762), and §3.2.1 scaled
dot-product attention. This gives you the original terminology behind the
blogs. Do not try to memorize every hyperparameter tonight.

## Part III — KV cache fundamentals (60–90 min)

### 4. Raschka — Coding the KV Cache in LLMs

[Understanding and Coding the KV Cache in LLMs from Scratch](https://magazine.sebastianraschka.com/p/coding-the-kv-cache-in-llms)

**This is the most important single resource for the interview.** Read it
after self-attention, not before.

Write this derivation in your own words:

```text
Without KV cache, at generation step t:
  run the full prefix of length t through every attention layer again

With KV cache:
  retain K/V from positions 0..t-1 for every layer
  run only the new token through projections
  append its K/V to the layer cache
  attend the new Q over cached K/V plus its own K/V
```

The win is avoiding repeated prefix projection work. The cost is memory that
grows with active sequence length and concurrent requests.

### KV-cache formula worth understanding

An approximate per-request cache size for a standard multi-head attention
decoder is:

```text
bytes ≈ 2 × layers × tokens × KV_heads × head_dim × bytes_per_element
```

The leading `2` is for keys and values. Exact size changes with grouped-query
attention, multi-query attention, quantization, tensor parallelism, sliding
windows, and architecture-specific state. In an interview, state the
assumptions before quoting a number.

**Check yourself:**

- Why does batch capacity become a memory-management problem?
- Why does a KV cache improve decode but not make arbitrary requests share
  work automatically?
- Why can a cache be compatible with one runtime but not another?

## Part IV — scheduling and memory management (2–3 hours)

### 5. ORCA: the reason continuous batching exists

[Yu et al., ORCA: A Distributed Serving System for Transformer-Based
Generative Models (OSDI 2022)](https://www.usenix.org/conference/osdi22/presentation/yu)

Read the [abstract and introduction](https://www.usenix.org/conference/osdi22/presentation/yu),
the [paper PDF](https://www.usenix.org/system/files/osdi22-yu.pdf), and the
iteration-level scheduling section.

**Core idea:** an autoregressive request takes many decode iterations. Static
batching waits for the longest request before admitting a new one. ORCA
schedules at iteration granularity: a finished request leaves and a waiting
request can join the next iteration.

```text
static batching:      [A B C] → wait until A, B, AND C finish → [D E]
continuous batching:  [A B C] → C finishes → [A B D] → A finishes → [B D E]
```

**Trade-off:** better utilization and throughput, but scheduler complexity,
per-request latency variation, memory pressure, fairness/admission policy,
and preemption all become first-class concerns.

### 6. Anyscale — continuous batching explainer

[How continuous batching enables 23x throughput in LLM inference](https://www.anyscale.com/blog/continuous-batching-llm-inference)

Read this [after ORCA](https://www.usenix.org/conference/osdi22/presentation/yu)
as a practical explanation. It connects iteration-level scheduling to GPU
utilization and latency. Do not repeat its benchmark number as a general fact;
explain the mechanism and say results depend on workload, model, hardware,
and SLO.

**Interview distinction:**

| Term | Meaning |
|---|---|
| Request batching | Group requests, then run a fixed batch to completion. |
| Continuous batching | Rebuild the active batch at token/iteration boundaries. |
| Prefill | Process prompt tokens; compute-heavy, often parallel. |
| Decode | Generate one/few tokens per iteration; memory-bandwidth/KV-sensitive. |
| Chunked prefill | Admit a long prompt over multiple scheduling iterations to protect decode latency. |

### 7. vLLM / PagedAttention

[Kwon et al., Efficient Memory Management for Large Language Model Serving
with PagedAttention (vLLM, SOSP 2023)](https://arxiv.org/abs/2309.06180)

Read the [abstract](https://arxiv.org/abs/2309.06180),
[Figures 1–5 and the design](https://arxiv.org/pdf/2309.06180), and the
evaluation discussion. The central problem is not attention math; it is
allocating growing, variable-length KV cache sequences without wasting GPU
memory through reservation and fragmentation.

**Mental model:** virtual memory for KV cache.

```text
request's logical KV blocks:  L0 → L1 → L2 → L3
physical GPU blocks:          P8    P2    P19   P5
block table maps logical blocks to non-contiguous physical blocks
```

PagedAttention allocates fixed-size physical blocks on demand. This reduces
fragmentation and enables block sharing/copy-on-write-like behavior in cases
such as parallel sampling.

**Do not say:** “PagedAttention makes attention itself sparse” or “it stores
one shared global cache for all requests.” Its central contribution is
KV-memory management and a block-table-compatible attention computation.

### 8. vLLM automatic prefix caching

[vLLM Automatic Prefix Caching design](https://docs.vllm.cc/en/latest/design/automatic_prefix_caching.html)

Read this immediately after [PagedAttention](https://arxiv.org/abs/2309.06180).
It explains how full KV blocks are identified using the block tokens and their
prefix, then reused across requests sharing an exact prefix.

Know the distinction:

```text
live session cache: state for one request continuing over time
prefix cache:       reusable completed prefix blocks across matching requests
response cache:     final output reused for a matching request
```

Also read the short [vLLM cache-salting security note](https://github.com/vllm-project/vllm/blob/main/docs/usage/security.md).
Cross-tenant prefix hits can become a timing side channel; cache reuse is a
security and isolation decision, not just a performance feature.

## Part V — long streaming context (45–60 min)

### 9. Attention Sinks / StreamingLLM

[Xiao et al., Efficient Streaming Language Models with Attention Sinks](https://arxiv.org/abs/2309.17453)

Read the [abstract](https://arxiv.org/abs/2309.17453),
[motivation and diagrams](https://arxiv.org/pdf/2309.17453). The paper asks:
if context is longer than a fixed cache budget, why does naïvely keeping only
a sliding window often fail? It observes that retaining initial “sink” tokens
plus a recent window can stabilize streaming behavior.

**Why it matters here:** it is a useful contrast with ASR replay windows. Both
systems bound state/history, but the correctness and model assumptions differ.
Do not claim that an ASR audio journal is an attention-sink implementation.

## Part VI — disaggregated KV cache and Mooncake (60–90 min)

### 10. Mooncake paper and documentation

[Qin et al., Mooncake: A KVCache-centric Disaggregated Architecture for LLM
Serving](https://arxiv.org/abs/2407.00079) and the [official Mooncake
documentation](https://kvcache-ai.github.io/Mooncake/)

Read the [abstract](https://arxiv.org/abs/2407.00079),
[architecture figure and paper](https://arxiv.org/pdf/2407.00079), and the
[official deployment/design documentation](https://kvcache-ai.github.io/Mooncake/).

Mooncake separates prefill and decode clusters and uses a distributed cache
pool/transfer engine for compatible LLM KV data. It is valuable when the
runtime has a defined format for exporting and importing those KV blocks.

**Use this exact answer for this repository:**

> Mooncake is not integrated. This repo’s real ASR adapters hold
> runtime-owned streaming state that they do not serialize. The gateway can
> therefore move audio and policy, not live state. Its safe recovery path is
> replay; a Mooncake integration would first require adapter-owned state
> export/import, exact compatibility identity, distributed fencing, and a
> demonstrated transfer-cost benefit.

## Part VII — interview drills (30–45 min)

Practice answering out loud, in two minutes each.

1. What are Q, K, and V? Why can K/V be cached during autoregressive decode?
2. What is the time-versus-memory trade-off of a KV cache?
3. Why is KV cache compatibility stricter than “same model name”?
4. Explain prefill, decode, TTFT, TPOT, and end-to-end latency.
5. Why does continuous batching increase utilization? What does it complicate?
6. What memory problem does PagedAttention solve? What does it not solve?
7. Prefix cache versus response cache versus a live session cache?
8. How can prefix caching leak information across tenants?
9. What is an attention sink, and why is a naïve sliding window unreliable?
10. What does Mooncake disaggregate, and where does the cache live?
11. Why is Mooncake not a transparent drop-in for an ASR worker's memory?
12. In this repository, where is the live state, where is the replay source,
    and what survives a worker crash?
13. Why have both sequence numbers and a generation/fencing token?
14. Why must Bifrost not route individual stateful streaming chunks?
15. Design a safe state-transfer protocol: what are the minimum validation and
    fallback steps?

### A strong design-answer structure

When asked to design or critique an inference system, answer in this order:

1. State the latency target and throughput/concurrency target.
2. Separate immutable model weights, live state/KV cache, input journal, and
   final response cache.
3. Define ownership and fencing for live state.
4. Define compatibility and serialization requirements before any transfer.
5. Define cache allocation, admission, eviction, and isolation policy.
6. Define failure behavior and the correctness fallback.
7. Name the measurements: TTFT, TPOT, p50/p95/p99, cache hit rate, memory
   utilization/fragmentation, queue time, recovery time, and error rate.

## Final 15-minute review card

```text
KV cache = per-layer keys/values from an already processed token prefix.
It avoids repeated prefix computation, but grows with active context.

Continuous batching = schedule at generation iterations, not whole requests.
PagedAttention = allocate KV cache in fixed physical blocks via block tables.
Prefix caching = reuse exact compatible prompt-prefix KV blocks across requests.
Mooncake = distributed KV transfer/storage for supported LLM runtime integrations.

This repo ≠ Mooncake:
  worker owns opaque live ASR state
  gateway owns journal + recovery
  checkpoint restore is mock-only
  replay is always the safety net
  Bifrost is for complete stateless work, never live chunk routing
```
