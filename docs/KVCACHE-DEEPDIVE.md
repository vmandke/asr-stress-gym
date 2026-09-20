# How production systems actually maintain a KV cache

Background reading. What this repository built is in
[KVCACHE.md](KVCACHE.md); what it chose not to build is in
[KVCACHE-ALTERNATIVES.md](KVCACHE-ALTERNATIVES.md);
[INTERVIEW-READING-LIST.md](INTERVIEW-READING-LIST.md) lists what to read
next.

This document is **the mechanics**: how a real open-source serving stack
allocates, identifies, shares, evicts, shrinks, moves and routes to KV
cache — and which of those mechanisms have an analogue here. Everything
below is about systems you can read and run: vLLM, SGLang, LMCache, NIXL,
llm-d.

---

## 0. KV cache from first principles

At each transformer layer a token produces a query (Q), key (K) and value
(V). To process token `t`, its query attends to the keys and values of
earlier tokens.

```text
token t
  └─ calculate Q(t), K(t), V(t)
       └─ Q(t) attends over K(0..t)
            └─ combines V(0..t)
```

Without a cache, generating token `t` recomputes K and V for the whole
prefix. With a KV cache, K/V for tokens `0..t-1` are retained and the model
computes only K/V for `t`. **The saving is repeated-prefix computation, not
a lookup of the final answer** — which is the distinction that separates a
KV cache from a response cache.

Those bytes are model-specific numerical state. Their meaning depends on
architecture, weights, layer count, hidden dimensions, precision, position
scheme, runtime version, device/runtime ABI and cache layout. A cache from
the wrong configuration is not merely a cache miss: **consuming it is
unsafe.** That is why this repository's compatibility key is checked before
deserialization rather than after.

### The ASR equivalent

Streaming ASR has the same incremental-state problem with different
structures:

```text
audio frame 1 ─┐
audio frame 2 ─┼─> encoder / decoder / predictor state ─> partial text
audio frame 3 ─┘                     │
audio frame 4 ───────────────────────┘ (reuse prior state)
```

That state can include transformer attention K/V, convolutional or
recurrent left context, transducer predictor state, CTC decoder history,
beam state and endpointing state. A native object such as
`sherpa_onnx.OnlineStream` is still KV-equivalent operationally even if it
contains nothing named `kv_cache` — and the three families this repository
serves show three different mixtures of those ingredients
([KVCACHE.md](KVCACHE.md) §1).

---

## 1. Why maintenance is the hard part

A KV cache entry exists per token, per layer, per KV head:

```
bytes_per_token = 2 (K and V) × layers × kv_heads × head_dim × dtype_bytes
```

For Llama-3-70B (80 layers, 8 KV heads after GQA, head_dim 128, fp16):

```
2 × 80 × 8 × 128 × 2  =  327,680 bytes  ≈  320 KB per token
```

At 8k context that is **~2.6 GB for a single sequence**. On an 80 GB H100
holding ~140 GB of weights across two cards, the cache — not the model —
is what decides how many users fit.

That produces four pressures, and every mechanism in this document is a
response to one of them:

| Pressure | Consequence |
|---|---|
| **It is huge** | Capacity is measured in concurrent sequences, not requests |
| **It grows during the request** | You cannot pre-size an allocation |
| **It is highly redundant across requests** | Shared system prompts, few-shot examples, chat history |
| **It is per-worker and non-portable** | A request must go where its cache already is |

The last one is the one this repository is really about, in a different
domain.

---

## 2. Paging: stop allocating contiguously

The original sin was allocating one contiguous buffer per sequence, sized
to `max_model_len`. A request that generates 100 tokens against a 4096
reservation wastes 97% of it, and the leftovers fragment.

**PagedAttention**, from vLLM, applies the operating-system answer.
Memory is carved into fixed-size **blocks**, each holding a set number of
tokens — **typically 16** — and storing that block's K and V for every
layer and head. A sequence is then a **block table**: an ordered list of
block IDs, exactly like a page table.

Consequences worth internalising:

- **Allocation is on demand.** A block is claimed when a sequence needs
  its 17th token, not at admission.
- **Physical blocks need not be adjacent.** The attention kernel is
  written to gather across the block table, which is why this needed a
  custom kernel rather than just an allocator change.
- **Waste is bounded by one block per sequence** — the partially filled
  tail — instead of by the difference between reservation and reality.
- **Sharing becomes possible.** Two sequences can point at the same
  physical block. This is the foundation everything in §3–§4 is built on.

In vLLM's V1 engine, block objects are **pre-allocated at startup** into a
pool and linked into a free list, so no Python object is created on the
hot path, and a fully computed block is **immutable by convention**: new
tokens go to newly allocated blocks, and existing ones are read but never
overwritten.

---

## 3. Identity: how a system knows it has seen this prefix

Sharing requires answering "is this block's content identical to one I
already hold?" cheaply. vLLM answers it with a **chained hash**. A block's
hash is computed from:

1. **the hash of the preceding block**,
2. **the token IDs inside this block**, and
3. **extra keys** — LoRA adapter ID, multimodal embedding references, and
   an optional **cache salt**.

The chain is the important part. Hashing only a block's own contents would
make any two blocks containing the same 16 tokens interchangeable, which
is wrong: attention is over the *whole* preceding context, so a block is
only reusable if everything before it is also identical. Chaining encodes
"same content **and** same history" in one comparison.

Two operational details fall out:

- **Only full blocks are cached.** A partially filled tail block has no
  stable hash yet, so the last few tokens of a prefix never hit. Cache-hit
  granularity is therefore the block size — a 16-token block means a
  15-token shared prefix gets you nothing.
- **The cache salt is a multi-tenancy boundary.** Injecting a per-tenant
  value into the first block's hash means only requests carrying the same
  salt can reuse those blocks. Without it, prefix caching is a
  cross-tenant side channel: an attacker can detect whether a given prefix
  is resident by timing it. (There is active research on exactly this
  class of attack, so treat salting as mandatory in shared deployments,
  not optional.)

**SGLang takes a different shape for the same job.** *RadixAttention*
keeps cached prefixes in a **radix tree**, where each edge is labelled
with a token *sequence* rather than a single token. An arriving request
walks the tree to find its longest cached prefix. The tree makes
*multi-level* sharing natural — a branching reasoning tree, several
samples from one prompt, a chat history that forks — because a shared
ancestor path is shared by construction rather than discovered by hashing.

Hash-map versus radix tree is a genuine design trade, not a
right-and-wrong: the hash map gives O(1) exact-prefix lookup; the radix
tree gives cheap longest-prefix matching and makes eviction order follow
the sharing structure.

---

## 4. Eviction: reference counting, then LRU

A block may be referenced by many sequences, so eviction is two-stage.

**Stage one: reference counting.** Every block carries a `ref_cnt`. A
block becomes a *candidate* only when its count reaches zero — no live
request is using it. This is ordinary resource management, not caching.

**Stage two: LRU over the free queue.** Freed blocks are not destroyed.
They go onto a **free queue** — a doubly linked list, so removal and
re-insertion are O(1) — and retain their hash, so a later request with the
same prefix can reclaim them. Eviction takes from the head.

The subtle bit is **insertion order**: when a request completes, its
blocks are appended to the tail of the free queue **in reverse order**.
The reasoning is that a block later in a sequence hashes more tokens, so
it is more specific and less likely to be reused by anyone else, and
should be evicted first. Earlier blocks — the shared system prompt — are
the general ones, and survive longest. That is a policy decision encoded
as a list insertion order, and it is the kind of detail worth knowing
exists.

**SGLang's radix tree evicts leaves recursively.** Only leaf nodes can be
evicted (an interior node is by definition a prefix of something still
resident), so eviction walks up from the least-recently-used leaves. Same
LRU intent, expressed over the structure that encodes sharing.

---

## 5. The memory hierarchy: offloading

GPU HBM is the fast tier and the scarce one. **LMCache** — now integrated
upstream into vLLM as a connector — adds tiers beneath it:

```
HBM      fastest, smallest       the live working set
DRAM     ~10-50ms per retrieval  evicted-but-likely-reused prefixes
SSD      slow enough to hurt     long-tail history
```

Reported behaviour worth carrying:

- **DRAM is worth it.** Loading KV from CPU DRAM adds modest overhead
  relative to HBM, and is dramatically cheaper than recomputing prefill.
- **SSD largely is not.** Published results describe the SSD tier as
  inefficient *even with GPU Direct Storage* — restoring KV from disk
  competes with simply recomputing it. Treat a disk tier as a claim to
  benchmark, not a given.
- **Hit rates are workload-shaped.** ~87% is reported for well-structured
  prompts with heavy prefix sharing; that number is a property of the
  traffic, not of the cache.
- **The cache can outlive the engine.** LMCache runs as a separate daemon,
  so a crashed inference process does not take the cache with it. That
  separation is exactly the one this repository does *not* have — see §9.

The decision rule underneath all of it: **fetch beats recompute only when
transfer latency is below prefill cost for the same tokens.** Long shared
prefixes win; short ones lose.

---

## 6. Making it smaller

Orthogonal to where you put it: make there be less of it.

**GQA (Grouped-Query Attention)** — share one KV head across several query
heads. Llama 3 uses 8 KV heads for 64 query heads, an 8× reduction in
cache size versus full MHA. This is an *architecture* decision, baked in
at training time.

**MLA (Multi-head Latent Attention)** — DeepSeek's approach, compressing
KV through a low-rank projection and caching the *latent*. DeepSeek-V3
reports **~70 KB/token against 192–328 KB/token** for GQA-based models of
comparable size: a 2.7–4.7× reduction. Also architectural.

**Quantization** — a serving-time knob, and the one you actually control.
`--kv-cache-dtype fp8` (E4M3) halves bytes per token and halves memory
traffic per attention step. With the FlashAttention-3 backend, attention
itself runs in FP8 rather than dequantizing first. 4-bit goes further:
one published result reports a **higher hit rate at 4-bit than at BF16
under a fixed HBM+DRAM budget — 86.8% vs 75.2%** — because the smaller
entries mean more of them fit. That is the crucial insight: quantization
is not only a memory saving, it is a **hit-rate** lever.

Caveat worth stating: FP8 KV quality is model- and kernel-path-dependent.
Validate per model rather than assuming it is free.

---

## 7. Moving it: disaggregated prefill/decode

Prefill and decode have opposite hardware profiles. Prefill is
compute-bound and parallel over the prompt; decode is memory-bandwidth-
bound and produces one token at a time. Running both on the same GPU means
long prefills stall decode steps for everyone else — head-of-line blocking
that shows up as p99 inter-token latency.

**PD disaggregation** splits them onto separate pools. The prefill worker
computes the KV cache, ships it to a decode worker, and the decode worker
generates. This has become the industry standard shape, supported across
vLLM, SGLang, TensorRT-LLM, LMDeploy and NVIDIA Dynamo.

The hard part is the shipping. **NIXL** (NVIDIA Inference Xfer Library,
open-sourced at GTC 2025) is the point-to-point transfer layer, with
backends for RDMA/InfiniBand, RoCE via UCX, TCP as fallback, NVMe-oF, and
S3-compatible object storage. vLLM exposes it via
`--kv-transfer-config` alongside `LMCacheConnector` and
`MooncakeConnector`.

Two operational notes from deployment guides: RDMA driver verification is
a real prerequisite, and the NIXL **metadata server is a startup-time
single point of failure** — the standard advice is two instances behind a
TCP load balancer.

---

## 8. Routing to it: the part that maps onto this repository

This is the section that matters most here.

Once cache lives on a specific worker, **the load balancer becomes part of
the cache system**. Round-robin and least-connections are actively harmful:
they scatter requests that share a prefix across workers, so each worker
builds its own copy and each copy is a miss that had to be paid for.

The fixes, in increasing precision:

- **SGLang Router** keeps an *approximate* radix tree mirroring what it
  believes each worker holds, lazily updated, and routes to the worker
  with the best predicted prefix match. Reported: **up to 1.9× throughput
  and 3.8× higher cache hit rate** versus naive balancing.
- **llm-d** goes exact: it subscribes to **real KV-block events emitted by
  vLLM and SGLang**, so the router knows which blocks are actually
  resident where, and scores endpoints on true resident-block fraction —
  then breaks ties by queued prefill work rather than request count.

The general result — **cache-aware routing roughly doubles throughput on
identical hardware** — is the strongest argument that cache placement is a
*systems* problem rather than a kernel problem.

**This is precisely what this repository's router is.** `internal/router`
pins a session to the worker holding its state and prefers a worker whose
compatibility key matches. The domain differs — accumulated encoder state
for an audio stream rather than attention KV blocks for a token prefix —
but the invariant is identical: *send the work where the state already is,
because the alternative is recomputing it.* Same problem, same answer,
different state.

---

## 9. Bounding it for unbounded streams

Directly relevant here, because ASR sessions are long-lived.

Naive sliding-window attention — keep only the most recent N tokens —
**collapses** when the window slides past the beginning. Not degrades:
collapses. Perplexity explodes on evicting the KV of the *first* token.

The reason, from **StreamingLLM**, is that models dump enormous attention
weight onto the first few tokens regardless of content. Softmax must sum
to 1, so the model needs somewhere to park attention it does not want to
use; the initial tokens become that sink. Evict them and every subsequent
softmax is renormalised over garbage.

The fix is almost embarrassingly small: **keep the first ~4 tokens
permanently** as attention sinks, and slide the window over everything
else. That alone yields stable generation over **4M+ tokens** with no
fine-tuning. The mechanism now ships in HuggingFace and TensorRT-LLM.

The transferable lesson: **an unbounded stream needs an explicit,
designed retention policy, and the obvious policy is wrong.** This
repository has the same shape of problem — a session may run for hours —
and answers it with a bounded journal ring plus trim-on-final. Different
mechanism, same obligation.

---

## 10. Mapping it back: what this repo has, and what it does not

| Mechanism | Production LLM serving | This repository |
|---|---|---|
| Paged allocation | PagedAttention blocks + block table | **None.** State is an opaque adapter object |
| Prefix identity | Chained block hash / radix tree | **None.** No cross-session reuse exists |
| Eviction | ref-count → LRU free queue | **None.** State dies with the session |
| Sharing | Copy-on-write blocks across requests | **None.** One session, one state, always |
| Offload tiers | HBM → DRAM → SSD (LMCache) | Warm checkpoint tier — **mock adapter only** |
| Shrinking | GQA / MLA / FP8 / 4-bit | **None.** Cannot even size the state (`state_bytes: 0`) |
| Transfer | NIXL, Mooncake, PD disaggregation | **None.** No mechanism moves live state |
| Cache-aware routing | SGLang Router, llm-d | **Yes** — `internal/router` pins and prefers by key |
| Recovery when the holder dies | Recompute prefill | **Yes** — audio journal replay |
| Bounded retention for long streams | Attention sinks + rolling window | **Yes** — bounded journal + trim on final |

The honest summary: **this repository implements the distributed-systems
half and none of the memory-management half.** It knows where state is,
keeps work near it, detects when the holder dies, and rebuilds
deterministically. It never allocates, sizes, shares, quantizes, evicts or
transfers a cache — and for the four real adapters it *cannot*, because
sherpa-onnx and CTranslate2 expose no handle to their internal state.

That is a defensible scope. It is not a KV cache implementation, and
§1–§7 above are the parts that would have to exist before it were one.

---

## 11. Questions worth being able to answer

1. Why is the block hash **chained** to the parent rather than computed
   from the block's own tokens? *(Attention depends on all preceding
   context; identical tokens with different history are not
   interchangeable.)*
2. Why are freed blocks appended to the free queue in **reverse** order?
   *(Later blocks encode more tokens, are more specific, are less
   reusable, and should be evicted first.)*
3. Why can a 15-token shared prefix yield a **zero** cache hit?
   *(Only full blocks are hashed and cached; block size is the hit
   granularity.)*
4. Why does prefix caching need a **salt** in a shared deployment?
   *(Otherwise residency is observable by timing — a cross-tenant side
   channel.)*
5. Why can 4-bit quantization **raise** the hit rate rather than just save
   memory? *(More entries fit in a fixed budget.)*
6. Why does round-robin load balancing **destroy** prefix cache
   efficiency? *(It scatters prefix-sharing requests, so each worker pays
   for its own copy.)*
7. Why does naive sliding-window attention collapse rather than degrade?
   *(Eviction of the attention-sink tokens breaks softmax
   normalisation.)*
8. When is fetching cache from DRAM **worse** than recomputing it?
   *(When transfer latency exceeds prefill cost for those tokens — i.e.
   short prefixes.)*

---

## Sources

- [vLLM — Automatic Prefix Caching (design)](https://docs.vllm.ai/en/stable/design/prefix_caching/)
- [vLLM — Automatic Prefix Caching implementation details](https://docs.vllm.ai/en/v0.6.2/automatic_prefix_caching/details.html)
- [vLLM — Quantized KV Cache](https://docs.vllm.ai/en/latest/features/quantization/quantized_kvcache/)
- [vLLM Blog — The State of FP8 KV-Cache and Attention Quantization](https://vllm-project.github.io/2026/04/22/fp8-kvcache.html)
- [vLLM — Disaggregated Prefilling](https://docs.vllm.ai/en/stable/features/disagg_prefill/)
- [vLLM — NixlConnector Usage Guide](https://docs.vllm.ai/en/stable/features/nixl_connector_usage/)
- [LMSYS — Fast and Expressive LLM Inference with RadixAttention and SGLang](https://www.lmsys.org/blog/2024-01-17-sglang/)
- [LMSYS — SGLang v0.4: Cache-Aware Load Balancer](https://www.lmsys.org/blog/2024-12-04-sglang-v0-4/)
- [SGLang paper — Efficient Execution of Structured Language Model Programs](https://arxiv.org/pdf/2312.07104)
- [llm-d — Precise prefix-cache aware routing](https://github.com/llm-d/llm-d/tree/main/guides/precise-prefix-cache-routing)
- [LMCache — Architecture Overview](https://docs.lmcache.ai/developer_guide/architecture.html)
- [LMCache paper — An Efficient KV Cache Layer for Enterprise-Scale LLM Inference](https://arxiv.org/pdf/2510.09665)
- [AMD ROCm — 4-bit KV Caching in LMCache](https://rocm.blogs.amd.com/software-tools-optimization/4bit-KV-LMcache/README.html)
- [StreamingLLM — Efficient Streaming Language Models with Attention Sinks](https://arxiv.org/abs/2309.17453)
- [MIT HAN Lab — How Attention Sinks Keep Language Models Stable](https://hanlab.mit.edu/blog/streamingllm)
- [NVIDIA NIXL and Disaggregated Inference](https://www.spheron.network/blog/nvidia-nixl-disaggregated-inference-guide/)
- [Mooncake — overview](https://kvcache-ai.github.io/Mooncake/)
