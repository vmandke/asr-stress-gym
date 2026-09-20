# The KV cache and the stateless path

What the cache actually is, where it lives, how a chunk moves through the
shared tier, and what that costs. Every number here was measured on this
machine against the running stack.

Paths not taken — Bifrost owning the cache, recompute-style statelessness,
Mooncake, token paging — are in
[KVCACHE-ALTERNATIVES.md](KVCACHE-ALTERNATIVES.md). How other serving
stacks solve the same problems is in
[KVCACHE-DEEPDIVE.md](KVCACHE-DEEPDIVE.md). The end-to-end journey of a
stream, independent of the cache, is
[ARCHITECTURE.md](ARCHITECTURE.md).

```bash
make live                         # fleet + tiers + Bifrost + stateless
./scripts/stateless_ab.sh 30 60s  # pinned vs stateless, side by side
curl -s localhost:7000/api/kv     # the layout tables below, live
```

---

## 1. What the cache is

Every deployed adapter drives its model's ONNX graphs directly and owns the
state tensors. sherpa-onnx hides that state behind `OnlineStream`; the graph
underneath does not, and the wrapper was the only thing in the way.

Read live from `/v1/kv/layout` on each worker:

| Family | Tensors | KV bytes | Total state |
|---|---|---|---|
| `zipformer_kv` — transducer | **35** | 466,944 | **1,091,664** |
| `conformer_ctc_kv` — CTC | **3** | 2,715,648 | **2,715,656** |
| `whisper_kv` — encoder-decoder | **2** | 5,505,024 | **5,505,024** |

The three are worth seeing side by side, because they are three genuinely
different shapes of the same idea.

**zipformer** — a bounded sliding window, shipped by the model:

```
cached_key_i    [2, 64, N, 192]   96 KB   attention KEYS
cached_val_i    [2, 64, N,  96]   48 KB   attention VALUES
cached_val2_i   [2, 64, N,  96]   48 KB
cached_conv1/2_i, cached_avg_i, cached_len_i
                                          x5 stacks -> 1.09 MB/session
left_context_len = 64,32,16,8,32          the KV window, per stack
```

Only 467 KB of its 1.09 MB is attention K/V; **614 KB is convolution
state**, which has no analogue in an LLM. The window is fixed per layer,
which is the same answer StreamingLLM reaches for unbounded token streams
(DEEPDIVE §9) — here the model was simply exported that way.

**conformer_ctc** — NeMo's cache-aware streaming tensors, and the most
lopsided of the three:

```
cache_last_channel      [1, 17, 70, 512]   2,437,120 B   attention K
cache_last_time         [1, 17, 512, 8]      278,528 B   attention V
cache_last_channel_len  [1]                        8 B   bookkeeping
```

**whisper** — the textbook case, and the only perfectly symmetric one:

```
in_n_layer_self_k_cache  [4, 1, 448, 384]   2,752,512 B
in_n_layer_self_v_cache  [4, 1, 448, 384]   2,752,512 B
```

4 layers × 448 positions × 384 dims, K and V exactly equal. This is what
every LLM KV-cache diagram draws, and it was sitting exported and unused in
this repository the whole time.

Note the spread: **whisper's state is 5× zipformer's.** That single fact
decides the tier topology in §3.

## 2. A checkpoint is four things, and three of them are not the tensors

The most valuable finding in this work, arrived at through three distinct
test failures, each with its own signature:

| Part | Omitting it produces | Status |
|---|---|---|
| **KV tensors** — acoustic context | Nothing works | Carried |
| **Feature seam** — up to 38 unconsumed frames (~380 ms) | Corruption *at the resume point*: `FOX`→`OX`, `JUMPS`→`JUMP` | Carried |
| **Decode hypothesis** — emitted tokens | Truncation *from the beginning*: `'OVER THE LAZY DOG…'` instead of `'QUICK BROWN FOX JUMPS OVER THE LAZY DOG…'` | Carried |
| **Extractor sample position** | One word differs on 1 of 12 clips | **Not carried** — §9 |

The failure modes are distinguishable, which is what made each one
findable. A single "restore is wrong" symptom would have been far harder.

"The KV cache" in casual usage means the first row. Three quarters of what
actually has to move is not it.

---

## 3. The shared tier

A streaming session used to be **pinned**: its cache lived inside one
worker process, so every chunk had to return to that worker and a load
balancer in front of the fleet could do nothing but honour the pin.

Now a chunk carries a **reference** to state held in a shared tier instead
of the state itself. Any worker in the family can serve any chunk.

```
gateway ──► Bifrost ──► worker        file=<chunk>
                                      state_ref=sess:41   state_sink=sess:42
                        worker ──► kvtier   GET sess:41    ← side channel
                               ──► kvtier   PUT sess:42    ← side channel
                        worker ──► {"text": "..."}          ← only text
```

Through Bifrost: the audio plus ~64 bytes of key. Between worker and tier:
the 1.09 MB. **The control request goes through the load balancer; the
state bytes never do.** That is the invariant Mooncake, LMCache and NVIDIA
Dynamo/NIXL all share; they differ in transport (RDMA) and scale, not in
shape.

Pieces: [`cmd/kvtier`](../cmd/kvtier/main.go) (the tier),
[`worker/kvtier.py`](../worker/kvtier.py) (client + local hot cache),
`kv_mode` on `/v1/audio/transcriptions` in
[`worker/server.py`](../worker/server.py),
[`backend.StatelessClient`](../internal/backend/stateless.go) (the gateway
half, which implements the ordinary `backend.Client` interface so the
session loop, failover and metrics are untouched).

`STATELESS_STREAM=1` plus the `bifrost` profile turns it on. Off by
default: it is a different trade, not a strict improvement.

### One tier per family, not one for the fleet

Blobs are opaque and a worker refuses a foreign compatibility key at load
(422), so a single shared tier would be *correct*. It would not be
*isolated*. Whisper state is 5.51 MB per session against zipformer's 1.09
MB (§1); sharing one byte ceiling means a burst in one family evicts
another family's state, and those sessions take a 424 and replay. That is a
cross-family blast radius created purely by co-tenancy.

Separate tiers also give each family its own failure domain, and keep the
compatibility key as a correctness backstop rather than the only wall.
Production draws the same boundary: a Kubernetes `InferencePool` is the
unit of deployment and scaling, so its cache tier is too.

The gateway learns which tier a family uses from the worker's `/health`,
never from its own config — the same rule the compatibility key follows, so
there is no second copy of the mapping to drift.

## 4. One chunk, step by step

`state_ref` is the version to read; `state_sink` is the version to write.
Both are named by the **caller**, up front, which is what §6 forces.

1. **Gateway** builds a multipart request: the audio chunk, `model`,
   `kv_mode=stream`, `state_ref=<handle>:<lastApplied>`,
   `state_sink=<handle>:<seqEnd>`. On the first chunk of an utterance there
   is no `state_ref` at all — the worker starts fresh.
2. **Bifrost** routes it to any worker in that family's pool. It forwards
   the unknown fields verbatim (§6) and knows nothing about their meaning.
3. **Worker** resolves `state_ref`: local hot cache first (§7), else `GET`
   from the tier. The compatibility key advertised by the blob is checked
   **before deserialization**; a mismatch is 422 and the bytes are never
   handed to a model. A reference that resolves nowhere is 424.
4. **Worker** infers, then `PUT`s the successor under `state_sink` —
   create-only, so a second attempt at the same chunk cannot overwrite it.
5. **Worker** returns `{"text": ...}` and nothing else, because nothing
   else would survive the trip back (§6).
6. **Gateway** retires `state_ref` now that its successor is durable (§5).

The tier stores opaque blobs and never parses one. It holds bytes, a
compatibility key it round-trips as a header, and a timestamp.

## 5. Version lifecycle

A chunk reads version N and writes version M; it never overwrites its own
input. That is what makes a retry safe — if a worker dies mid-chunk, the
retry finds its input intact.

Immutable must not mean kept forever, and the first implementation got this
wrong in a way only load exposed. A session writes one version per chunk,
so at a 160 ms cadence a 60 s session produces ~375 of them. Nothing
reclaimed a version until the session closed, and the 5-minute TTL never
fired inside a 60 s run. Measured at 30 streams for 60 s:

| | leaking | after retirement |
|---|---|---|
| versions written | 6,528 | 5,977 |
| **evicted by the LRU** | **5,682 (87%)** | **0** |
| retired on purpose | 846 | 5,977 |
| peak resident | 268 MB (pinned at the ceiling) | **46 MB** |

The LRU was doing the collecting, which cost 12 sessions their state and
sent them through audio replay. So `StatelessClient.Push` retires version N
once N+1 is durable — safe precisely there, because the push has returned
(no retry of it is outstanding) and every later push reads the successor.
Best-effort and off the critical path: a failed delete costs memory the TTL
reclaims and must never fail a chunk already served.

A related bug found next to it: an exact-key `DELETE` had been implemented
as a prefix delete, so retiring `s1:10` would also take `s1:100` and
`s1:101` — silent state loss for any session long enough to reach
three-digit versions. Both are covered by tests.

## 6. "Immutable" has to mean immutable everywhere

Versions are immutable in the tier. They were not immutable in the worker's
hot cache, and that was the most serious bug in this work.

Adapters mutate inference state **in place** and return the same object
(`zipformer_kv._consume` writes `bank.pending` and `frames_consumed`,
absorbs new tensors, extends the hypothesis). The hot cache holds live
state objects. So handing out a cached entry and letting `infer` run turned
that entry into the state *after* the chunk while it was still filed under
the version *before* it — and `store` then filed the very same object under
the successor:

```
_hot[s:100] ──┐
              ├── one object, now holding post-chunk state
_hot[s:200] ──┘
```

The consequence is worse than a stale read. A retry of that chunk landing
back on the same worker reads its own corrupted entry and applies the chunk
**twice**; the identical retry routed to a peer fetches the correct
serialized bytes from the tier and is right. **Correctness becomes a
function of placement** — precisely the property this design exists to
remove.

Two fixes:

- **Consume on load.** `_hot_take` removes the entry rather than peeking,
  so once a version has been handed to inference it is gone and a retry
  must go to the tier's real bytes. A session is served serially, so in the
  happy path a version is read exactly once and this costs nothing. The
  alternative — deep-copying 1.09 MB of tensors on every local hit — pays a
  memcpy per chunk to preserve an entry nothing reads. A fetched object is
  likewise no longer filed under the version it is about to stop being.
- **Create-only writes.** `PutIfAbsent` (409 on an existing key,
  test-and-set under one lock) so two attempts at one chunk cannot both
  write the same `state_sink`. The worker treats 409 as success: it only
  needs the version to exist.

Locality survives both — the successor is cached under the version the next
chunk actually asks for, and the fan-out proof still shows 4 local hits on
the sticky policy. The regression tests were verified to **fail** with the
bug reintroduced.

## 7. The Bifrost constraint that shaped the design

Measured against Bifrost with a purpose-built echo provider:

| Direction | Unknown fields |
|---|---|
| gateway → worker | **forwarded verbatim** (`['model','kv_dtype','state_blob','file']` all arrived) |
| worker → gateway | **stripped** — only `text` plus Bifrost's own `extra_fields` |

Bifrost parses provider responses into a normalized schema; anything
outside it is by definition not part of the contract. That is not a bug, it
is what a *semantic* gateway is for.

So state can travel **in** through Bifrost but cannot come **back**. The
design never needs it to: the caller names the output reference up front
(§4), so the response carries nothing but text.

The same stripping is why a cache miss is signalled as an **HTTP status**
rather than a response field. Status codes survive the hop, and a miss
genuinely means this provider cannot serve the request.

---

## 8. Results

### Safety, against the running stack

| Case | Expected | Measured |
|---|---|---|
| `state_ref` resolves nowhere, via Bifrost | refuse, let the caller replay | **424**, worker error body preserved through the hop |
| zipformer blob read by a CTC worker | refuse, never coerce | **422** `incompatible_state`, names both keys |
| zipformer blob read by its twin | succeed | **200**, `kv_hit=tier`, 1,105,800 bytes moved |

The 422 is the important one: it is the same rule as `/v1/stream/restore` —
refuse, never partial-restore, never coerce — now enforced on a path a load
balancer chooses. The check runs **before** deserialization, against the
compatibility key the tier round-trips as a header.

### What it costs

`corpus/02_number_transfer.wav`, 2.20 s, 5 × 500 ms chunks, zipformer pair:

| | alternate | sticky |
|---|---|---|
| workers used | 2 | 1 |
| local hits | 0 | 4 |
| tier fetches | 4 | 0 |
| fetch bytes | 4,413,840 | 0 |
| put bytes | 5,508,744 | 5,500,416 |
| wall | 113.6 ms | 60.4 ms |
| state / audio | **141×** | **78×** |

Both transcripts are byte-identical to a single-worker reference run.

**This confirms the earlier estimate.**
[KVCACHE-ALTERNATIVES.md](KVCACHE-ALTERNATIVES.md) priced a state-carrying
request at 427× the audio for 160 ms chunks. Scaled to 500 ms chunks that
predicts 137×; measured 141×.

Two things follow, and they are the point:

- **Locality is worth 1.88×** and takes inbound transfer to zero. So a
  cache-aware router — vLLM prefix-cache routing, a Gateway API Inference
  Extension endpoint picker — is not a nicety, it is where the performance
  is.
- **Both policies are correct.** They differ only in cost. That is the
  whole change: affinity stops being a *correctness requirement* and
  becomes an *optimization*.

```
pinned session   affinity REQUIRED    a miss is data loss
shared tier      affinity PREFERRED   a miss costs a fetch
```

Outbound bytes barely move between policies because the tier write is
synchronous. That is deliberate — a write-behind would let a peer read a
reference the tier does not have yet, which degrades safely to a miss but
would make the demonstration flaky. Production overlaps the write with the
next chunk.

### Cross-worker restore, through the gateway

All workers but one pair drained, 4 streams, the serving worker SIGKILLed
mid-utterance:

```
failover_total          = 2
failover_same_model     = 2
checkpoint_RESTORES     = 2      <- permanently 0 on real models before this
checkpoint_degraded     = 0
duplicate_finals        = 0
errors = 0   discontinuities = 0
```

Verified independently at the HTTP layer: a 1.10 MB safetensors blob
checkpointed from one container and restored into another, continuing
correctly.

### `state_bytes` is a real number

**2.19 MB** for two live sessions, replacing the hardcoded `0` that every
real adapter reported through M10. That zero was honest — nothing could
size an onnxruntime-owned C++ object — and it is now unnecessary.

### Speed

Driving the graphs ourselves is **faster** than going through the wrapper:

| | RTF |
|---|---|
| `zipformer_kv` (ours) | **0.0139** |
| `zipformer` (sherpa-onnx) | 0.0188 |

0.74×, because there is one ORT session per graph and no wrapper between.

### Quantization

12 corpus clips, checkpoint at the midpoint, restore, compare:

| width | identical | blob | encode |
|---|---|---|---|
| fp32 | 11/12 | 1.10 MB | 0.4 ms |
| fp16 | 11/12 | 0.56 MB | 0.3 ms |
| **int8** | **11/12** | **0.28 MB** | 0.6 ms |

**int8 costs nothing measurable and is 4× smaller.** The single
non-identical clip is *the same clip with the same one-word difference at
all three widths* — including fp32, which is lossless. So quantization is
not the cause, and the headline is clean: on this model, the KV cache
quantizes to int8 for free.

Integer bookkeeping tensors (`cached_len_*`) are never quantized; scaling a
counter would corrupt the encoder rather than blur it.

---

## 9. The one known gap, characterised

On `09_longer_monologue.wav`, a restored session says
`'…SEVERAL INFERENCE CALLS SINCE…'` where an uninterrupted one says
`'…SEVERAL INFERENCE CALLED…'`. (The restored version is the *more*
accurate of the two, which is luck, not a feature.)

Isolated by experiment:

```
split 30% : two-push == reference  TRUE    restored == reference  FALSE
split 40% : two-push == reference  TRUE    restored == reference  FALSE
split 50% : two-push == reference  TRUE    restored == reference  FALSE
split 70% : two-push == reference  TRUE    restored == reference  TRUE
```

**Splitting audio across two `infer()` calls is perfectly transparent at
every split point.** Only the restore differs, and only sometimes. That
isolates the cause to the one piece of state not carried: the feature
extractor's sample-level position. `OnlineFbank` is continuous across
`accept_waveform` (verified), so a warm extractor and a cold one produce
different frame grids, and near a word boundary that can flip a token. At a
70% split the boundary happened to land on a frame boundary, so it agreed.

Attempted fix, unsuccessful: priming the restored extractor with
10/25/40/60/100 ms of already-consumed audio does not realign the grid,
because `snip_edges=False` re-applies start padding to a fresh extractor.

Closing it properly means reproducing the extractor's internal sample
offset, which `kaldi-native-fbank` does not expose. Recorded rather than
hidden: **11/12 clips restore identically; the twelfth differs by one
word.** The production path has the same property, because the gateway
replays audio from `last_seq_applied` into a freshly-constructed extractor.

The same seam shows on the stateless path: at chunk 3 of the fan-out proof
the *partials* differed across a transfer boundary (`RUPE` vs `RUPEES`)
while both finals converged identically.

## 10. The blob format

**safetensors, never `pickle` or `torch.load`.** These blobs leave the
process: adapter → worker → tier → a *different* worker. `pickle.loads`
executes arbitrary code by design, so using it here would make "restore a
checkpoint" a remote code execution primitive reachable by anything that
can write to the store. safetensors is a length-prefixed JSON header plus
raw tensor bytes and cannot execute anything. There is a test asserting the
blob is not a pickle.

The header carries `cache_schema_version`, the compatibility-key hash, a
tensor-geometry digest, dtype and quantization scales, and every field is
checked **before any tensor is examined**. Mismatch raises; the gateway
degrades to audio replay, which is always available because it holds the
journal.

---

## 11. How faithful is this to production?

| This | Production |
|---|---|
| HTTP over a Docker bridge | RDMA / NVLink — µs, not ms |
| One in-memory process (a SPOF) | Sharded, replicated across cluster DRAM/SSD |
| Keyed by session | Content-addressed by prefix hash, so sessions **share** blocks |
| Whole-state snapshots | Paged into blocks, shared and evicted per-block |
| One tier — every remote read is a fetch | HBM → DRAM → SSD → remote, hot copy stays local |
| safetensors + HTTP copy | Zero-copy / GPUDirect DMA |

The local hot cache in `worker/kvtier.py` is what keeps the last row from
being fatal: a worker that served the previous chunk still has the live
state object and pays nothing. Without it, this design is stateless but
strictly slower than pinning.

**Faithful:** reference in the request with bytes on a side channel;
immutable versions so a retry finds its input intact; a byte ceiling with
LRU so memory is bounded; a miss that degrades to recompute rather than
corrupting; a tier that stores opaque blobs and never parses one; a format
version checked at load.

**Not faithful:** the transport, the scale, and the absence of sharing
between sessions. Scale here is demonstration-grade; the mechanisms are
real.

## 12. Not built

- **Zero-copy.** All containers share a host, so a `/dev/shm` data plane
  with mmap'd safetensors would remove the HTTP copies entirely — the
  single-host analogue of RDMA, and what safetensors' format exists for.
  The tier would become a metadata/ownership service, which is structurally
  what Mooncake Store is.
- **Write-behind** publishing, overlapped with the next chunk.
- **Cache-aware routing** on resident state — §8 shows it is worth 1.88×.
- **Offline sessions** still run the pinned path. They emit no partials and
  already have a better route (one stateless transcription of the whole
  utterance), so moving their chunks through a tier would move state for no
  reason — but the pinned open/push/discard sequence they do run is
  wasteful for its own reasons
  ([KVCACHE-ALTERNATIVES.md](KVCACHE-ALTERNATIVES.md)).
- **Failover reverts to pinned.** `statelessClientFor` runs at session
  start. If `coord` recovers a session onto a new worker, that worker's
  ordinary `HTTPClient` takes over and the session finishes on the pinned
  path. Correct, but a recovered session stops exercising this.
- **A probe still needs a new session to carry it.** `router.Pick` runs at
  `session.start`, so an ejected worker recovers only when fresh sessions
  arrive — observed: one worker stayed ejected while eight 4-minute
  sessions ran, because none of them ended. A router-side background prober
  on its own timer is the real fix.
- **The latency comparison is not established**, and should not be quoted.
  p50 favours stateless consistently (partials 40 ms vs 20 ms), but those
  are round numbers plausibly quantized to the 20 ms frame cadence rather
  than a real 2×. The pinned path's partial p95 varied **30× across four
  runs** (280 / 3,780 / 5,020 / 8,020 ms) on a machine busy with rebuilds,
  and a control run with `CHECKPOINTS_ENABLED=false` produced the *worst*
  of those — refuting the obvious explanation that per-chunk checkpoint
  serialization was the cost. What the design costs in latency is an open
  question needing a quiet machine and repeated runs.
