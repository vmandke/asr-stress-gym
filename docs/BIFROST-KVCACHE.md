# Why Bifrost does not own a live ASR KV cache

## Decision

**Use Bifrost for stateless transcription work (offline jobs, retries, and
provider fallback), but keep every live online stream and its default final
directly pinned to one worker.**

This is not a limitation of a particular Bifrost configuration. It follows
from the difference between an AI gateway that routes independent requests
and an ASR worker that owns mutable, model-specific inference state. Bifrost
is still a real part of the system: it supplies fallback for work that can be
reissued as a new request. It must not be made responsible for a stateful
stream unless it gains explicit session affinity and state-transfer support.

## Four things that are easy to call “cache”

| Thing | Owner | Meaning | Safe for Bifrost to use as a live stream cache? |
|---|---|---|---|
| **ASR inference/KV cache** | One worker, keyed by its opaque `handle` | Encoder/predictor/attention state after an ordered sequence of audio chunks | **No.** It is mutable, runtime-specific, and bound to a worker-local handle. |
| **Gateway checkpoint** | `coord.CheckpointStore` | A validated serialized snapshot for a compatible target | Only where an adapter explicitly supports serialization. In this fleet, mock-only. |
| **Bifrost response/semantic cache** | Bifrost | A previously completed HTTP response, keyed by request content or similarity | Not a substitute. It may avoid a repeat request; it does not restore a live decoder state. |
| **Prefix cache** | One inference runtime | Reuse of common, immutable input prefixes | Not session continuation and not transferable by an API gateway. |

A response cache answers “have I already answered this request?” A KV cache
answers “what state is the model in after this exact ordered history?” They
are different problems.

## The stateful path

```text
audio chunks, ordered by seq
        |
        v
gateway: session_id, worker_id, handle, generation, compatibility key
        |
        | Push(handle, expected_generation, chunk N)
        v
pinned worker: handle -> model state / KV cache
        |
        | Push(handle, expected_generation + 1, chunk N+1)
        v
same pinned worker
```

`handle` is deliberately opaque and local to a worker. `generation` makes a
write compare-and-commit rather than a blind append. The cache is valid only
for the same model identity, runtime, cache schema, dtype, chunk sequence,
and prior state. The gateway knows how to react when that state becomes
unavailable: it emits `partial.reset`, selects an eligible target, restores a
validated compatible checkpoint when possible, otherwise replays journaled
audio. Bifrost does not have those facts or that recovery protocol.

## What goes wrong if Bifrost routes `Push`

```text
chunk N   -> Bifrost -> worker-a  (creates/updates handle H)
chunk N+1 -> Bifrost -> worker-b  (H is unknown; b has no state)
```

There are only bad outcomes:

1. `worker-b` rejects `H`, so a request router has to learn the gateway's
   session-recovery protocol to make progress.
2. `worker-b` creates fresh state, silently dropping chunk N's context.
3. Bifrost retries/falls back on an incompatible worker without the gateway
   emitting `partial.reset` or replaying the journal.

The latter two are transcript-corruption bugs, not ordinary latency failures.
Passing `handle` through Bifrost does not fix this: the handle refers to a
worker-local state-map entry, not a portable model object.

The same reason applies at an online final. Direct `Flush(handle)` finalizes
the hot state already accumulated by the pinned worker. Bifrost's
`POST /v1/audio/transcriptions` receives a complete WAV and creates fresh
adapter state, infers, finalizes, and discards it. It is a new
transcription, not continuation of the stream. In this deployment, direct
flush measured 9.2 ms and the Bifrost path measured 265.3 ms; different text
is also expected when the Bifrost provider is a different model.

## The supported boundary

| Work | Route | Reason |
|---|---|---|
| Online partials | Direct to the session's pinned worker | Requires handle affinity and mutable state. |
| Default online final | Direct `Flush(handle)` | Reuses the hot state that produced the partials. |
| Online final with `BIFROST_FINALS=1` | Bifrost, then direct fallback on failure | An intentional full re-transcription / second opinion, not continuation. |
| Offline transcription | Bifrost | No partials or live handle; retry and provider fallback are correct. |
| Streaming worker failure | Gateway coordinator | Needs compatibility checks, checkpoint validation, reset emission, and journal replay. |
| Stateless provider failure | Bifrost fallback chain | The whole WAV request can safely be issued anew. |

This division gives Bifrost non-duplicated work: it owns provider retry and
fallback for complete, stateless transcription requests, while the gateway
owns stateful session recovery.

## Why semantic caching is not the solution

Bifrost documents semantic caching as caching completed LLM responses, with
either vector similarity or exact-match/hash mode. It does not export or
rehydrate a provider's attention/decoder state. Enabling it for live ASR
would therefore not continue a stream. At best, exact duplicate complete
audio could return a completed transcript; similarity matching is the wrong
correctness model for speech audio. Keep it disabled for the stateful online
path.

Likewise, a model server's prefix cache is an implementation-local
optimization for common immutable input prefixes. It does not make an active
decoder state safe to move to another runtime or replica.

## If the requirement is “Bifrost must be used”

The current design satisfies that requirement without pretending Bifrost can
transfer a KV cache:

```bash
BIFROST_URL=http://bifrost:8080 docker compose --profile bifrost up -d --build
```

Register each worker as a separate Bifrost provider and pass an ordered
fallback chain. Offline jobs then gain Bifrost retry/fallback behavior; the
gateway takes the direct path if Bifrost itself fails. See
[../bifrost/README.md](../bifrost/README.md) for the deployable configuration.

If “Bifrost must route online traffic” is a hard requirement, the minimum
safe design is a **new stateful integration**, not the existing
OpenAI-compatible transcription route:

1. Route on a stable session-affinity key, never per chunk independently.
2. Carry the gateway's worker ID, handle, expected generation, and
   compatibility key as opaque routing metadata.
3. Guarantee every request for that affinity key reaches the same worker.
4. On worker failure, return control to the gateway coordinator; it alone may
   decide reset, checkpoint restore, or replay.
5. Do not use ordinary Bifrost provider fallback for a `Push`; fallback is a
   fresh request and a fresh request is not a valid continuation.

That extension largely removes the benefits of generic gateway load balancing
for this path and adds another component to the recovery correctness boundary.
Implement it only if the requirement explicitly demands stateful Bifrost
routing, rather than simply adopting Bifrost.

## What a fully stateless online path would cost

The sections above argue that a Bifrost final is a new transcription rather
than a continuation. That raises the obvious follow-up: if the online path
were *also* made stateless, Bifrost could route everything and the gateway
would shed `coord`, `CheckpointStore`, generation checks and the worker's
handle map. What does that cost?

It is quadratic, and the derivation is short enough to check. With a 160 ms
chunk policy, an utterance of `T` seconds is `N = T / 0.16` chunks. A
stateless partial has no prior state, so chunk `k` must re-transcribe the
whole utterance so far — `k · 0.16` seconds of audio. Summing over the
utterance:

```text
audio reprocessed  =  0.16 · N(N+1)/2
multiplier vs stateful  =  0.16 · N(N+1)/2  /  (0.16 · N)  =  (N+1)/2  ≈  N/2
```

**The multiplier is `N/2` — linear in utterance length, not a constant
factor.** It cannot be bought out with faster hardware; it grows with how
long someone talks. Against `zipformer`'s measured RTF of 0.020
([RTF.md](RTF.md)):

| utterance | chunks `N` | multiplier | zipformer compute | effective RTF |
|---|---|---|---|---|
| 5s | 31 | 16× | 1.6s | 0.32 |
| 15s | 94 | 47× | 14.2s | **0.95** |
| 30s | 188 | 94× | 56.6s | **1.89** |
| 120s | 750 | 375× | 901.2s | **7.51** |

Effective RTF ≥ 1.0 means the stream falls behind realtime and never
catches up — the lag grows without bound. That threshold is
`N = 2 / RTF_model`:

| adapter | RTF | stateless online breaks at |
|---|---|---|
| `zipformer` | 0.020 | **16.0s** of continuous speech |
| `conformer_ctc` | 0.041 | **7.8s** |
| `whisper_ct2` | 0.154 | **2.1s** |

The corpus's `long_form` clips are 60–120s. A stateless online path does not
degrade there — it diverges.

Two further costs are independent of compute:

- **Partial latency scales with utterance length.** The last partial of a
  `T`-second utterance must transcribe `T` seconds. Against the locked
  p95 ≤ 250 ms partial target ([DECISIONS.md](DECISIONS.md)), zipformer
  breaks that around 12.5s of speech.
- **Partials stop being monotone.** A streaming decoder's state commits a
  prefix. An independent re-transcription may revise any earlier word, so
  displayed text can churn rather than grow.

The journal does **not** disappear either: audio must still be retained to
re-transcribe from. Statelessness removes the cache, not the storage.

## The trade curve

Two middle grounds exist between "full state" and "fully stateless". Both
are recorded here so they are not rediscovered later as novel:

- **Bounded re-transcription window.** Re-transcribe only the last `W`
  seconds instead of the whole utterance. Cost becomes constant per chunk
  (`W / 0.16`) rather than `N/2`. The price is lost context across the
  window boundary, and a coherent final still needs either a full pass or
  retained state.
- **Coarser partial cadence.** At a 2s cadence instead of 160 ms, the 15s
  case falls from 47× to roughly 3.75×, which is affordable. But partial
  latency becomes 2s, far past the 250 ms target — this trades the
  responsiveness partials exist to provide.

Both trade partial quality or latency for compute. Neither recovers the
cache's benefit; they only make its absence cheaper.

## What the machinery buys

Stated in both directions, so the decision stays re-decidable rather than
inherited.

The gateway's session pinning, health-for-selection,
rate-budget-for-selection, checkpoint store and journal replay are not
incidental complexity that a general-purpose gateway could absorb. They are
the machinery that makes a **per-worker, per-session cache safe** — and
they are deletable only by deleting the cache. Roughly:

| Package | Lines (non-test) | Exists because |
|---|---|---|
| `internal/router` | 542 | pin a session; prefer a compatible key on failover |
| `internal/coord` | 370 | restore a checkpoint, else replay audio |
| `internal/journal` | 129 | the replay floor when no checkpoint helps |
| parts of `internal/session`, `cmd/gateway` | — | generation, epochs, compare-and-commit |

That is the price. The 16×–375× compute above is what it buys. A reviewer
who concludes the cache is not worth ~1400 lines should also accept a
system that cannot transcribe a two-minute utterance in realtime on one
core.

## Implementation gap: offline still runs the stateful path

The boundary table above describes the intended design. The code does not
match it yet, and the divergence is worth recording rather than leaving for
someone to trip over.

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

The intended shape, specified here but **not yet implemented**: an offline
session journals audio, never opens a handle, never pushes, emits no
partials, and issues one stateless transcription at `session.end` — through
Bifrost when enabled, and otherwise directly to a `router.Pick`ed worker's
existing `POST /v1/audio/transcriptions`. That yields **one offline code
path** whether or not Bifrost is deployed, with Bifrost replacing only the
choice of provider.

Note the direction of the benefit: this *protects* the cache rather than
competing with it. It stops offline traffic from occupying worker state it
never reads, leaving that state for the online sessions that do.

## Invariants and review checklist

- A live `Push` always reaches the worker that owns its handle.
- No component silently substitutes a model/runtime while a stream is active.
- A stateful failure always produces gateway-managed reset/recovery; a
  Bifrost retry never substitutes for it.
- A Bifrost result is accepted as an online final only when full
  re-transcription is the requested product behavior.
- Offline Bifrost fallback is tested by killing its primary provider and
  receiving a transcription from the next provider.
- Bifrost outage cannot lose a final: the gateway takes the direct path.
- An offline session emits no `partial` events (currently violated — see
  "Implementation gap" above).
- An offline session holds no worker handle and no live model state.

## Sources

- Local implementation: [Bifrost boundary in ARCHITECTURE.md](ARCHITECTURE.md#10-the-bifrost-boundary), [gateway final routing](../cmd/gateway/conn.go), and [Bifrost deployment README](../bifrost/README.md).
- [Bifrost transcription API](https://docs.getbifrost.ai/api-reference/audio/create-transcription) — complete multipart audio requests, optional streaming, and fallbacks.
- [Bifrost retries and fallbacks](https://github.com/maximhq/bifrost/blob/dev/docs/features/retries-and-fallbacks.mdx) — fallbacks are fresh requests and run configured plugins again.
- [Bifrost semantic cache configuration](https://github.com/maximhq/bifrost/blob/dev/docs/deployment-guides/helm/plugins.mdx) — response-cache semantics and exact-hash mode.
