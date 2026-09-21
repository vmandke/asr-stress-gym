# Stateless streaming: the actual request path

This is the default live-demo path in the current code. It describes what the
processes actually do, rather than an idealized production system.

    client
      | WebSocket, ordered PCM frames
      v
    gateway: validate, journal, VAD, choose compatibility cohort
      | multipart HTTP: provider/model + opaque KV reference envelope
      v
    Bifrost: named primary, then same-family fallbacks
      v
    worker: load state, infer one chunk, publish successor state
      |                         ^
      +-- serialized immutable KV version -- KVTier

The client never uploads a KV cache. A request carries a small reference to
state. The larger serialized tensor blob moves directly between a worker and
that model family's KVTier.

## 1. Ownership and deployment shape

| Component | Owns | Does not own |
|---|---|---|
| Client | microphone capture and ordered audio-frame sequence numbers | worker selection and inference state |
| Gateway | WebSocket session, VAD, audio journal, admission and cohort selection | model tensors and tier blobs |
| Bifrost | proxying to the requested provider and fallbacks | a session registry, cache compatibility, or tier placement |
| Worker | model execution, local hot state, serialization/deserialization | the global fleet or a client journal |
| KVTier | bounded, TTL-backed immutable blobs for one family | inference and transcript text |
| Fleet manager | local demo worker creation and Bifrost provider registration | serving request traffic |

The live stack starts with one worker in each family:

| Family | Seed | Adapter | Shared tier |
|---|---|---|---|
| Zipformer | worker-zip-1 | zipformer_kv | kvtier-zip |
| FastConformer CTC | worker-ctc-1 | conformer_ctc_kv | kvtier-ctc |

The dashboard creates worker-zip-dyn-* and worker-ctc-dyn-* peers. Before the
gateway adds one to its router, it reads health and verifies that the worker's
compatibility key and advertised KVTier URL match the seed of the requested
family. A display model name is not enough: the compatibility key is the
proof that a worker can deserialize another worker's state.

## 2. Client connection, admission, and family selection

A browser or cmd/loadgen connects to the gateway WebSocket, sends session.start,
then sends monotonically ordered mono 16 kHz signed-16-bit PCM frames. One
gateway session loop owns the mutable Go state for that connection: sequence
tracking, VAD pipeline, journal, virtual KV handle, and selected worker ID.
Many sessions run concurrently; frames within one session are handled in order.

Admission accepts up to MAX_SESSIONS (200 default, with a 150 soft threshold).
At the hard limit a new client receives overloaded. Existing admitted clients
are not intentionally discarded to make capacity.

At session start the router filters out workers that are drained, ejected,
rate-limited, not healthy/probeable, or cannot serve online streaming. It
randomizes candidate iteration for tie fairness and scores the rest as:

    (outstanding sessions + 1) * recent worker p95 latency

An unseen healthy worker is assigned a small nonzero provisional latency so it
can compete. The winning worker's compatibility key selects the model family.
The selected worker ID is recorded in the stream table as the initial primary.

## 3. Gateway audio work: journal, VAD, and chunks

Every accepted frame, including silence, is appended to the audio journal.
This is the durable-in-memory recovery material. If a state version disappears,
the gateway can recompute it by replaying audio; it never invents missing
model context.

The gateway then passes PCM to the WebRTC VAD pipeline. Clients send 80 ms
transport frames (four complete detector windows) by default; they are
reframed to 20 ms detector windows. The current policy is:

| Setting | Value | Effect |
|---|---:|---|
| Speech-start hysteresis | 40 ms | avoids opening on one noisy window |
| Silence endpoint | 600 ms | ends an utterance after sustained silence |
| Pre-roll | 160 ms | retains speech onset before detection |
| Online chunk | 160 ms voiced audio | one backend inference unit |

Silence remains journaled but does not normally become inference work. At an
endpoint the gateway flushes a final, emits it to the client, and opens a
fresh virtual inference context for the next utterance.

## 4. StatelessClient and immutable state references

For an online worker that advertises serializable state and a KVTier, the
gateway builds a StatelessClient. Open does not call a worker. It only creates
a handle like kv:s-abc and starts its last-applied sequence at zero.

For a chunk ending at sequence N, the gateway posts an OpenAI-shaped multipart
request through Bifrost:

    model       = worker-zip-1/asr-1
    fallbacks   = worker-zip-dyn-1/asr-1
    prompt      = asr-stress-gym-kv:v1:<base64url envelope>
                  { mode: stream,
                    state_ref: kv:s-abc:<previous N>,  // absent for first chunk
                    state_sink: kv:s-abc:<current N> }
    file        = WAV containing this chunk

Each version is create-only. A chunk reads N-1 and writes N; it never
overwrites N-1. That makes retries safe: if a previous attempt created N, a
retry receives a create conflict rather than clobbering newer state. After N
is confirmed, the gateway best-effort retires N-1. That prevents a long stream
from retaining one multi-megabyte blob per 160 ms chunk.

At endpointing, the same envelope carries `mode=final` and references the
latest version; the request has an empty, valid WAV. The worker finalizes
accumulated model state without appending utterance audio a second time.

## 5. Bifrost: stateless data, but a sticky primary preference

Bifrost receives standard multipart HTTP. Version 1.5 drops unknown form
fields, so the gateway places the tiny, versioned reference envelope in the
standard OpenAI `prompt` field, which Bifrost preserves. The worker unwraps it
into `kv_mode`, `state_ref`, and `state_sink`. Bifrost never accesses KVTier or
sees KV bytes; it only routes the audio request and opaque reference names. It
normalizes responses, so worker-specific fields such as worker_id do not come
back through Bifrost.

The gateway explicitly names a provider/model primary. Its fallback list is
built from workers with the same compatibility key. Therefore Bifrost:

1. sends ordinary successful chunks to the named primary;
2. tries same-key fallbacks only when that request fails; and
3. has no independent knowledge that Zipformer workers form one valid cohort
   while CTC workers form another.

This is the answer to the apparent contradiction: KV correctness is stateless,
but placement is still primary-affine as a locality optimization. A primary
that handled the previous chunk usually has its predecessor state in a local
hot cache, avoiding a tier fetch.

Adding a worker does not migrate existing traffic. Existing StatelessClients
already contain their primary and fallback list. The new worker is considered
when the gateway opens a new session, or when Bifrost needs a fallback. Bifrost
is not round-robining successful chunks across all workers in the current
configuration.

The gateway also probes each worker directly for the operator view. A failed
probe immediately ejects that worker from *new-session* selection, even when
Bifrost has not yet noticed its provider is dead. For an already-open
stateless session, Bifrost first exhausts the same-key fallback list carried
by that request. If the whole cohort is unavailable, the request ends with a
clear shared-KV-cohort error. The gateway deliberately does **not** use its
generic cross-family recovery in that case: that fallback would install a
worker-local direct client and silently turn a stateless KVTier session into a
pinned legacy session. It is safer and more truthful to fail than to claim
shared-state continuity after the compatible cohort has disappeared.

Using only a bare model alias cannot safely fix this. With one Bifrost provider
per worker, Bifrost does not know compatibility keys or KVTier membership. A
family-level Bifrost scheduler would have to enforce those invariants at
membership time and should remain locality-aware; otherwise it makes nearly
every chunk pay a remote state fetch.

## 6. Worker concurrency and queueing

Each worker container has two Python processes:

    supervisor.py :9001    process kill/restore only
    server.py     :9000    FastAPI/Uvicorn inference server

There is one Uvicorn worker process and one asyncio event loop by default. It
can accept many HTTP requests concurrently. Model calls must not run on that
event loop, so every inference path uses asyncio.to_thread. Blocking ONNX
Runtime inference, KV serialization, and urllib tier I/O run in Python's
default thread-pool executor.

The adapters configure ONNX Runtime with intra_op_num_threads=1 and
inter_op_num_threads=1. Several requests can therefore execute concurrently
in executor threads, but an individual ONNX call does not fan out across all
CPU cores. This is intentional under the worker's 1.5-CPU Docker limit.

There is no application-level bounded executor queue today. The executor's
pending-work queue is the effective backlog. Worker health exposes:

    HTTP request accepted
        -> inflight++
        -> submitted to asyncio.to_thread
        -> running++ while an executor thread executes it
        -> model and KVTier work
        -> running-- and inflight--

    queue_depth = max(0, inflight - running)

| Metric | Meaning |
|---|---|
| inflight | accepted inference requests not yet answered |
| running | requests executing inside executor threads |
| queue_depth | accepted requests waiting for executor capacity |
| inflight_high_water | peak accepted concurrency since worker start |
| rtf_p50 | median inference seconds / audio seconds over 64 calls |

The dashboard's queue chart is this measured backlog, not a guessed queue
length. It is an observation today; the router does not yet use it as an
admission or routing input.

Two locks protect different things:

* The legacy direct stream endpoint stores a SessionRecord per handle. Its
  per-record lock performs generation-checked compare-and-commit, rejecting an
  out-of-order write rather than silently applying stale audio.
* The default stateless route does not create a worker-local session record.
  The gateway serializes chunks from one client session, and immutable tier
  versions make retries safe even when another same-family worker serves them.

## 7. KVTier and local hot state

Each family has its own Go KVTier process: Zipformer has a 256 MiB default
ceiling; CTC has 512 MiB. Separate tiers isolate eviction and outage domains.
A burst of CTC state cannot evict Zipformer state merely because the two are
co-located.

The worker first resolves state_ref from its local hot cache. That cache holds
deserialized state objects, up to KV_HOT_MAX (32 by default). A hit is removed
from the cache before inference because adapters mutate state in place. Leaving
the predecessor object under its old reference would let a retry apply the
same audio twice. After inference, state_sink is serialized, PUT to the tier,
and kept locally under its new reference.

On a local miss, the worker GETs the immutable blob from its family tier,
checks X-Compat-Key before deserializing, and then infers. The tier stores bytes
and keys only; it never interprets model tensors.

| Event | Response | Correct outcome |
|---|---|---|
| local hot hit | no tier transfer | fastest correct path |
| peer-produced version | tier GET | extra transfer, correct state |
| missing/evicted/expired ref | HTTP 424 | gateway replays journaled audio |
| tier unavailable | HTTP 503 | recovery policy, never fabricated state |
| incompatible key | HTTP 422 | reject rather than deserialize another family |

A 424 is deliberately not treated as fresh state. Fresh state would yield a
plausible but context-losing transcript. Replay costs latency but preserves
correctness.

## 8. Failures, observability, and current limits

| Event | Path |
|---|---|
| primary provider fails | Bifrost tries same-key fallback; peer can fetch state from KVTier |
| tier version missing | worker returns 424; gateway replays journal |
| worker slow | router records p95 and can eject it for future session selection |
| new worker joins | only new sessions choose it; existing primaries stay unchanged |
| worker killed from dashboard | supervisor stays alive on :9001 and can spawn a new server child |

The dashboard shows per-stream initial family/worker, worker p95/error status,
queue depth, RTF, memory, and KVTier hit/miss/put counters. These distinguish
slow executor backlog from a tier fetch, eviction/replay, or a Bifrost primary
failure.

The current demo intentionally lacks a family-level Bifrost scheduler,
proactive migration of existing sessions, bounded worker executor queues,
queue-aware admission, RDMA/object-store transport, and cross-host replicated
tiers. Those are capacity and availability improvements; they do not change
the central correctness rule: only an exact compatibility-key match can read a
state blob, and missing state is rebuilt from journaled audio rather than
guessed.
