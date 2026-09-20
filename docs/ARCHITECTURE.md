# Architecture — what happens to a stream of audio

This traces one client's audio from the moment it leaves them to the
moment a transcript comes back, naming every component it passes through,
every piece of state it touches, and what each step costs.

Every number here is measured, not estimated. The tools that produced them
are in [`cmd/inspect`](../cmd/inspect), and each section says which command
to run to reproduce its output:

```bash
make inspect-chunks    # offline: what the gateway does to audio. Needs nothing running.
make inspect-trace     # live: one session end to end, every event stamped
make inspect-fleet     # live: the fleet as the ROUTER sees it
make vad-economics     # live: what gating silence actually saves
```

For the design reasoning behind these choices, see
[build-plan.md](build-plan.md) and [implementation-plan.md](implementation-plan.md).
This document describes what was built.

---

## 0. The one organising rule

**`internal/audio` is the only package in this repository that touches a
PCM sample.**

```
                      ┌─────────────────────────────────────┐
  cmd/gateway         │  never sees a sample                │
  internal/session    │  never sees a sample                │
  internal/journal    │  stores Payload []byte, opaque      │
  internal/coord      │  replays []byte, opaque             │
  internal/router     │  never sees audio at all            │
  internal/backend    │  carries bytes, never inspects them │
                      └──────────────┬──────────────────────┘
                                     │  Ref · Chunk · Record · Span
                      ┌──────────────▼──────────────────────┐
  internal/audio      │  decode · VAD · reframe · pre-roll  │
                      │  chunk cutting                      │
                      └─────────────────────────────────────┘
```

Everything else in this document follows from that. The gateway does not
know what a sample rate is. It moves opaque byte slices tagged with
sequence ranges and durations, and the one package below the line decides
what those bytes mean.

The practical test: changing the VAD, the window size or the chunk policy
must not require touching a test outside `internal/audio`. If it does, the
boundary leaked.

---

## 1. Physical topology

```
    client                     gateway (one Go process)              workers (6 containers)
 ┌────────────┐          ┌───────────────────────────────┐      ┌──────────────────────────┐
 │ loadgen    │  :7070   │  :7070  WebSocket (clients)   │      │ :9001 supervisor.py      │
 │ smoketest  │◄────────►│  :7000  HTTP (health, debug)  │─────►│   /admin/die, /restore   │
 │ chaostest  │  binary  │                               │ HTTP │                          │
 │ inspect    │  frames  │  ── process-lifetime state ── │ 1.1  │ :9000 server.py (child)  │
 └────────────┘          │  router.Router                │ keep │   /v1/stream/*           │
                         │  coord.CheckpointStore        │alive │   /v1/audio/transcriptions│
                         │  admission.Controller         │      │   /admin/slow|429|...    │
                         │  metrics (atomics)            │      │   StateStore: handle→state│
                         └───────────────────────────────┘      └──────────────────────────┘
```

Two processes per worker container, on two ports, and the split is
load-bearing:

| Port | Process | Survives `/admin/die`? |
|---|---|---|
| 9000 | `server.py`, the child — all real traffic | **no** — that is the point |
| 9001 | `supervisor.py`, the parent — `die`/`restore` only | yes |

`die` SIGKILLs the child, so nothing on 9000 can answer. That silence *is*
the "worker died" signal the gateway detects. A surviving parent on a
separate port is the only way to bring it back, which is why fault
injection is split across two ports rather than one.

### The fleet

`make inspect-fleet`:

```
worker         key   status     streaming cache   pinned
worker-mock    K1    healthy    yes       RESTORE 0
worker-a       K2    healthy    yes       replay  0
worker-b       K2    healthy    yes       replay  0
worker-c       K3    healthy    yes       replay  0
worker-d       K4    healthy    no        replay  0
worker-e       K5    healthy    no        replay  0
```

Four facts in that table carry the whole design:

- **worker-a and worker-b share K2.** A failover between them is
  cache-compatible — the cheap path exists.
- **Neither can restore.** sherpa-onnx exposes no serializer, so that cheap
  path *degrades to audio replay*. The degradation is the finding, not a gap.
- **worker-d and worker-e run identical weights** (`whisper-tiny.en`) under
  different runtimes and still hold different keys. The subtlest case in
  the design.
- **worker-d and worker-e cannot stream.** They are healthy backends the
  router must *refuse* for an online session rather than mis-serve.

---

## 2. The client sends a frame

A client emits **20 ms frames at 16 kHz mono s16le** — 320 samples, 640
bytes — paced on a wall-clock ticker, roughly 50 per second.

```
byte 0        type         1 = AUDIO, 2 = CONTROL
bytes 1-8     seq          uint64 big-endian
bytes 9-12    capture_ms   uint32, client clock, relative
bytes 13-16   num_samples  uint32   ← the payload's own duration
bytes 17+     payload      PCM s16le, or UTF-8 JSON for CONTROL
```

Binary, not JSON: base64 would inflate every payload by a third, 50 times
a second, per stream.

**`num_samples` is authoritative and never inferred from arrival cadence
or message size** — invariant 1. The gateway computes
`duration_ms = num_samples * 1000 / sample_rate_hz` and validates
`len(payload) == num_samples * 2`.

A mismatch is a **protocol violation, not a transient loss**: `error`, and
the session closes. This is deliberately harsher than a sequence gap
(which produces `discontinuity` and continues), because continuing on
unverified framing risks exactly the silent corruption invariant 1 exists
to prevent.

A client may batch several nominal frames into one message. Nothing
downstream changes, because accumulation is by duration — see §4.

---

## 3. Inside the gateway: three goroutines

Each WebSocket connection gets exactly three, wired by
[`handleConnection`](../cmd/gateway/conn.go):

```
  ws.Read ──▶ readLoop ──frames(64)──▶ sessionLoop ──events(64)──▶ writeLoop ──▶ ws.Write
              decode                   THE ONLY WRITER              marshal
              validate                 of session state             JSON
```

| Goroutine | Owns | Cancels the context? |
|---|---|---|
| `readLoop` | nothing | **never** |
| `sessionLoop` | `InferenceState`, `Pipeline`, `Journal` | **never** |
| `writeLoop` | the socket's write side | **yes**, on exit |

Both channels are bounded at 64 — invariant 16, no unbounded queue
anywhere.

**Only `handleConnection` cancels, and only after `<-done` proves
`writeLoop` drained.** This is not stylistic. Earlier, `sessionLoop` and
`readLoop` each cancelled on their own early-return paths, which could
abort a goroutine *before* `writeLoop` flushed the very `error` or `final`
event that return had just produced. It dropped the last event
intermittently, and only showed up under `-race -count=30`.

`sessionLoop` being the sole writer is what makes the whole state model
work without locks: one goroutine owns the session, so there is nothing to
contend over.

---

## 4. Chunking — the decision that shapes everything

`make inspect-chunks` runs this offline, against the real
`internal/audio`, with no gateway and no worker.

**A model is never called per frame.** Frames arrive every 20 ms; calling
a backend 50 times a second per stream would spend the entire latency
budget on HTTP. Instead the pipeline accumulates until **160 ms of
declared duration** has arrived, then cuts one `Chunk`:

```
frames   20ms 20ms 20ms 20ms 20ms 20ms 20ms 20ms │ 20ms 20ms ...
         └────────────── 160 ms ─────────────────┘
                                          cut ──▶ Chunk{ID, SeqStart, SeqEnd, Bytes}
```

**Accumulation is by duration, never by frame count.** A client batching 3
frames per message produces the same chunk count as one sending singles.
That is invariant 2, and it is why `num_samples` is authoritative.

Before accumulating, each frame is VAD-tagged. Here is a real
silence-heavy clip, one character per 20 ms frame:

```
     0.0s ##############################.........###################################.....#####################
     4.0s #############################################.....##################################################
     6.0s ###.................................................................................................
     8.0s ....................................................................................................
    10.0s ....................................................................................................
    12.0s .............................................############################.....######################
```

with the boundary events the VAD derived from it:

```
      0.02s  speech.start  seq=2
      6.64s  endpoint      seq=333
     12.92s  speech.start  seq=647
     18.84s  endpoint      seq=943
```

VAD config: `window=20ms start=40ms endSilence=600ms preRoll=160ms`.
Speech must persist 40 ms before an utterance opens (so a cough does not),
and silence must persist **600 ms** before one closes (so a pause
mid-sentence does not split it).

What that clip cost:

```
frames in                     1902   (38.02s of audio)
frames the VAD called speech   816   (43%)
backend calls made             105
backend calls if ungated       238
saved by gating silence        56%
audio actually dispatched     16.68s of 38.02s
```

**Every frame is journaled regardless of voicing.** Gating decides what
reaches a *model*; it never decides what is retained for replay. That
distinction is what keeps recovery correct on audio the model never saw.

Across the whole corpus (`make vad-economics`), gating recovers **83–86 %**
of the theoretically available saving at realistic silence fractions. It
does not recover 100 % because pre-roll and hangover deliberately dispatch
a little silence either side of speech — clipping a word's onset to save a
backend call is a bad trade.

---

## 5. Dispatch — gateway to worker

Chunk in hand, `sessionLoop` calls `dispatchChunk`:

```
POST /v1/stream/push
  X-Handle: 7f3c...                ← which session's state on that worker
  X-Seq-End: 160                   ← how far this chunk reaches
  X-Expected-Generation: 12        ← compare-and-commit guard
  body: <raw PCM bytes>            ← binary, not base64
```

The worker's side:

1. **Idempotency first.** If `seq_end <= last_seq_applied`, return the
   cached text unchanged without re-inferring. This is what makes
   replay-on-failover safe to call freely — §7 depends on it.
2. **Inference off the event loop.** `asyncio.to_thread(adapter.infer)`.
   sherpa-onnx and CTranslate2 block in C++ for the whole decode; inline,
   one session would stall every other session in that process.
3. **Compare-and-commit** against `expected_generation`, or `409`.

Then, back on the gateway, in order:

```
partial   → the client replaces its displayed text entirely
ack       → highest_contiguous_seq the gateway has durably accepted
```

**That ordering is a contract, not an accident.** A `partial` carries no
sequence number, so on its own you cannot tell which audio produced it.
The `ack` that immediately follows names the chunk. This is how
`cmd/loadgen` and `cmd/inspect` attribute latency correctly — measuring a
partial against "the last frame I wrote" under-reports badly under load,
reporting 20 ms for what was really 120 ms, which would make overload look
*fast*.

---

## 6. What it costs — measured

`make inspect-trace`, one 30 s dialogue clip through the real stack:

```
t (ms)      event           detail
0.1         → session.start mode=online 16kHz s16le mono
6.2         ← ack           session_id=s-030443f4  (admitted + pinned + Open'd)
66.5        ← speech.start  utterance=u1
186.6       ← partial       rev=1
186.6       ← ack           seq=8   round trip 19.3ms
347.3       ← partial       rev=2
347.3       ← ack           seq=16  round trip 20.0ms
...
9347.6      ← FINAL         utterance=u1 seq=2-435
9967.7      ← speech.start  utterance=u2
14106.8     ← FINAL         utterance=u2 seq=497-674
```

```
where the time went
  session open (admit + pick + Open)       6.5 ms
  first partial (time to first text)     187.2 ms
  chunk round trip  p50 / p95             20.0 / 20.9 ms   (n=161)

what the gateway did
  frames received                         1518   (30.36s of audio)
  partials emitted                         161
  acks emitted                             161
  finals emitted                             5
  backend pushes (gateway → worker)        162
  ungated, this clip would have been       190
  failovers during this session              0
  duplicate finals (must be zero)            0
```

Reading it:

- **6.5 ms to open** covers admission, worker selection and the `Open`
  round trip. Selection is a scan over six workers in memory; the HTTP
  call dominates.
- **187 ms to first text** is one chunk (160 ms of audio) plus one round
  trip. You cannot transcribe audio before it exists — the floor here is
  the chunk policy, not the model.
- **20 ms chunk round trip, p95 20.9 ms.** Tight because the p50 *is* the
  model: zipformer's measured RTF is 0.020, so 160 ms of audio costs about
  3 ms of compute, and the rest is HTTP and scheduling.
- **5 finals from one session.** Endpointing closed an utterance at each
  conversational pause. `finals == streams_opened` would be wrong here.
- **162 pushes against 190 ungated** — this clip is mostly speech, so
  gating saves less than the silence-heavy case above.

---

## 7. Where state lives

**Nothing is persisted. No disk writes, no volumes, no database.** Every
structure below dies with its process. That is a decision, not an
omission: the answer to a gateway dying is client-side replay from its own
`ack` ring, not server-side durability.

### Gateway, per session — created in `sessionLoop`, dies with the connection

| What | Shape | Bound |
|---|---|---|
| `InferenceState` | session_id, mode, worker_id, handle, compat key, generation, last_applied_seq, stream_epoch, failover_epoch | — |
| **Audio journal** | ring of `audio.Record` | **1500 records ≈ 30 s**, preallocated |
| `Pipeline` | VAD state + accumulator + pre-roll | 160 ms retained |
| `Emitter` | revision counter, emitted-finals set | per utterance |

The journal **wraps rather than grows**. It is the recovery floor: replay
from it always works, which is why it is bounded by capacity rather than
by hope.

### Gateway, per process — one instance, shared by every session

| What | Contents |
|---|---|
| `router.Router` | fixed worker list; per worker: status, rolling outcomes + latencies (50 each), eject timer, backoff, rate-limit `Bucket` |
| `coord.CheckpointStore` | `map[sessionID] → CacheCheckpoint`, **latest only**, deleted at session end |
| `admission.Controller` | admitted count (atomic), high-water mark |
| `metrics` | package-level `atomic.Int64` counters |

### Worker, per process

| What | Contents | Lifetime |
|---|---|---|
| `StateStore` | `dict[handle] → {model_state, generation, last_seq_applied, last_text}` | until `/v1/stream/close` |
| RTF window | `deque(maxlen=64)` | rolling |

`model_state` is the real inference state — a `sherpa_onnx.OnlineStream`
owned by C++, or a buffer for the Whisper adapters.

### The three tiers

```
HOT       worker-local live inference state    dies with the worker
WARM      gateway-side checkpoint blob         survives the worker's death
RECOVERY  gateway-side audio journal           always works
```

**Checkpoints live gateway-side, not on the worker.** A checkpoint *about*
worker-a stored *in* worker-a dies with it, which is precisely when it was
needed. The worker only produces the blob on request and owns nothing
about its storage. Same argument as the journal.

The warm tier is real on **`worker-mock` only**. The other four answer
`501 not_supported` permanently, and the gateway now skips asking them.

---

## 8. A worker dies mid-utterance

```
dispatchChunk gets an error
   │
   ├─ 429?  → Router.On429(worker, retryAfter)   zero the bucket, do NOT sleep
   │
   ▼
coord.HandleBackendFailure
   Report(dead, ok=false); dead.UnbindSession()
   for attempt := 1..3:
       target := Pick(mode, exclude=tried, prefer=currentKey)
       sameKey ? RecoverSameModel : RecoverCrossModel
```

|  | same compatibility key | different key |
|---|---|---|
| **valid checkpoint** | `Restore` blob, replay only the tail after `cp.Seq` | never attempted — state from one model is meaningless to another |
| **no / corrupt checkpoint** | falls through to full replay | fresh `Open`, replay from the last committed final |

Both paths then do the same two things, in this order:

1. emit **`partial.reset`** — always, on any failover
2. re-emit the last replayed chunk's text as a fresh **`partial`**

Without step 2 the client sees a reset with nothing following it and the
transcript goes blank.

**Replay is bounded at the last committed final**, never from session
start. That bound only matters once utterances are long — which is why the
corpus has 60–120 s `long_form` clips.

### Two axes, counted separately

This is the distinction the real models forced into the open:

```
failover_same_model_total / failover_cross_model_total   did the key match?
checkpoint_restores_total / checkpoint_degraded_total    did the warm tier pay off?
```

While every worker was a mock, both questions had the same answer in every
case that existed, and the code conflated them. A real same-model failover
between two zipformer workers reads:

```json
{ "failover_same_model_total": 1, "failover_cross_model_total": 0,
  "checkpoint_restores_total": 0, "checkpoint_degraded_total": 1,
  "duplicate_finals_total": 0 }
```

A compatible worker *was* chosen; the warm tier *was* attempted; it did
not pay off; replay recovered the session anyway. That is the project's
thesis stated as four counters.

### Selection

```
FILTER   health          Healthy | Degraded | Ejected-but-timer-expired
         rate budget     Bucket.Available()      ← read-only, spends nothing
         capability      caps.Streaming || mode == offline
                         mode ∈ caps.Modes
SCORE    (Outstanding+1) × latencyP95Sec,  × 0.5 if key == prefer
COMMIT   Bucket.Take()   ← on the winner only
```

Two details that are easy to get wrong and were:

- **`Available` must not spend.** `Pick` evaluates every candidate and
  commits to one. Charging a token per candidate considered would drain
  the fleet's budget N times faster than it is being used.
- **`Outstanding + 1`, not `Outstanding`.** With plain multiplication,
  every worker scores exactly 0 when nothing is loaded yet — which erases
  the compatibility-key preference in exactly the case a fresh session
  most needs it.

Candidates are also iterated in a **randomised order per call**. Without
that, ties resolved to whichever worker happened to be first in the slice,
fixed for the process's whole lifetime, and every session piled onto one
worker until load differentiated them.

---

## 9. Backpressure

Degradation follows [build-plan.md](build-plan.md)'s order, and the
ordering is the design:

| Step | Where | What |
|---|---|---|
| 1 | `router.Pick` | route new sessions to a less loaded worker |
| 2 | `admission.ChunkPolicy` | above the soft threshold, widen chunks 160 → 320 ms |
| 3 | `router.Pick` | prefer a cheaper/faster backend |
| 4 | `admission.Admit` | **reject NEW sessions** with `overloaded` (retryable) |
| 5 | — | **never drop an already-admitted session** |

Step 5 is guaranteed structurally rather than by care: **admission is
checked once, at `session.start`, and no mechanism exists that could
terminate an admitted session for load.** You cannot do the wrong thing if
the code to do it was never written.

**`overloaded` is not `error`.** `error` is terminal; `overloaded` says the
session was never created and the client may retry. A client that cannot
tell them apart either retries a terminal failure forever or abandons a
recoverable one.

**There is no queue.** A refused session is refused *now*. Queueing
converts a fast failure into a slow one, and a transcript delivered eight
seconds late is worth nothing.

A 429 from a worker never causes a sleep on the online path. A 500 ms
backoff against a 200 ms budget is catastrophic; the session moves to
another backend, and the bucket zeroing is what stops the router handing it
straight back to the worker that just refused.

---

## 10. The Bifrost boundary

For the full distinction between Bifrost response routing and a worker's live
inference/KV cache, including the safe design if stateful Bifrost routing is
ever made a hard requirement, see [BIFROST-KVCACHE.md](BIFROST-KVCACHE.md).

```
partials during speech  ──▶ DIRECT to the pinned worker   stateful, latency-critical
online finals           ──▶ DIRECT by default             hot state exists — see below
offline jobs            ──▶ Bifrost ──▶ any worker        no hot state to discard
```

### Why online finals are NOT routed through Bifrost by default

build-plan.md reasons that a complete utterance is a discrete stateless
request, so finals belong behind Bifrost. **That is true of the audio and
false of the worker.**

When an utterance ends, the pinned worker is holding inference state built
from exactly that audio — accumulated encoder and predictor context. A
direct `flush` finalizes *that state*. A Bifrost-routed final cannot:
`/v1/audio/transcriptions` is stateless by construction — fresh state,
infer, finalize, discard — so it throws the hot state away and
re-transcribes from raw samples.

Measured, same session shape, same clip:

```
direct flush       9.2 ms    "mock1 mock2 mock3 ..."            ← finalizes state already held
via Bifrost      265.3 ms    "FIFTY THOUSAND RUPEES TO MIRR..." ← worker-a, recomputed from audio
```

29× slower, and a different model's text. The latency is a *symptom*; the
cause is that the cheap path exists precisely because the state is already
there, and routing around it discards the thing this project is about.

So it is opt-in (`BIFROST_FINALS=1`), useful when you deliberately want a
second, heavier opinion on the final — and off otherwise.

**Offline is the opposite case, and the one Bifrost genuinely fits.**
Offline emits no partials, so nothing is being reused and nothing is
discarded. There is no latency budget, so retry-with-backoff is *correct*
here rather than catastrophic. And the router has no advantage to offer:
no session to pin, no compatibility key to prefer, no hot state to
preserve. Backend selection for that class is Bifrost's to own, and it is
routed there by default whenever `BIFROST_URL` is set.

`POST /v1/audio/transcriptions` creates fresh state, infers, finalizes and
**discards**. It never touches `StateStore`, never calls
`compare_and_commit`, never serializes. All five adapters support it.

**The streaming path cannot go through Bifrost, and this is structural.**
`push` carries a handle and an expected generation; the worker holds state
keyed by that handle. Bifrost load-balances between providers, so chunk N
could land on worker-a and chunk N+1 on worker-b — the second holding no
state, and the gateway never learning the model changed underneath it. No
`partial.reset`, no replay, a silently corrupt transcript. That is the
exact failure this system exists to prevent.

> **Bifrost decides where a request may run; the session layer decides how
> a live stream is rebuilt when that answer changes.**

### Off by default, and free to fail

```bash
make up                                  # no Bifrost; finals go direct
BIFROST_URL=http://bifrost:8080 docker compose --profile bifrost up -d --build
                                         # Bifrost on :8080; gateway routes eligible finals through it
```

The gateway reads `BIFROST_URL`. Unset builds a `nil` client and every final
takes the direct path — which is *also* the fallback on any Bifrost error:
unreachable, timeout, non-200, malformed body. Enabling it can cost latency
but cannot cost a transcript.

All five workers are registered, each as its own Bifrost provider, because
`base_url` is per-provider rather than per-key. Routing to a single provider
would leave Bifrost's fallback and weighted balancing with nowhere to go.

### What Bifrost actually earns here — and what it does not

Stated plainly, because the gap between what the tool advertises and what
it contributes in this deployment is itself worth recording:

| Feature | Status here |
|---|---|
| Key rotation on 429 | **inert** — no keys; five local containers with no auth |
| Weighted load balancing | **inert** — balancing is across keys *within* a provider, and each provider has exactly one |
| Provider fallback | **real, and the one feature doing non-duplicated work.** The gateway implements failover for the *streaming* path only (`internal/coord`); it has nothing equivalent for discrete requests. Verified: with the primary worker SIGKILLed, a request naming it still returned a transcript, served by the next provider in the chain — on `/v1/audio/transcriptions`, which the published docs only describe for chat completions |
| Health tracking | **redundant** — `internal/router` already ejects on latency vs cluster p95, with probing recovery and a rate budget |
| Retry with backoff | **redundant, and wrong for the online path**, which must never sleep on a retry |

Two of those five are genuinely inert for a reason that cannot be fixed
locally: there are no keys and no quotas, because these are containers on
one host. Health tracking and retry are redundant against a router that
already does the harder version.

What is left is **fallback for discrete requests**, which is real. It
covers a class the gateway deliberately does not: `internal/coord` rebuilds
*streaming* sessions, and has no equivalent for a one-shot transcription
whose backend is down. With the offline class routed here, Bifrost owns
backend selection for it end to end — a non-overlapping division of
responsibility rather than a second opinion.

That is the correct shape for this system. Bifrost would earn considerably
more against multiple **external** providers with separate keys, quotas,
billing and independent outages; against five local containers behind a
router that knows about sessions and compatibility keys, one real feature
and a clean boundary is what is on offer, and it is worth having.

### What it costs, measured

One session, `BIFROST_URL` set:

```
89 partials    p50 20.0 ms     ← direct to the pinned zipformer worker
1 final        641 ms          ← through Bifrost to worker-d (Whisper)
```

The final's text changes visibly with its provenance —
`"Please transfer 50,000 rupees to Mira from..."` (punctuated Whisper)
rather than the pinned worker's uppercase transducer output. That is the
boundary working: a complete utterance is a discrete request and can be
served by any model, while the live stream stays pinned.

**Almost none of that 641 ms is Bifrost.** Same 15.05s clip, three runs
each, worker-d either way:

```
direct to the worker   0.616  0.582  0.576  s
through Bifrost        0.596  0.605  0.588  s
```

The ranges overlap — the proxy hop is single-digit milliseconds. The ~600ms
is `whisper-tiny.en` transcribing 15 seconds of audio.

It is tempting to compare 641ms against the 33ms a direct `flush` takes and
call the difference "the cost of Bifrost". That comparison is wrong, and
worth naming because it is the easy mistake: a `flush` finalizes state the
streaming worker *already holds*, while a final routed here is a complete
re-transcription by a heavier model. The difference is **model choice**,
not proxying. Routing finals to `worker-a/whisper-1` instead would land
near the direct number.

So the honest summary is that Bifrost is cheap and mostly idle. What it
costs you is whatever model you point it at; what it buys you in *this*
deployment is discussed below.

---

## 11. Known gaps

- **Orphaned worker sessions.** A worker holds a session until the gateway
  calls `Close`. A gateway that dies leaves that state resident forever —
  there is no TTL or GC. This caused a real misdiagnosis: a stale session
  made a chaos test's pinning detection pick the wrong worker. Currently
  mitigated in tooling (pinning is detected by *delta*, not absolute
  count); the real fix is a worker-side session TTL.
- **`state_bytes` reports 0.** Honestly unmeasured rather than invented —
  the real state is a C++ object Python cannot size, and a guessed number
  feeding the router's memory filter would be worse than an honest zero.
- **Rate-limit division.** Each gateway sizes its buckets to the full
  worker limit. With one gateway that is correct; the HA profile adds a
  second, and that is the moment N gateways overshoot by N×.
