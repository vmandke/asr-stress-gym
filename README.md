# ASR Stress Gym

A fault-tolerant ASR serving system. The audio and model internals are a
black box; the engineering is session state, model-specific inference
cache, routing, replay, failover and observability.

> **Model cache accelerates recovery when compatible. Audio replay
> guarantees recovery when it is not.**

## Try it

```bash
make live             # fleet + per-family KV tiers + Bifrost + stateless
                      # streaming, idle by default, then prints the mic URL
make live STREAMS=20  # same, with 20 background streams
```

Then open the printed `…/dashboard/mic.html` and **talk into it while the
load runs**. It is a real client on the same wire protocol as the load
generator, and it shows, live: which worker answered and whether the
session is pinned or on the shared KV tier, the cache's actual tensors and
sizes, how your audio is chunked, how much the VAD gated as silence, and
your own state versions being published and retired. Kill the worker
serving you from the dashboard and keep talking — the transcript survives.

The dashboard starts with one worker in each streaming family. Click **+ zip**
or **+ ctc** to launch a new, cache-compatible worker during
the demo. The isolated local fleet manager waits for its health
advertisement, registers its Bifrost provider, and the gateway verifies its
compatibility key and family KVTier before it is allowed into routing. This
is deliberately a local-demo feature: only that service holds the Docker
socket.

See [`docs/FLEET-MANAGER.md`](docs/FLEET-MANAGER.md) for the lifecycle and
security boundary.

Microphone capture needs a secure context. `localhost` counts as one, a LAN
IP does not, so open the URL exactly as printed.

`make live` rebuilds by default. A stale worker image ignores the `kv_mode`
field and quietly serves on the old path instead of erroring, which is
exactly how a demo lies to you; pass `NO_BUILD=1` when you know the images
are current.

## What it does

The gateway streams audio over the wire protocol, journals it, and either
pins each session to a capability-compatible worker or — with
`STATELESS_STREAM=1` — lets Bifrost route every chunk within the session's
model family, because the request carries a *reference* to state held in a
shared tier rather than the state itself. Either way it recovers from
worker failure through compatible restore plus tail replay, or fresh-state
audio replay when the compatibility key differs.

Every deployed adapter owns a **real KV cache**: it drives the model's ONNX
graphs directly rather than through a wrapper that hides the state.

```
worker-zip-*          zipformer_kv       transducer        35 tensors   1.09 MB
worker-ctc-*          conformer_ctc_kv   CTC                3 tensors   2.72 MB
```

Four properties of that fleet are load-bearing:

- **Workers in one family share one compatibility key**, so a failover inside
  that family is the cheap path — a 1.09 MB safetensors blob moves and the
  session continues.
- **The two families cannot read each other's state.** Transducer state
  is structurally meaningless to CTC, so a cross-family failover must build
  fresh state and replay the journal. That degradation is the finding, not
  a gap.
- **State sizes differ.** That is why each family gets its own KV tier
  rather than sharing one byte ceiling — one family's burst must not evict
  another family's sessions.

## Entry points

```bash
./scripts/check_env.sh   # verify go/python/docker/compose are present
make live                # the full stateless demo, above
make start               # the default pinned stack
make stateless-ab        # pinned vs stateless, measured side by side
make stateless-proof     # prove chunks really fan out across a pool
make chaos               # real-stack failover scenarios
make dashboard           # bring the stack up and open the operator view
make mic                 # open the microphone client
make bench               # the frozen loadgen benchmark set
make kv-quant            # fp32 / fp16 / int8 blob size vs text drift
make test                # Go + Python suites
make models              # fetch model weights
make up / make down      # compose up --build / remove every demo container
make reset               # make down, then remove Compose volumes too
```

**Port 7000 on macOS.** AirPlay Receiver listens there by default since
Monterey. Either disable it (System Settings → General → AirDrop & Handoff)
or remap: `GATEWAY_DASHBOARD_PORT=7001 make live`. `scripts/live.sh` detects
the clash and moves to 7001 on its own.

## Design and build plan

**Start with [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** — what happens
to a stream of audio, end to end: chunking, dispatch, where every piece of
state lives, what each step costs. Written from measured output, not prose.

- [`docs/KVCACHE.md`](docs/KVCACHE.md) — the KV cache and the stateless
  path: what the streaming families' caches actually contain, the shared tier,
  version lifecycle, what it costs, and the one known gap.
- [`docs/KVCACHE-ALTERNATIVES.md`](docs/KVCACHE-ALTERNATIVES.md) — every
  design considered and rejected, with the number that settled it: Bifrost
  owning the cache, recompute-style statelessness, Mooncake, token paging.
- [`docs/KVCACHE-DEEPDIVE.md`](docs/KVCACHE-DEEPDIVE.md) — background: KV
  caches from first principles, then how vLLM, SGLang, LMCache and NIXL
  allocate, identify, evict, shrink, move and route to them.
- [`docs/DASHBOARD.md`](docs/DASHBOARD.md) — the operator view and the
  microphone client, panel by panel.
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md) — the wire contract: client↔gateway
  framing and gateway↔backend HTTP surface.
- [`docs/STATUS.md`](docs/STATUS.md) — living tracker: what is built and
  verified vs. pending, per milestone.
- [`docs/DECISIONS.md`](docs/DECISIONS.md) — every question the design left
  open, resolved once, up front.
- [`docs/FAQ.md`](docs/FAQ.md) — the reasoning behind locked decisions that
  are not self-evident from a one-line table entry.
- [`docs/build-plan.md`](docs/build-plan.md) — the architecture and thesis:
  principles, invariants, protocol, failover algorithms, benchmarks.
- [`docs/implementation-plan.md`](docs/implementation-plan.md) — the
  executable milestone plan and the interface contracts that make it
  buildable.
- [`docs/BENCH.md`](docs/BENCH.md) — the frozen `cmd/loadgen` contract and
  the corpus's clip kinds.
- [`docs/RTF.md`](docs/RTF.md) — measured real-time factor per adapter.
- [`docs/INTERVIEW-READING-LIST.md`](docs/INTERVIEW-READING-LIST.md) — an
  ordered reading plan for transformers, KV caches, vLLM, Mooncake, and
  this repository's state model.

## Repository layout

```
cmd/gateway                Go: the gateway
cmd/kvtier                 Go: the shared per-family KV tier
cmd/loadgen, cmd/chaostest Go: load generation and failover scenarios
internal/audio             ALL sample-level code lives here — see the audio
                            boundary in implementation-plan.md — and nowhere
                            else in this repository
internal/{wire,session,journal,router,backend,coord,bifrost,metrics,dash}
worker/                    Python worker: HTTP surface, model adapters,
                            kvcache/ (the tensors), kvtier.py (the client)
corpus/                    committed known-text clips for ground-truth tests
scripts/                   setup, chaos scenarios, benchmarks — checked in,
                            not run ad hoc
```

## Not implemented yet, and why

Tracked live in [`docs/STATUS.md`](docs/STATUS.md), with the KV-specific
list at the end of [`docs/KVCACHE.md`](docs/KVCACHE.md). The largest
standing items: zero-copy state transfer over `/dev/shm`, write-behind tier
publishing, cache-aware routing on resident state, a router-side background
prober, and an offline path that stops opening worker handles it never
reads.
