# Implementation plan — ASR Stress Gym

## Context

[docs/build-plan.md](build-plan.md) is a design essay. It argues a thesis well — *model cache accelerates recovery when compatible; audio replay guarantees recovery when it is not* — and its principles, invariants and topology analysis should survive intact. But it cannot be executed as written: it specifies three mutually inconsistent backend interfaces, leaves a correctness hole in replay that would silently corrupt reconstructed state, scatters sample-level DSP through layers that should never see samples, and schedules its own thesis as Phase 5 of 7.

This plan keeps the design doc as the architecture reference and replaces its "Phases and done" section with an executable build. It resolves every question the doc defers, fixes the defects below, confines all audio processing behind a single boundary, and sequences the work so the graded behaviour — kill a worker, transcripts keep flowing — is demonstrable on day 4 rather than day 9.

Confirmed for this build: **~2 weeks**, **Go gateway + Python workers**, **real streaming models**, **Docker from milestone 1**, **a fleet of several model types across several deployments**, and **audio kept as abstract as the design allows**.

## The audio boundary

The doc's own premise is that audio and model internals are a black box and the engineering is session state, cache, routing, replay and observability. Its body then contradicts that — reframer code, 512-sample windows, zero-crossing heuristics, hysteresis counts — spread across sections that have no business knowing a sample rate.

**One package owns every sample-level decision. Nothing above it sees PCM.**

```
gateway/internal/audio     ← the only code that touches samples
    Decode / validate declared duration
    VAD (library-backed, behind an interface)
    Reframing to whatever window the VAD wants
    Pre-roll retention
    Chunk cutting by accumulated duration
```

Its exported surface is deliberately tiny, and it is the *only* thing the rest of the plan depends on:

```go
type Ref struct {              // an opaque handle to retained audio
    Seq        uint64
    DurationMs float64
    Voiced     bool
}

type Chunk struct {            // what a backend is called with
    ID       uint64
    SeqStart uint64
    SeqEnd   uint64
    Bytes    []byte            // opaque payload, never inspected above this line
}

type Pipeline interface {
    Ingest(frame wire.Frame) (Ref, error)   // decode, validate, VAD-tag
    Ready() []Chunk                          // cut chunks when duration accumulates
    Boundary() (Event, bool)                 // speech.start / endpoint, source unspecified
    Recut(spans []Span) ([]Chunk, error)     // reproduce chunks for replay
}
```

Everything above the boundary — session, journal, coordinator, router, backend client — deals in `Ref`, `Chunk`, seq ranges and durations. The journal stores opaque bytes. The coordinator replays opaque bytes. The router never sees either.

| Inside the boundary (not a plan concern) | Outside (what the plan is actually about) |
|---|---|
| VAD algorithm and thresholds | *that* silence is gated before dispatch |
| Reframe window size | — |
| Pre-roll length | *that* pre-roll survives journal trimming |
| Chunk cutting mechanics | *that* accumulation is by declared duration, never frame count |
| Sample format handling | *that* the format is declared and validated, never guessed |

The right column is load-bearing and gets tested. The left column is library work behind an interface, tuned by config, and the README reports the values chosen rather than defending them. VAD in particular follows the doc's own non-goal: **use a library, do not write one.**

The practical payoff: swapping the VAD implementation, the window size, or the chunk policy touches one package and no test above it. If a change to chunk cutting breaks a coordinator test, the boundary has leaked.

## Findings that change the design

Verified against upstream source during planning, not assumed.

**sherpa-onnx cannot serialize inference state.** The Python bindings expose `accept_waveform`, `input_finished`, `set_option`, `has_option`, `get_option`, `get_frames` on `OnlineStream`, and `create_stream`, `is_ready`, `decode_stream`, `decode_streams`, `get_result`, `is_endpoint`, `reset` on `OnlineRecognizer`. No save, no restore, no clone. The doc flagged this as a Phase 4 risk; choosing real models promotes it to a day-1 architectural fact.

Not fatal — the doc's own `recoverSameModel` already degrades to audio replay when no restorable checkpoint exists. The consequence is that **the warm-checkpoint tier is real only on the mock adapter**, so benchmarks B and C run there. Real adapters demonstrate the degradation path. Both are honest results; what is unacceptable is discovering this in week two and quietly dropping benchmark B.

**Bifrost works but must point at a local provider.** It exposes OpenAI-compatible `/v1/audio/transcriptions` for discrete requests, confirming the doc's boundary (finals and offline through Bifrost, partials direct). It routes to external providers by default, which would break the hermetic `docker compose up`. Worker D must therefore also expose an OpenAI-compatible transcription endpoint and register in Bifrost as a custom provider.

**The same weights under two runtimes genuinely disagree.** sherpa-onnx's Whisper-tiny ONNX export and faster-whisper's CTranslate2 build of the same checkpoint produce materially different text (sherpa-onnx issue #2900). Since transcription quality is explicitly excluded from grading, this is an asset: it makes the doc's `same weights → different runtime serialization` incompatibility case *observable* — a reviewer watches the transcript legitimately change across that failover, which is exactly what `partial.reset` exists for.

### Defects to fix

| # | Defect in the design doc | Fix |
|---|---|---|
| 1 | Three conflicting backend interfaces: `ASRModelAdapter.Infer(audio, state)→state`, HTTP `push{handle}`, and `target.Restore/Replay/CreateState` | Split into two named layers (below). `Replay` and `CreateState` are deleted — they are `Push` and `Open` |
| 2 | Sample-level DSP spread across session, chunker and VAD sections | Single `audio` package; everything above it moves opaque `Ref`/`Chunk` |
| 3 | Replay undefined over silence. The chunker skips silence for inference but journals everything; replaying everything reconstructs a state the original worker never had | Replay from a **dispatch log** of seq spans, re-cut by `audio.Recut` |
| 4 | Chunk boundaries affect streaming-encoder state, but replay does not preserve them | Dispatch log records exact spans; `Recut` reproduces them identically |
| 5 | `audio_b64` on the gateway→worker hot path, after the doc rejects base64 client-side for inflating payloads 33% | Binary body, metadata in headers. Same URL shape, so the Bifrost story holds |
| 6 | `handleBackendFailure` recurses through both recovery functions; the attempt guard sits after `router.Pick` | Explicit bounded loop, guard first |
| 7 | Client-side replay needs the server to ack `highest_contiguous_seq`, but no `ack` event exists in the contract | Add `ack` to the transcript contract |
| 8 | `TrimBefore(committedSeq)` can discard pre-roll for an utterance already open | Journal trims to `min(committedSeq, openUtteranceStart)` less the pipeline's declared retention |
| 9 | "Same-model failover may continue without reset" needs cross-generation text comparison to be safe | Always emit `partial.reset` on failover. The measurable difference between modes is recovery time and recomputed audio, not whether reset fires |
| 10 | At-least-once finals need dedupe; no dedupe structure specified | Coordinator holds an emitted-finals set keyed by `utterance_id` |
| 11 | Every adapter is assumed to stream. A non-streaming model cannot serve online partials at all | Adapters declare `Capabilities`; the router filters on them before scoring |
| 12 | Thesis (failover) scheduled as Phase 5 of 7 | Moved to M3, on mock adapters, before the audio pipeline and before real models |

## Model fleet and deployments

The doc's three workers (two sharing a key, one not) prove the thesis minimally. A fleet across several architectures and several deployments turns the compatibility key from a two-valued flag into a real matrix, and turns routing into genuine constraint satisfaction rather than a preference.

Two independent axes:

- **Model type** — the architecture, which determines what inference state even *is*. The doc's terminology section already says transducers hold encoder + predictor state, CTC holds hidden state and no attention cache, encoder-decoders hold literal KV tensors. The fleet makes that concrete.
- **Deployment** — how a given model is served: replica, runtime, dtype, resource limit, local versus provider-routed. Two deployments of identical weights can still be cache-incompatible, which is the subtlest and most valuable case.

### The fleet

| Worker | Family | Model | Runtime | Dtype | Key | Streaming | Serializable | Role |
|---|---|---|---|---|---|---|---|---|
| `worker-a` | transducer | zipformer-en 20M | sherpa-onnx | int8 | **K1** | yes | no | online primary |
| `worker-b` | transducer | zipformer-en 20M | sherpa-onnx | int8 | **K1** | yes | no | same-key failover target |
| `worker-c` | ctc | zipformer-ctc-en | sherpa-onnx | int8 | **K2** | yes | no | cross-family failover |
| `worker-d` | encdec | whisper-tiny.en | ctranslate2 | int8 | **K3** | **no** | no | offline + finals; Bifrost provider |
| `worker-mock` | mock | mock-v1 | mock | — | **K0** | yes | **yes** | the only real checkpoint tier |
| `worker-e` *(profile)* | encdec | whisper-tiny.en | onnx | int8 | **K4** | no | no | **same weights as D, different runtime** |

Compose profiles keep this from becoming the project: default brings up `a, b, c, d, mock`; `--profile models` adds `e`; `--profile ha` adds nginx + a second gateway; `--profile gpu` is a documented override, never the happy path.

CPU budget at pinned limits — 1.5 vCPU per real worker, 0.5 for mock, 2 for gateway, 1 for loadgen, 0.5 for Bifrost — is ~10 vCPU default and ~11.5 with `models`. Colima gets 12 of this machine's 14 cores. Without pinned limits no capacity number is reproducible, so the limits are not optional.

### What each pair demonstrates

| Failover pair | Keys | Recovery | Why it earns its place |
|---|---|---|---|
| a → b | K1 → K1 | checkpoint if available, else tail replay | the cheap path; with sherpa it degrades, and the degradation is the finding |
| mock → mock′ | K0 → K0 | **real checkpoint restore + tail replay** | the only place benchmark B is measurable |
| a → c | K1 → K2 | fresh state + full replay + `partial.reset` | cross-architecture: transducer state is meaningless to CTC |
| d → e | K3 → K4 | fresh state + replay | **same weights, different runtime.** The transcript visibly changes |
| a → d | K1 → K3 | refused by the router | non-streaming adapter cannot serve online partials |

That last row is the one the doc has no answer for, and it is the most realistic: a healthy, capable backend that is simply the wrong shape for this traffic.

### Compatibility matrix as a deliverable

`make matrix` enumerates every ordered pair in the fleet, queries each worker's advertised key and capabilities, and emits `docs/compat-matrix.md`: which pairs restore a checkpoint, which replay audio, which the router refuses, and the measured recovery cost of each. One generated table proving the thesis across a real fleet is worth more than prose, and it cannot drift from the code because it is generated from the running system.

## Locked decisions

These replace the doc's "What remains unspecified". They go in `docs/DECISIONS.md` and the README.

| Question | Decision |
|---|---|
| Audio format | One declared format, required in `session.start`, validated not guessed. Format handling lives in the `audio` package; the rest of the system is format-agnostic |
| Latency milestone | `final_latency_ms` (endpoint decision → `final` on the wire) p95 ≤ 300ms; `partial_latency_ms` p95 ≤ 250ms. **All server-side.** The interview's 100ms network term is not observable in compose; it is budgeted, not measured, and the README says so |
| Concurrency target | 200 concurrent online streams on mock; 20 on sherpa streaming. Measured and reported, not claimed |
| Partials | Yes for online, no for offline |
| Checkpoint tier | Real on mock only; `serializable == False` on every real adapter, exercising the documented degradation path |
| VAD | Library-backed behind `audio.VAD`. Thresholds are config; chosen values reported, not defended |
| Gateway↔worker transport | HTTP/1.1 keep-alive, binary body, JSON response |
| Bifrost | Finals + offline only, behind `BIFROST_ENABLED`, direct fallback always present. First thing cut |

## Interface contracts

The most important fix after the audio boundary. Three layers, named so they cannot be conflated.

**1. `gateway/internal/backend.Client` — Go, coordinator side, crosses the network.** Mirrors the HTTP surface exactly.

```go
type Client interface {
    Open(ctx context.Context, r OpenReq) (OpenResp, error)      // -> handle, compat_key_hash, capabilities, generation
    Push(ctx context.Context, r PushReq) (PushResp, error)      // -> text, last_seq_applied, generation
    Flush(ctx context.Context, r FlushReq) (FlushResp, error)   // -> text, final
    Restore(ctx context.Context, r RestoreReq) (OpenResp, error)
    Close(ctx context.Context, handle string) error
    Health(ctx context.Context) (WorkerAdvert, error)
}
```

`PushReq` carries an `audio.Chunk` — opaque bytes plus a seq span. No `Replay`: replay is `Push` over a span the worker may already have consumed, made safe by `last_seq_applied`. No `CreateState`: that is `Open`. The coordinator never sees a tensor, a `ModelState`, or a sample.

**2. `worker/adapters/base.Adapter` — Python, worker side, in-process.** Never crosses a network. Receives an opaque buffer plus the session's declared format; any decoding it needs is its own business.

```python
@dataclass(frozen=True)
class Capabilities:
    streaming: bool          # can accept incremental chunks mid-utterance
    serializable: bool       # warm-checkpoint tier available
    endpointing: bool        # native endpoint detection
    modes: frozenset[Mode]   # {ONLINE, OFFLINE}
    min_chunk_ms: int
    max_chunk_ms: int

class Adapter(Protocol):
    def capabilities(self) -> Capabilities: ...
    def compatibility_key(self) -> CompatKey: ...
    def create_state(self, session_id: str) -> State: ...
    def infer(self, audio: AudioBuffer, st: State) -> tuple[Delta, State]: ...
    def finalize(self, st: State) -> Delta: ...
    def serialize(self, st: State) -> bytes: ...      # raises NotSupported
    def deserialize(self, blob: bytes) -> State: ...  # raises NotSupported
```

`capabilities()` is what makes a heterogeneous fleet tractable. Without it the router cannot tell that worker D is a perfectly healthy backend that must never receive a mid-utterance chunk.

**3. The worker HTTP server** — owns `handle → State`, applies generation compare-and-commit, calls the adapter. `expected_generation` is enforced here and nowhere else.

### Adapter registry and conformance

Five adapters is where bespoke implementations rot. Two structures prevent it:

- `adapters/registry.py` — name → constructor, selected by `ADAPTER=mock|zipformer|zipformer_ctc|whisper_ct2|whisper_onnx`. Adding a model is one registry entry and one file.
- `tests/test_adapter_conformance.py` — one parametrized suite every adapter must pass: key stability across calls, state isolation between sessions, idempotent re-infer, `finalize` after `infer`, `NotSupported` raised honestly when `serializable` is false, declared `Capabilities` matching observed behaviour.

The doc's test still holds and gets stronger: swapping any model is one env var and touches no Go code.

### Capability-aware routing

`Pick` gains a filter stage before the doc's scoring:

```go
func (r *Router) Pick(mode Mode, exclude map[string]bool, prefer CacheCompatibilityKey) (Worker, error) {
    // 1. FILTER — hard constraints, no scoring
    //    usable health, rate-limit budget, memory headroom,
    //    caps.Streaming || mode == Offline,
    //    mode ∈ caps.Modes
    // 2. SCORE — outstanding × latencyP95, halved when key == prefer
}
```

If the filter empties, that is `ErrNoCapacity` and an `overloaded` event — not a silent downgrade to a backend that cannot serve the traffic.

### Journal and replay

The journal stores opaque payloads and metadata. It never inspects audio:

```go
type Record struct {
    Seq        uint64
    DurationMs float64
    Voiced     bool
    Payload    []byte    // opaque
}

type Span struct {
    ChunkID  uint64
    SeqStart uint64
    SeqEnd   uint64
}
```

Recovery replays **the dispatch log of spans**, handing them to `audio.Recut` to reproduce the original chunk boundaries. This reproduces byte-for-byte the call sequence the dead worker saw — which matters because a streaming encoder's state depends on how audio was segmented, not only on which samples it contained. Replaying raw journal order would feed silence the original never saw and produce a divergent state.

## Repo layout

```
Makefile  docker-compose.yml  docker-compose.ha.yml  README.md
docs/     build-plan.md (kept as the design reference)
          PROTOCOL.md  DECISIONS.md  implementation-plan.md
          compat-matrix.md (generated by `make matrix`)
corpus/   clips + transcripts.json (committed; known text = assertable correctness)
gateway/  cmd/gateway/main.go
          internal/audio/      ← ALL sample-level code lives here, nowhere else
          internal/{wire,session,journal,router,backend,coord,metrics,dash}
worker/   server.py  supervisor.py  state.py
          adapters/{base,registry,mock,zipformer,zipformer_ctc,whisper_ct2,whisper_onnx}.py
          tests/test_adapter_conformance.py
loadgen/  main.go            # Go, imports gateway/internal/wire
scripts/  demo_kill.sh  run_all_scenarios.sh  scenarios/NN_*.sh
          bench_*.sh  make_matrix.py  make_charts.py
models/   fetch.sh           # build-time only, never at run time
bifrost/  config.json
```

Note what is *absent*: no `vad/`, no `chunker/`, no `reframe/` as peers of `session/`. Those are internals of `audio/`. A reviewer sees the boundary in the directory listing.

`loadgen` is Go and imports the gateway's `wire` package — avoids a second codec drifting from the first, and Go drives 200 paced streams on tickers where Python would become the bottleneck being measured.

## Milestones

Day numbers are sequencing, not a schedule promise. Each ends with a command that proves it.

### M0 — prerequisites and skeleton (day 0.5)

`brew install colima docker docker-compose && colima start --cpus 12 --memory 20 --arch aarch64`. Go module, Python package, Makefile, compose with gateway + workers serving `/health` and nothing else. Pinned `deploy.resources.limits` from the start. `restart: "no"` on workers, or Docker restores them behind your back and the recovery demo lies.

**Done:** `docker compose up` → all healthchecks green; `make down` clean.

### M1 — protocol, codec, one stream end to end (days 1–1.5)

`docs/PROTOCOL.md` written **before** any code. Wire codec with declared duration validated against actual payload length. Sequence validation, three-way switch. WS server with `readLoop`/`sessionLoop`/`writeLoop` on bounded channels. A pass-through `audio.Pipeline` (no VAD, fixed chunking) so the boundary exists from the first commit and is filled in later. Worker HTTP over the mock adapter. `SessionInferenceState`, `CacheCompatibilityKey`, the three counters.

**Done:** `make smoke` — one stream end to end from a corpus clip; a dropped frame produces `discontinuity`; a frame whose declared duration disagrees with its payload is rejected.

### M2 — journal, dispatch log, emission contract (days 2–3)

Ring journal over opaque payloads with the `Record`/`Span` split. Utterance lifecycle. Emission rules with immutability and finals-dedupe in exactly one place. `ack` events.

**Done:** unit tests for journal (append, `ReadAfter`, `ReadFromCommitted`, trim with retention honoured, wraparound) and emission (revision strictly increases, final locks the utterance, duplicate seq and duplicate final are no-ops). A client batching 3 frames per message yields the same chunk count as one sending singles — the boundary contract test that proves accumulation is by duration, not frame count.

### M3 — failover, both modes (days 3–4.5) ← the thesis

Router with the filter/score split, session pinning. Health with latency ejection against cluster p95, plus probing recovery. Fault injection via supervisor (`POST /admin/die|restore|slow|blackhole|429|corrupt`). Checkpointing on mock. Both recovery algorithms as a bounded loop. Two mock deployments with different keys suffice; real models come later.

**Done:** chaos scenarios 2, 3 and 5 pass headlessly with non-zero exit on breach, `duplicate_finals_total == 0` in all three. **If the schedule slips anywhere, it does not slip here.**

### M4 — the audio pipeline (day 5)

Fill in the boundary established at M1: library-backed VAD, reframing to whatever window it requires, hysteresis, pre-roll retention, duration-accumulating chunk cutting, `Recut`. All of it inside `internal/audio`. Thresholds go to config with chosen values recorded in the README.

**Done:** 60s of silence produces ≈0 backend calls; speech onset is not clipped — asserted by comparing the transcript's first word against the corpus's known text, not by listening. **And: no test outside `internal/audio` changed in this milestone.** If one did, the boundary leaked and that is the bug to fix before moving on.

### M5 — the model fleet (days 5.5–7.5)

Adapter registry, `Capabilities`, conformance suite — built *before* the adapters. Then the four real adapters in dependency order: `zipformer` (streaming path), `zipformer_ctc` (a second state shape), `whisper_ct2` (non-streaming), `whisper_onnx` (the runtime-mismatch pair). `models/fetch.sh` runs at **build** time; weights bake into images, never download at run time.

**Done:** conformance suite green for all five adapters; the M3 chaos suite passes with `ADAPTER=zipformer`; RTF measured per adapter and recorded; the `serializable == False` degradation appears in the event log.

### M6 — heterogeneous routing and the matrix (days 7.5–8.5)

Capability filtering wired into `Pick`. Compose profiles. `make matrix` generating `docs/compat-matrix.md`. Chaos scenarios extended to the new pairs: cross-family (a→c), same-weights-different-runtime (d→e), and streaming-required-but-unavailable (a→d refused).

**Done:** the matrix generates from the running fleet; all pair scenarios pass; a→d is refused with `overloaded` rather than silently mis-served.

### M7 — load, backpressure, rate limits, Bifrost (days 8.5–9.5)

Paced loadgen with `--streams --ramp --speech-ratio --jitter-ms --drop-pct --reconnect-every --mode --out`. Admission control in the doc's five-step degradation order. Token buckets with headroom, `Retry-After` honoured, no sleeping on the online path. Bifrost for finals and offline, pointed at worker D's OpenAI-compatible endpoint so the stack stays hermetic.

**Done:** scenarios 4, 6, 7, 9, 10, 11 pass. `--speech-ratio 1.0` vs `0.5` shows backend call volume halving — the VAD economic argument, measured at the boundary rather than asserted.

### M8 — benchmarks and charts (day 10)

Benchmarks A–F. **B and C run on mock**, where checkpointing is real; the README says so plainly rather than implying real models checkpoint. Benchmark C expands from a pair to the fleet matrix. Four charts: latency over time with the kill marked, latency vs concurrency, inference ms vs utterance length with cache on/off, recovery time by mode.

**Done:** `make bench` produces CSVs and the four charts unattended.

### M9 — dashboard and HA profile (day 11, hard timebox)

Static HTML at `/dashboard`, SSE at `/api/events`, canvas charts, client-side filtering, control plane over the existing chaos endpoints. Nodes render their compatibility key hash so cross-model failover is *visible* as a key mismatch. HA profile: nginx L4 `least_conn`, two gateways, client-side replay ring.

**Done:** click worker → Kill → watch latency spike and recover. **One day, hard.** If it is not working by end of day, ship the static charts.

### M10 — writeup (day 11.5)

README with the thesis in four sentences, the numbers, the compatibility matrix, the "not implemented, and why" list. Fresh-clone test with the build cache cleared.

**Schedule honesty:** the fleet adds roughly 1.5 days over a two-worker build, pushing the total to ~11.5 days against a 10-day fortnight. The dashboard is therefore genuinely at the cut line, not nominally. That is the right trade — the matrix is evidence, the dashboard is presentation — but it should be a decision, not a week-two surprise.

## Invariant → test map

The doc states 16 invariants and never says how any is checked. Each gets a named test, and the audio ones are tested at the boundary rather than inside it.

| Inv | Test |
|---|---|
| 1 duration never inferred from arrival | `audio`: declared-vs-payload mismatch rejected; batched-frames contract test |
| 2 never call model per frame | `audio`: chunks emitted == `ceil(total_ms/chunk_ms)` regardless of framing |
| 3 one state owner | `router`: pinning test; concurrent `Open` for one session rejected |
| 4 applying seq is idempotent | `worker`: replayed push returns cached delta, generation unchanged |
| 5 generation-checked mutation | `worker`: stale `expected_generation` → `ErrStaleStateWrite` |
| 6 exact compatibility match | `CanRestore` table test across all key fields; d→e asserts mismatch |
| 7 model change → fresh state | scenarios 3 and d→e assert no deserialize attempted |
| 8 audio replay always available | scenario 5: corrupt checkpoint → replay → session succeeds |
| 9 finals immutable | `session`: any event after final for an utterance panics in test builds |
| 10 partials replaced wholesale | contract test on revision monotonicity across reset |
| 11 epochs never collapse | `session`: reconnect bumps `stream_epoch` only; failover bumps `failover_epoch` only |
| 12 stale failover epoch rejected | `coord`: late response from dead worker dropped |
| 13 checkpoint failure non-fatal | fault-inject checkpoint store; inference latency unchanged |
| 14 realtime not starved | benchmark D: p99 online latency with/without offline load |
| 15 bounded retries | scenario 4 asserts retry ceiling, no storm |
| 16 bounded queues | loadgen at 150% → `overloaded`, no unbounded memory growth |
| — capability routing | a→d refused; `ErrNoCapacity` not a silent downgrade |
| — boundary integrity | `Recut(spans)` reproduces the original dispatch byte-for-byte |

## Verification

```bash
make up          # docker compose up
make smoke       # M1: one stream end to end
make test        # Go unit tests + pytest + adapter conformance
make matrix      # regenerate docs/compat-matrix.md from the running fleet
make demo        # kill a worker under load, produce the chart
make chaos       # all scenarios, non-zero exit on assertion breach
make bench       # benchmarks A-F, CSVs + charts
make down
```

End-to-end acceptance is the doc's own README opening, run from a clean clone on a machine that is not this one:

```
docker compose up
open http://localhost:7000
click "Start 200 streams"
click worker-a → Kill
```

`make chaos` and `make matrix` are the verifiable artifacts for a reviewer who never opens the UI.

## Risks and the cut line

| Risk | Pre-decided response |
|---|---|
| sherpa-onnx has no serializer (**confirmed**) | Checkpoint tier real on mock only; real adapters demonstrate degradation. Stated up front, not discovered late |
| Audio work expands to fill the schedule | It is one package with a fixed interface, scheduled after the thesis. If M4 overruns, the M1 pass-through pipeline still ships a working system |
| Five adapters is five times the integration surface | Registry + conformance suite built in M5 *before* the adapters. Any adapter that fights the suite gets dropped, not special-cased |
| CPU budget at 5–6 workers | Pinned limits, 20M zipformer rather than full-size, mock at 0.5 vCPU. `--profile models` is opt-in |
| sherpa CPU RTF too slow for 20 streams | Drop the concurrency target and report the real number; RTF is measured, not promised |
| Bifrost integration eats a day | Flag-gated with direct fallback. **First thing cut** |
| Dashboard becomes the project | Hard one-day timebox at M9, after headless scripts already pass |
| ~11.5 days of work in a 10-day fortnight | Cut order: Bifrost → dashboard → HA profile → `worker-e` (keep the design note) → benchmarks D/E/F → advanced VAD (keep the M1 pipeline). **M1–M3 are never cut** |

If everything slips, the fallback is the doc's own: M1, M2, M3 on mock adapters. Working failover under load beats real models with hand-wavy fault tolerance, and it matches what was asked, since transcription quality was explicitly excluded from grading.

## First action on approval

Create the repo skeleton and `git init` (this directory is not a repository yet), install Colima, and land `docs/implementation-plan.md` alongside the existing design doc so both are version-controlled together.

Sources for the verified findings: [sherpa-onnx OnlineStream bindings](https://github.com/k2-fsa/sherpa-onnx/blob/master/sherpa-onnx/python/csrc/online-stream.cc), [OnlineRecognizer bindings](https://github.com/k2-fsa/sherpa-onnx/blob/master/sherpa-onnx/python/csrc/online-recognizer.cc), [streaming zipformer CTC models](https://k2-fsa.github.io/sherpa/onnx/pretrained_models/online-ctc/zipformer-ctc-models.html), [Whisper ONNX export](https://k2-fsa.github.io/sherpa/onnx/pretrained_models/whisper/export-onnx.html), [runtime disagreement on identical weights](https://github.com/k2-fsa/sherpa-onnx/issues/2900), [Bifrost](https://github.com/maximhq/bifrost).
