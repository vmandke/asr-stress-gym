# Status

Living tracker for [implementation-plan.md](implementation-plan.md).
Updated as work lands — checked items are **verified working**, not just
written. See that doc for why each item exists; see
[build-plan.md](build-plan.md) for the underlying design.

Legend: `[x]` done and verified · `[~]` in progress · `[ ]` pending

## M0 — prerequisites and skeleton

- [x] Repo initialized (`git init`), directory skeleton matches the planned layout
- [x] Go module (`asr-stress-gym`), builds clean (`go build ./...`, `go vet ./...`)
- [x] Package doc stubs for every `internal/*` package (boundary + milestone documented, no logic ahead of schedule)
- [x] Gateway binary: `/health` endpoint
- [x] Worker: stdlib HTTP server, `/health` advertises `WorkerAdvert` shape (worker_id, model, compatibility_key_hash, ...)
- [x] Dockerfiles: gateway, loadgen, worker (one image, env-differentiated per fleet member)
- [x] `docker-compose.yml`: gateway + worker-mock/a/b/c/d, resource limits pinned, `restart: "no"`, healthcheck-based `depends_on`
- [x] `docker compose up --build` → **all 6 services healthy**, gateway `/health` responds, `docker compose down -v` clean — verified via `scripts/verify_m0.sh`
- [x] `docs/DECISIONS.md` — locked decisions transcribed from planning
- [x] `docs/implementation-plan.md` landed alongside `build-plan.md`
- [x] `scripts/check_env.sh`, `scripts/verify_m0.sh` — setup/verification captured as checked-in scripts, not ad hoc commands
- [x] Known issue fixed: Homebrew `docker` CLI can't see Docker Desktop's credential helper → PATH fix in `check_env.sh` (self-contained, no system file touched)
- [x] Known issue fixed: host port 7000 collides with macOS AirPlay Receiver → `GATEWAY_DASHBOARD_PORT` override + README troubleshooting note
- [ ] `worker-e`, Bifrost, HA profile — deliberately **not** in `docker-compose.yml` yet; land at M6/M7/M9 respectively, not stubbed early

**Everything above is verified, not just written.** M0 is done.

## M1 — protocol, codec, one stream end to end

- [ ] `docs/PROTOCOL.md` (written before any wire code, per the plan)
- [ ] `internal/wire`: binary frame codec; declared duration validated against actual payload length
- [ ] Sequence validation (three-way switch: expected / duplicate / gap → `discontinuity`)
- [ ] WS server: `readLoop` / `sessionLoop` / `writeLoop` on bounded channels
- [ ] Pass-through `internal/audio.Pipeline` (no VAD yet — establishes the boundary, M4 fills it in)
- [ ] Worker HTTP surface (`open`/`push`/`flush`/`close`) over the mock adapter; `worker/adapters/base.py` + mock adapter
- [ ] `SessionInferenceState`, `CacheCompatibilityKey`, the three counters (stream/failover epoch, generation)
- [ ] **Done-when:** `make smoke` — one stream end to end from a corpus clip; dropped frame → `discontinuity`; mismatched declared-vs-actual duration → rejected

## M2 — journal, dispatch log, emission contract

- [ ] Ring journal over opaque payloads (`Record`/`Span` split, defect #2/#3 fix)
- [ ] Utterance lifecycle
- [ ] Emission rules: immutability + finals-dedupe in one place (defect #10 fix); `ack` event (defect #7 fix)
- [ ] Unit tests: journal (append, `ReadAfter`, `ReadFromCommitted`, trim w/ retention, wraparound)
- [ ] Unit tests: emission contract (revision monotonic, final locks utterance, duplicate seq/final = no-op)
- [ ] Boundary contract test: batched vs. single frames → same chunk count

## M3 — failover, both modes (the thesis)

- [ ] Router: filter/score `Pick`, session pinning
- [ ] Health: latency ejection vs. cluster p95, probing recovery
- [ ] Fault injection via supervisor (`die`/`restore`/`slow`/`blackhole`/`429`/`corrupt`)
- [ ] Checkpointing on mock adapter
- [ ] Both recovery algorithms as one bounded loop (defect #6 fix — no unbounded recursion)
- [ ] **Done-when:** chaos scenarios 2, 3, 5 pass headlessly, non-zero exit on breach, `duplicate_finals_total == 0` in all three

## M4 — the audio pipeline

- [ ] Library-backed VAD behind `audio.VAD`
- [ ] Reframing, hysteresis state machine, ~150ms pre-roll
- [ ] Duration-accumulating chunk cutting, `Recut`
- [ ] **Done-when:** 60s silence → ≈0 backend calls; onset not clipped (checked against known corpus text)
- [ ] **Boundary check:** no test outside `internal/audio` changed this milestone

## M5 — the model fleet

- [ ] `worker/adapters/registry.py` + `Capabilities` dataclass
- [ ] `tests/test_adapter_conformance.py` (written **before** the adapters)
- [ ] Adapter: `zipformer` (sherpa-onnx, streaming) — confirm `serializable == False` in practice
- [ ] Adapter: `zipformer_ctc` (sherpa-onnx, second state shape)
- [ ] Adapter: `whisper_ct2` (ctranslate2, non-streaming)
- [ ] Adapter: `whisper_onnx` (onnxruntime — same weights as `whisper_ct2`, different runtime)
- [ ] `models/fetch.sh`, weights baked at image build time
- [ ] **Done-when:** conformance suite green ×5; M3 chaos suite passes with `ADAPTER=zipformer`; RTF measured per adapter; degradation path visible in the event log

## M6 — heterogeneous routing and the matrix

- [ ] Capability filtering wired into `Pick` (defect #11 fix)
- [ ] Compose profiles (`models` adds `worker-e`)
- [ ] `make matrix` → generates `docs/compat-matrix.md` from the running fleet
- [ ] Chaos: a→c (cross-family), d→e (same weights, different runtime), a→d (refused, non-streaming)

## M7 — load, backpressure, rate limits, Bifrost

- [ ] `cmd/loadgen`: paced client, all flags (`--streams --ramp --speech-ratio --jitter-ms --drop-pct --reconnect-every --mode --out`)
- [ ] Admission control, five-step degradation order
- [ ] Token buckets w/ headroom, `Retry-After` honored, no sleep on online path
- [ ] Bifrost wired to worker-d's OpenAI-compatible endpoint, `BIFROST_ENABLED` flag + direct fallback
- [ ] **Done-when:** scenarios 4, 6, 7, 9, 10, 11 pass; speech-ratio 1.0 vs 0.5 halves backend call volume

## M8 — benchmarks and charts

- [ ] Benchmarks A (cache vs no-cache), B (cold replay vs checkpoint — mock only), C (same- vs cross-model — full matrix), D (offline interference), E (rate limiting), F (capacity)
- [ ] Four charts: latency-over-time w/ kill marker, latency-vs-concurrency, inference-ms-vs-utterance-length, recovery-time-by-mode

## M9 — dashboard and HA profile (hard 1-day timebox)

- [ ] Static `/dashboard`, `/api/events` SSE, canvas charts, client-side filtering
- [ ] Control plane over existing chaos endpoints; compat-key-hash visible per node
- [ ] `docker-compose.ha.yml`: nginx L4 `least_conn`, two gateways, client-side replay ring
- [ ] **Fallback if timeboxed out:** ship the static charts from M8, skip the UI

## M10 — writeup

- [ ] README: thesis in four sentences, real numbers, compatibility matrix, "not implemented, and why"
- [ ] Fresh-clone test, build cache cleared, on the exact `docker compose up` → click sequence

## Defects from the design doc — tracked to the milestone that fixes them

| # | Defect | Fixed at | Status |
|---|---|---|---|
| 1 | Three conflicting backend interfaces | M1 (`backend.Client` + `Adapter`) | [ ] |
| 2 | Sample-level DSP scattered outside one boundary | M1 (stub) / M4 (real) | [x] boundary exists; [ ] filled in |
| 3 | Replay undefined over silence | M2 (dispatch log) | [ ] |
| 4 | Chunk boundaries not preserved on replay | M2 (`Recut`) | [ ] |
| 5 | `audio_b64` contradicts the doc's own base64 rejection | M1 (binary body) | [ ] |
| 6 | Unbounded recursion in `handleBackendFailure` | M3 (bounded loop) | [ ] |
| 7 | No `ack` event for client-side replay | M2 | [ ] |
| 8 | `TrimBefore` can discard pre-roll | M2 | [ ] |
| 9 | "Continue without reset" needs unsafe text diffing | M3 (always reset) | [ ] |
| 10 | No finals-dedupe structure specified | M2 | [ ] |
| 11 | Non-streaming adapters have no capability signal | M5/M6 (`Capabilities`) | [ ] |
| 12 | Thesis scheduled as phase 5 of 7 | M3 (moved up) | [x] resequenced in the plan |

## Open risks (not yet decisions — watch these)

- sherpa-onnx real-model RTF on this machine is unmeasured until M5; concurrency target (20 streams) may need revising down once measured.
- **Docker Desktop memory is tight.** Checked via `docker info`: 14 CPUs (fine) but only **~7.75 GiB** RAM allocated to the VM, no CLI to resize it. The planned default fleet (gateway + 5 workers, M0 limits) budgets ~8 GiB at the *limits* — already at the edge before M5 adds real model weights on top of worker-a/b/c/d. Action before M5: raise Docker Desktop's memory allocation (Settings → Resources) to 16 GiB+, or trim per-worker limits once real memory footprints are measured. Not a blocker today (M0's stub workers use a fraction of their limits).
