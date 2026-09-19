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

- [x] `docs/PROTOCOL.md` (written before any wire code, per the plan) — full contract, with an explicit "wired at M1" column per event so it won't need rewriting each milestone
- [x] `internal/wire`: binary frame codec; declared duration validated against actual payload length; `ValidateSessionStart` enforces the locked format
- [x] Sequence validation (three-way switch: expected / duplicate / gap → `discontinuity`) — `internal/session.SeqValidator`
- [x] WS server: `readLoop` / `sessionLoop` / `writeLoop` on bounded channels (`cmd/gateway/conn.go`)
- [x] Pass-through `internal/audio.Pipeline` — chunking is real (duration-accumulated, invariant 2 holds), only VAD is deferred to M4; `Flush()` added for session-end tail dispatch
- [x] Worker HTTP surface (`open`/`push`/`flush`/`restore`/`close`) over the mock adapter; `worker/adapters/base.py` + `mock.py` + `registry.py`, `worker/state.py` (generation compare-and-commit)
- [x] `SessionInferenceState`, `CacheCompatibilityKey` (opaque token, not the 7-field struct — see below), the three counters (stream/failover epoch, generation)
- [x] Emission contract enforced, not just documented: `internal/session.Emitter` — revision monotonicity, final immutability (panics on violation in test builds), idempotent final re-delivery
- [x] `corpus/`: 10 known-text clips generated via `scripts/gen_corpus.sh` (macOS `say`), committed; `internal/corpus` WAV reader (shared with `cmd/loadgen` at M7)
- [x] `cmd/smoketest` + `scripts/smoke.sh`: real end-to-end verification against the containerized stack (not the fake worker `cmd/gateway`'s own tests use)
- [x] **Done-when, verified via `make smoke` against the real compose stack:** one stream runs end to end from a corpus clip (tested against 4 different clips incl. the multi-chunk monologue); a dropped frame produces `discontinuity`; a frame whose declared duration disagrees with its payload produces `error` and the session terminates
- [x] Go suite: 60+ tests across `wire`/`audio`/`session`/`backend`/`cmd/gateway`/`internal/corpus`, `-race` clean over repeated runs; Python suite: 19 tests (adapter conformance, generation compare-and-commit incl. a real concurrent-writers test, full server integration)

**Design refinement made while implementing:** `CacheCompatibilityKey` on the Go side ended up as an opaque comparable string, not the 7-field struct build-plan.md's Go sample embeds. Carrying the full struct (`ModelFamily`, `Runtime`, `Dtype`, ...) into the gateway would mean the gateway knows a backend is a model — precisely what build-plan.md's own boundary rule 2 forbids. The full struct lives worker-side only (`worker/adapters/base.py`'s `CompatKey`); the Go side only ever compares the hash it produces. Documented in `internal/session/state.go`.

**Bugs found and fixed during verification** (the kind `make smoke` against a real stack catches and a fake-worker unit test can't):

| Bug | Where | Fix |
|---|---|---|
| Restore's checkpoint blob went into a JSON string via a raw byte→string cast — lossy for binary data, despite `PROTOCOL.md` already saying "base64 here" | `internal/backend.Client.Restore` | base64-encode/decode properly; strengthened the test to use deliberately invalid-UTF-8 bytes so this class of bug can't silently pass again |
| `worker/Dockerfile` never ran `pip install`, and didn't copy the new `state.py` — both stale from the M0 stdlib-only version | `worker/Dockerfile` | install from `pyproject.toml`, copy `state.py` |
| **Goroutine shutdown race:** both `sessionLoop` and `readLoop` called `cancel()` on their own early-return paths; that cancellation could unblock/abort a goroutine *before* `writeLoop` finished flushing the very error/final event the return just produced — intermittently dropped the last event before the client could read it | `cmd/gateway/conn.go` | only `handleConnection` cancels, and only after `<-done` confirms `writeLoop` fully drained; caught by running the integration tests with `-race -count=30`, not a single run |
| **Client-side `coder/websocket` gotcha:** canceling a `Read`'s context (e.g. a short timeout used to "poll without blocking") closes the whole connection, not just that call — `cmd/smoketest`'s original drain loop did exactly this between every audio frame | `cmd/smoketest/main.go` | replaced with a dedicated reader goroutine on the connection's one long-lived context feeding a channel, drained via non-blocking `select`/`default` — the same shape `cmd/gateway`'s own `writeLoop` already used, which is why the server side never had this bug |

## M2 — journal, dispatch log, emission contract

- [x] Ring journal over opaque payloads: `internal/journal` — fixed-capacity, wraparound-tested, `Append`/`ReadAfter`/`ReadFromCommitted`/`TrimBefore`
- [x] Utterance lifecycle: `InferenceState.NewUtterance()` — resets the per-utterance emission counters, session-wide `seq`/`Generation` untouched
- [x] Emission rules: immutability + finals-dedupe in one place, now correctly **per-utterance** rather than session-wide (a real latent bug found and fixed this milestone — see below); `ack` event was already wired at M1
- [x] Journal wired into `cmd/gateway`'s hot path: every accepted audio frame appended (including silence, since `Voiced` is always true until M4's VAD lands); `TrimBefore` called with the final's own `seq_end` when it locks
- [x] Unit tests: journal — append, `ReadAfter`, `ReadFromCommitted`, `TrimBefore` (incl. trimming beyond what's retained, and a floor set before wraparound physically evicts it anyway), wraparound (single wrap and repeated full cycles)
- [x] Unit tests: emission contract — revision monotonic (M1), final locks the utterance (M1), duplicate seq/final = no-op (M1), **and now**: two sequential utterances are fully independent (revision resets, a new utterance's partials don't panic against the old one's final, dedupe follows the current utterance, not a stale one)
- [x] Integration test tying M1+M2 together: journal-stored records fed through `audio.Pipeline.Recut` reproduce the exact chunk boundaries live dispatch produced (`TestJournalRecordsFeedRecutCorrectly`)
- [x] Boundary contract test (batched vs. single frames → same chunk count): already built and passing at M1 (`TestChunkCountIndependentOfFraming`)
- [x] Full regression + real end-to-end `make smoke` against the containerized stack, re-verified after wiring the journal into the hot path

**Design refinement made while implementing:** the plan's wording suggested a separate "dispatch log" of `audio.Span` values alongside the record ring. Building M2 surfaced that this would be redundant — M1's `Recut` already proved (`TestRecutReproducesLiveDispatch`) that replaying raw `Record`s alone deterministically reproduces the original chunk boundaries, *provided* replay only starts where the live accumulator was empty. Both of this system's actual replay entry points (a checkpoint, taken right after a chunk commits; a committed final, reached right after `Flush()` empties the accumulator) satisfy that by construction. So `Journal` + `Recut` together already deliver what a separately persisted span log would have — see `internal/journal`'s package doc for the full reasoning. Nothing in M3's planned failover algorithms needs anything more than `ReadAfter`/`ReadFromCommitted` feeding `Recut`.

**Bug found and fixed while implementing** (the same "verify end to end" discipline as M1 — this one caught by re-reading M1's own code before building on it, not by a test failing): `Emitter`'s `Finalized`/`lastFinal` were session-wide. Once *any* utterance finalized, every later utterance's `Partial()` would have panicked forever — harmless today (M1/M2 never has more than one utterance per session) but a real, silent trap for M4. Fixed by scoping both to the current utterance via `NewUtterance()`'s reset, proven by `TestUtterancesAreIndependentAfterNewUtterance`.

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
| 1 | Three conflicting backend interfaces | M1 (`backend.Client` + `Adapter`) | [x] `internal/backend.Client` (Go↔worker) and `worker/adapters/base.Adapter` (in-process) are the only two, verified compatible end-to-end via `make smoke` |
| 2 | Sample-level DSP scattered outside one boundary | M1 (real chunking) / M4 (VAD) | [x] duration-based chunk cutting is real and tested (invariant 2 holds); VAD gating alone deferred to M4 |
| 3 | Replay undefined over silence | M1 (`Recut` skips non-voiced) + M2 (journal retains everything) | [x] journal stores every record, voiced or not (`TestSilenceNeverEntersChunkContent` + journal's own completeness); no separate dispatch-log structure needed — see M2's design refinement below |
| 4 | Chunk boundaries not preserved on replay | M1 (`Recut`) + M2 (journal integration proven) | [x] `TestJournalRecordsFeedRecutCorrectly` confirms journal-stored records feed `Recut` and reproduce live dispatch exactly |
| 5 | `audio_b64` contradicts the doc's own base64 rejection | M1 (binary body) | [x] `Push` is a true binary body; verified against the real worker, not just the fake one |
| 6 | Unbounded recursion in `handleBackendFailure` | M3 (bounded loop) | [ ] |
| 7 | No `ack` event for client-side replay | M2 | [x] early: `ack` implemented and wired at M1 (piggybacks session_id delivery on session.start's ack too) |
| 8 | `TrimBefore` can discard pre-roll | M2 (current scope) / M4 (full formula) | [x] safe for M1/M2's actual shape — one utterance per session, trimmed once at its own final, no "still-open utterance" case can exist yet; the full `min(committedSeq, openUtteranceStart)` formula is only meaningful once M4 allows a new utterance to already be open when an earlier one's final commits |
| 9 | "Continue without reset" needs unsafe text diffing | M3 (always reset) | [ ] |
| 10 | No finals-dedupe structure specified | M2 | [x] `Emitter`'s dedupe is now correctly per-utterance (not session-wide — a real bug found and fixed this milestone, see below), proven by `TestUtterancesAreIndependentAfterNewUtterance` |
| 11 | Non-streaming adapters have no capability signal | M5/M6 (`Capabilities`) | [x] early: `Capabilities` struct implemented and returned from `open` at M1, ahead of its M6 router caller |
| 12 | Thesis scheduled as phase 5 of 7 | M3 (moved up) | [x] resequenced in the plan |

## Open risks (not yet decisions — watch these)

- sherpa-onnx real-model RTF on this machine is unmeasured until M5; concurrency target (20 streams) may need revising down once measured.
- **Docker Desktop memory is tight.** Checked via `docker info`: 14 CPUs (fine) but only **~7.75 GiB** RAM allocated to the VM, no CLI to resize it. The planned default fleet (gateway + 5 workers, M0 limits) budgets ~8 GiB at the *limits* — already at the edge before M5 adds real model weights on top of worker-a/b/c/d. Action before M5: raise Docker Desktop's memory allocation (Settings → Resources) to 16 GiB+, or trim per-worker limits once real memory footprints are measured. Not a blocker today (M0's stub workers use a fraction of their limits).
- **Standing gotcha for any future `coder/websocket` client code** (`cmd/loadgen` at M7, the dashboard's control plane at M9, any chaos script that speaks WS directly): canceling a `Read`'s context — including via a short per-call timeout used to "check without blocking" — closes the whole connection, not just that call. Found and fixed once already in `cmd/smoketest` (see M1's bugs-found table). The safe shape is always: one dedicated reader goroutine on the connection's full-lifetime context, feeding a channel; drain that channel with a non-blocking `select`/`default`, never a fresh short-lived context per poll.
- Minor: `worker`'s pytest run emits a `StarletteDeprecationWarning` about `httpx`/`starlette.testclient` (`pip install httpx2` suggested). Not failing anything; worth a look if it becomes a hard error in a future FastAPI/Starlette bump.
