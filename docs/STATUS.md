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

- [x] Router: filter/score `Pick`, capability filtering, session pinning, reactive error/latency health and single-probe recovery
- [x] Fault injection: process-level supervisor (`die`/`restore`) plus mock-worker controls (`slow`/`blackhole`/`429`/`corrupt`)
- [x] Gateway-side asynchronous mock checkpoints; validation failure degrades safely to audio replay
- [x] Both recovery algorithms in one bounded loop (defect #6 fix — no unbounded recursion); failed replacement attempts restore the prior owner and close their temporary handle
- [x] **Done-when, verified against the real Compose fleet:** `make chaos` runs scenarios 2, 3, and 5 headlessly, exits non-zero on a breach, and asserts `duplicate_finals_total == 0` in each. The runner resets worker children and the gateway's in-memory health view between scenarios, because each scenario intentionally changes it.

## M4 — the audio pipeline

- [ ] Library-backed VAD behind `audio.VAD`
- [ ] Reframing, hysteresis state machine, ~150ms pre-roll
- [ ] Duration-accumulating chunk cutting, `Recut`
- [ ] **Done-when:** 60s silence → ≈0 backend calls; onset not clipped (checked against known corpus text)
- [ ] **Boundary check:** no test outside `internal/audio` changed this milestone

## M5 — the model fleet

- [x] `worker/adapters/registry.py` + `Capabilities` dataclass — registry entries are now `(module, class)` resolved lazily, so a worker pays for the one adapter `ADAPTER` names, not all five
- [x] `tests/test_adapter_conformance.py` (written **before** the adapters) — now exercises a real corpus clip rather than synthetic silence; two new cases below
- [x] Adapter: `zipformer` (sherpa-onnx, streaming transducer) — `serializable == False` confirmed against the real bindings, not assumed
- [x] Adapter: `conformer_ctc` (sherpa-onnx, second state shape) — **substituted for the planned `zipformer_ctc`**; see DECISIONS.md "M5 deviations"
- [x] Adapter: `whisper_ct2` (ctranslate2, non-streaming)
- [x] Adapter: `whisper_onnx` (ONNX Runtime — same weights as `whisper_ct2`, different runtime)
- [x] `models/fetch.sh`, weights baked at image build time; `.dockerignore` added so a developer's local weights are never shipped into the build context in place of the ones fetch.sh puts there
- [x] `docs/RTF.md` generated by `scripts/measure_rtf.py`
- [x] Worker `/health` reports a real `rtf_p50` from a bounded rolling window (was hardcoded `null`)
- [x] Inference moved off the event loop (`asyncio.to_thread`) — required now that a decode blocks in C++ for its whole duration
- [x] Gateway no longer asks a non-serializable backend for a checkpoint on every push — four of six workers now answer 501 permanently, so that became the common case rather than an edge one
- [x] **Done-when, all four bars met:** conformance suite green ×5; RTF measured per adapter (`docs/RTF.md`); chaos scenarios 2, 3 and 5 pass against the real fleet with `ADAPTER=zipformer`; the `serializable == False` degradation is visible as a counter, not a claim

**M5 is done.** The fleet runs real models end to end: `make smoke` passes
against containerized sherpa-onnx, and `make chaos` passes all three
scenarios with `duplicate_finals_total == 0` throughout.

### The degradation, as a number

The single most useful thing M5 produced. Metrics from one same-model
failover (worker-a → worker-b, both `ADAPTER=zipformer`) on a freshly
recreated gateway:

```json
{ "failover_total": 1,
  "failover_same_model_total": 1,   "failover_cross_model_total": 0,
  "checkpoint_restores_total": 0,   "checkpoint_degraded_total": 1,
  "duplicate_finals_total": 0 }
```

Read across: the replacement *was* cache-compatible, the warm tier *was*
attempted, and it *did not* pay off — because sherpa-onnx cannot
serialize inference state — so recovery fell through to audio replay and
the session survived anyway. That is the thesis stated as four counters.

**This required splitting a metric that M3 had conflated**, and the real
adapters are what exposed it. Until M5 the only same-key pair in the
fleet was two mock workers, and mock is the only serializable adapter, so
"same key" and "checkpoint restored" had the same answer in every case
that existed — and `RecoverSameModel`'s degrade path reached
`failover_cross_model_total` simply by *calling* `RecoverCrossModel`.
Standing up worker-a and worker-b for real made chaos scenario 2 fail
with `failover_same_model_total 0 -> 0` on a failover between two workers
running identical weights. The two axes are now counted separately
(`internal/metrics`), the same/cross decision moved to
`HandleBackendFailure` where the keys are actually compared, and three
regression tests in `internal/coord/failover_test.go` pin each
combination.

### Bugs and findings from building against the real models

| What | How it surfaced | Resolution |
|---|---|---|
| sherpa-onnx's Whisper recognizer SIGSEGVs the worker process on a zero-length buffer | Spiking the adapter before writing it; the process died with no traceback | `buffered.MIN_UTTERANCE_S` guard + `test_finalize_with_no_audio_is_safe`. Without it, an ordinary empty utterance is indistinguishable from a crashed worker — the exact signal chaos testing relies on |
| Streaming finals truncated (`"...near the river"`, `"...lazy dog ne"`) | Comparing spike output against `corpus/transcripts.json` | 0.6s tail padding before `input_finished()`, measured across 0/0.3/0.6/1.0s |
| `input_finished()` is terminal for a sherpa `OnlineStream`, but a session outlives an utterance | Reasoned from the M2 lifecycle, then pinned down by `test_state_survives_a_second_utterance` | `finalize` rolls a fresh stream. This one would have passed every mock test and failed on the second utterance of every real session |
| Local `docker` build broken by a stale `credsStore: "desktop"` in `~/.docker/config.json` | Build failed pulling the base image: `docker-credential-desktop: executable file not found` | Worked around with a scratch `DOCKER_CONFIG`; the user's global config still points at a credential helper Docker Desktop left behind when it was removed |
| `scripts/chaos.sh` treated `/admin/restore` as meaning "ready" | Scenario 3 failed intermittently right after scenario 2's kills, with no `partial.reset` — then passed in isolation | `/admin/restore` returns as soon as the child is *spawned*; with the mock adapter that was the same instant it could serve, but a real child loads weights first (up to ~1s for the 130MB CTC model). `restore_worker` now polls the worker's own `/health` before returning |

## M6 — heterogeneous routing and the matrix

- [x] Capability filtering wired into `Pick` (defect #11 fix) — online routing filters non-streaming targets before scoring; the router test proves an offline-only worker is refused online and accepted offline
- [x] Compose profiles (`models` adds `worker-e`) — `worker-e` is the ONNX Whisper deployment paired with CTranslate2 `worker-d`; both advertise identical weight identity but distinct opaque keys
- [x] `make matrix` → generates `docs/compat-matrix.md` from the running fleet — verified against all six live advertisements. The artifact records both online and offline policy, so it shows `a→c` replay, `d→e` offline replay, and `a→d` online refusal directly
- [~] Chaos: a→c (cross-family), d→e (same weights, different runtime), a→d (refused, non-streaming) — policy coverage is live in the generated matrix and router test; the forced pair-specific kill scenarios remain to be added

## M7 — load, backpressure, rate limits, Bifrost

- [x] `docs/BENCH.md` — loadgen's CLI and CSV frozen as a contract **before** the implementation, so M8 could start without waiting on this milestone's source
- [x] `cmd/loadgen`: paced client, all flags (`--streams --ramp --speech-ratio --jitter-ms --drop-pct --reconnect-every --mode --out`), event CSV + summary
- [x] Admission control (`internal/admission`): steps 4 and 5, plus step 2 (chunk widening above a soft threshold). Steps 1 and 3 were already the router's scoring and capability filter
- [x] `overloaded` event — retryable, distinct from terminal `error`, emitted only at `session.start`
- [x] Token buckets with headroom (`internal/router/bucket.go`), `Retry-After` honoured, **no sleep on the online path** — a 429 zeroes the bucket and the session moves
- [x] `corpus/large`: ~200 clips across six kinds (monologue, dialogue, long_form, silence_heavy, rapid_turns, noisy) with per-utterance boundaries in the manifest. Every test/bench/chaos path reads it
- [x] Chaos scenarios **4** (429 storm), **6** (gray failure), **7** (blackhole), **9** (overload), **10** (long silence), **11** (all backends down) — all passing against the real fleet
- [x] `GET /api/debug/workers` — the router's live health/load/rate-limit view. Scenario 6 asserts "ejected on latency, not errors", which is a claim about router state that nothing else exposed
- [x] Bifrost wired — **all five workers**, not just worker-d; `BIFROST_URL` (default empty) + direct fallback on any failure
- [x] Worker `POST /v1/audio/transcriptions` — OpenAI-shaped, multipart, stateless; verified across all five adapters
- [x] **Done-when: all three bars met.** Scenarios 4, 6, 7, 9, 10, 11 pass against the real fleet; speech-ratio measured (below); Bifrost routes finals with partials staying direct

**M7 is done.**

### Bifrost, verified end to end

With `BIFROST_URL` set, one session's final arrives as
`"Please transfer 50,000 rupees to Mira from..."` — punctuated Whisper text
from worker-d — while its 89 partials streamed from the pinned zipformer
worker at 20.0ms p50. Bifrost's own log shows the gateway's request
(`Go-http-client/1.1` from the gateway's container IP) returning 200.

**The proxy itself is nearly free.** Same clip, three runs each against
worker-d: direct `0.616 / 0.582 / 0.576`s, through Bifrost
`0.596 / 0.605 / 0.588`s — overlapping ranges, single-digit-millisecond
overhead.

**But routing an online final through it is still wrong by default**, and
for a better reason than latency. `/v1/audio/transcriptions` is stateless
by construction, so a Bifrost-routed final discards the pinned worker's
accumulated inference state and re-transcribes from raw audio — where a
direct `flush` finalizes state the worker already holds:

```
direct flush       9.2 ms    "mock1 mock2 mock3 ..."
via Bifrost      265.3 ms    "FIFTY THOUSAND RUPEES TO MIRR..."   ← recomputed, different model
```

29×, and the cheap path is cheap *because* the state is already there.
Online finals are therefore opt-in (`BIFROST_FINALS=1`); **offline sessions
route through Bifrost by default**, because offline has no partials, no hot
state to discard, and no latency budget — the one class where
retry-with-backoff is correct and the router has no advantage to offer.

Two framings were wrong along the way and are recorded because both are
easy: first attributing the whole 641ms to the proxy (it was the model),
then treating "a complete utterance is a discrete request" as settling the
question (true of the audio, false of the worker holding state built from
it).

All five workers are registered, so Bifrost's fallback and weighted
routing have somewhere to go. Routing to a single provider would have made
those features inert.

### Bifrost findings

| What | How it surfaced | Resolution |
|---|---|---|
| `base_url` inside a `keys[]` entry is ignored | Requests came back 401 with OpenAI's *own* error text about an invalid API key — the config was being ignored, not applied, and traffic was silently leaving for `api.openai.com` | `base_url` is per **provider**, in `network_config`. Five workers therefore need five providers with `custom_provider_config.base_provider_type: "openai"`, not five keys of one |
| `connection to private IP 172.19.0.4 is not allowed` | Once routing was right, Bifrost's SSRF guard blocked the compose network | `allow_private_network: true` per provider. Found in the binary's own strings — it is not in the published config docs |
| Two rebuild traps | A 404 from every worker, then a gateway that ignored `BIFROST_URL` despite the env var being set in the container | `--force-recreate` does **not** rebuild. Both times the image predated the code. `up -d --build` |
| Bifrost refuses a read-only `APP_DIR` | Container exited 1 immediately | It creates `config.db`/`logs.db` beside `config.json`. Mount read-write; the SQLite **WAL sidecars** (`.db-wal` was 2.6MB after one run) need `bifrost/*.db*`, since `*.db` matches neither `-wal` nor `-shm` |
| Per-request `fallbacks` are undocumented for non-chat endpoints | Needed to know whether Bifrost could do real work here at all | Tested directly: SIGKILL the primary worker, send a request naming it with a `fallbacks` chain — a transcript came back, served by the next provider. It **does** work on `/v1/audio/transcriptions`. This is the one Bifrost feature doing non-duplicated work, since `internal/coord` covers streaming failover only |

### All nine chaos scenarios pass with Bifrost enabled

`2, 3, 4, 5, 6, 7, 9, 10, 11` — `duplicate_finals_total == 0` throughout.
One number worth keeping from the run: with every final forced through
Whisper under 20 concurrent streams, `final_p50` was **4721ms**. That is
what made the default primary a zipformer worker rather than the Whisper
one.

### The VAD economic argument, measured

The plan words the bar as "`--speech-ratio 1.0` vs `0.5` shows backend call
volume halving". Measured, it does not exactly halve, and the gap is the
interesting part — so `scripts/vad_economics.sh` reports the real numbers
against two reference lines rather than asserting a round figure:

| speech ratio | ungated | perfect gating | actual | recovered |
|---|---|---|---|---|
| 1.0 | 300 | 300 | 294 | — |
| 0.75 | 298 | 223 | 250 | 64% |
| 0.5 | 298 | 149 | 174 | **83%** |
| 0.25 | 299 | 75 | 105 | **86%** |

`actual` sits above `perfect` because a VAD with pre-roll and hangover
deliberately dispatches a little silence either side of speech — clipping
a word's onset to save a backend call is a bad trade. Halving the speech
ratio cuts backend calls by **41%**, not 50%, and the VAD captures 83-86%
of the theoretically available saving at realistic silence fractions.
Reported, not rounded up.

### Bugs and findings from building M7

| What | How it surfaced | Resolution |
|---|---|---|
| Partial latency measured against the wrong frame | Reasoning about what `latency_ms` means when the gateway is slow | A `partial` carries no seq, so "time since the last frame written" under-reports badly under load — the partial for seq 9 arriving after frame 15 reads as 20ms instead of 120ms. **Overload would have looked fast.** loadgen now holds each partial until the following `ack`, whose seq identifies the chunk |
| `findPinnedWorker` picked a worker holding a *leaked* session | Scenario 7 blackholed worker-a while the session under test streamed happily through worker-b | A worker holds a session until the gateway Closes it, so any session that outlived its gateway is counted forever. Inferring "pinned" from an absolute count is a confident false positive; it is now a **delta** against a baseline taken before the open |
| `/admin/restore` treated as "ready" | Scenario 3 failed intermittently right after scenario 2's kills, then passed alone | Restore returns when the child is *spawned*; a real child then loads weights. `chaos.sh` now waits on the worker's own `/health`, and die-then-restores so no scenario inherits leaked sessions |
| `streams_refused` never incremented | Scenario 9 failed with `refused=0` while `overloaded=10` | Two different questions — refusal *events* vs streams that never got a session — and only the second is comparable against `--streams` |
| Scenario 9 asserted `finals == streams_opened` | Failed on a perfectly healthy run: 20 admitted, 67 finals | M4's endpointing closes an utterance per pause, so one session legitimately yields many finals. The assertion was wrong, not the system |
| Scenario 11 could pass vacuously | 0.27s pass looked too fast to be real | It took an `else` branch that only checked elapsed time, never inspecting the response. Now dials and sends `session.start` explicitly, asserts the event is `overloaded`/`error`, and bounds the refusal at 10s so a timeout unwinding cannot pass as an admission decision |

## M8 — benchmarks and charts

- [ ] Benchmarks A (cache vs no-cache), B (cold replay vs checkpoint — mock only), C (same- vs cross-model — full matrix), D (offline interference), E (rate limiting), F (capacity)
- [ ] Four charts: latency-over-time w/ kill marker, latency-vs-concurrency, inference-ms-vs-utterance-length, recovery-time-by-mode

## M11 — a real KV cache (Phase 0 spikes: **all gates passed**)

Plan: [docs/KVCACHE-ALTERNATIVES.md](KVCACHE-ALTERNATIVES.md). Background:
[KVCACHE-DEEPDIVE.md](KVCACHE-DEEPDIVE.md).

The enabling discovery: **sherpa-onnx hides the streaming state behind
`OnlineStream`, but the ONNX graph underneath does not.** The encoder
takes 35 state tensors in and returns 35 out, including `cached_key`,
`cached_val` and `cached_val2` — literal attention keys and values, with
`left_context_len = 64,32,16,8,32` frames per stack. Driving the graph
directly makes that cache ours: sizeable, serializable, transferable.

All measured against `models/download/zipformer-en-20M/`, the weights
`worker-a`/`worker-b` already serve.

| Spike | Question | Result |
|---|---|---|
| **S1** | Can we drive encoder+decoder+joiner ourselves? | **PASS** — `'QUICK BROWN FOX JUMPS OVER THE LAZY DOG NEAR THE RIVER'` |
| **S2** | Snapshot mid-utterance, restore, continue → identical text? | **PASS — 22/22 identical** across 8 clips × 3 kill points |
| **S3** | Restore into a **separate process** → identical? | **PASS** — 1.09 MB blob written by process A, restored by process B, same transcript |
| **S4** | RTF vs sherpa-onnx | **PASS** — ours 0.0139, sherpa 0.0188: **0.74×, i.e. faster** |
| **S5** | `kaldi-native-fbank` usable? | **PASS** — with two API gotchas, below |

KV state: **35 tensors, 1.09 MB per session** at fp32 (545 KB fp16,
273 KB int8).

### Findings from the spikes

| What | How it surfaced | Resolution |
|---|---|---|
| **The KV tensors are not the whole state — the feature seam matters** | S2 first ran **2/10 identical**, and every failure was at the resume point: `FOX`→`OX`, `JUMPS`→`JUMP`, `TEST`→`DUST` | On snapshot, up to `T-1 = 38` feature frames (~380 ms) sit in a buffer the encoder has not consumed. Dropping them loses that audio. Carrying the seam across took S2 to **22/22**. In production the gateway already replays exactly this audio from the journal (`last_seq_applied`), so the adapter must either serialize the buffer or rely on that replay — a real design decision, not an accident |
| `OnlineFbank.get_frame` indexes **absolutely**, not from a cursor | `IndexError: deque` on the second chunk | `pop()` shifts the frame indices; the caller must keep its own read position instead. Wrapped in a `Feats` class |
| Encoder state outputs are `new_<name>`, not `<name>_next` | `InvalidArgument: Invalid input name: new_cached_val_3` | The in/out pairing is derived from the graph and **asserted**, never hardcoded — a different export that breaks the convention now fails loudly instead of mis-wiring silently |
| Our transcripts drop the final word (`RECOR`, missing `BANK`) | Comparing against sherpa's output | The same tail-padding issue M5 already documented: a streaming encoder has not seen the last ~0.6 s without right context. `TAIL_PADDING_S = 0.6` applies to our loop too |

**Consequence:** the repository can, for the first time, checkpoint and
restore a **real model's attention state**. The warm tier stops being
mock-only.

### Phases 1–3 — built and verified end to end

- [x] `worker/kvcache/` — `state.py` (the tensor bank + layout, derived
      from the graph and asserted), `serde.py` (safetensors, validated
      header), `quant.py` (fp32/fp16/int8)
- [x] `worker/adapters/zipformer_kv.py` — **`serializable: True`**, the
      first real adapter for which that is true
- [x] `state_bytes` wired through `/health` — measured **2.19 MB for two
      live sessions**, replacing the hardcoded 0
- [x] `worker-f` / `worker-g` behind the `kv` compose profile, sharing a
      compatibility key with each other and with neither `worker-a/b`
- [x] 15 KV tests + 7 conformance tests green; worker suite 59 → **81
      passing**, no new failures

**The headline, measured against the running stack.** All workers but the
KV pair drained, 4 streams, `worker-f` SIGKILLed mid-utterance:

```
failover_total          = 2
failover_same_model     = 2
checkpoint_RESTORES     = 2      <- was permanently 0 on real models
checkpoint_degraded     = 0
duplicate_finals        = 0
errors=0  discontinuities=0
```

Cross-container transfer verified directly at the HTTP layer too: a
1.10 MB safetensors blob checkpointed from `worker-f` and restored into
`worker-g`, a different container.

### Further findings from building it

| What | How it surfaced | Resolution |
|---|---|---|
| **The KV tensors are not a complete checkpoint either — the decode hypothesis must travel with them** | `test_restore_continues_identically` failed with the restored text missing its *opening* words: `'OVER THE LAZY DOG NEAR THE RIVER BANK'` against `'QUICK BROWN FOX JUMPS OVER THE LAZY DOG NEAR THE RIVER BANK'` | The tensors restore *acoustic* context; the accumulated transducer hypothesis is the *transcript* state. It is tempting to omit because the decoder is stateless given the last `context_size` tokens — but then a restore silently truncates the utterance to whatever was decoded after the failover. `StateBank` now carries all three parts, and each one has a test that fails distinctively without it |
| setuptools flat-layout discovery broke the image build | `error: Multiple top-level packages discovered in a flat-layout: ['kvcache', 'adapters']` | Auto-discovery worked only while `adapters` was the sole top-level package. `[tool.setuptools] packages` is now explicit |
| onnxruntime emits a `recursive_mutex lock failed` abort at interpreter exit | Seen after the suite passes | Teardown noise from ORT session finalization, not a test failure — `pytest` exits 0 for the KV and conformance suites. Noted rather than chased |

**Still open (Phases 4–6):** the int8 path is implemented and unit-tested
but not yet benchmarked for text drift; chaos scenarios 12/13 are not
written; the Bifrost-versus-router evaluation is planned but not yet
written up.

## M9 — dashboard and HA profile (hard 1-day timebox)

See [docs/DASHBOARD.md](DASHBOARD.md) for the design, the endpoint list, and why SSE.

- [x] Static `/dashboard/`, `/api/events` SSE with `Last-Event-ID` resume, canvas charts, client-side filtering
- [x] Control plane over the existing chaos endpoints (`POST /api/chaos/{worker}/{action}`); compat-key hash visible per node, and rendered as MATCH/MISMATCH on every failover log line
- [x] `POST /api/load/{n}` — the stack comes up quiet and the reviewer applies load from the page (`cmd/loadgen --control-addr`, additive; the CLI contract in BENCH.md is unchanged)
- [x] Per-node resource graphs: memory against each container's **cgroup limit**, real queue depth, CPU, sessions, RTF — plus the gateway on the same axes
- [x] Stacked per-worker traffic chart, so a kill is visible as throughput moving rather than as a log line
- [x] `docker-compose.ha.yml`: nginx **L4** `least_conn`, two gateways. Verified by `docker kill`ing gateway-1 mid-stream: **new** sessions through nginx kept succeeding (3/3 finals, 0 errors), while the 3 in-flight sessions pinned to the dead gateway ended with `errors=3`. That is the documented limit, now measured rather than asserted — the gateway holds the journal and checkpoints in memory, so HA covers *new* sessions, not in-flight ones
- [x] `make demo` — the headless twin of click-Kill, with assertions
- [ ] Client-side replay ring under HA: `cmd/loadgen -reconnect-every` exists and resumes from the last ack, but no scenario asserts a session surviving a **gateway** death

**Verified against the running stack**, 20 streams, `worker-mock` SIGKILLed at
steady state:

| | before | +4s | +10s |
|---|---|---|---|
| worker-mock pushes | 1545 | 1574 | **1574** (dead, frozen) |
| worker-a pushes | 1077 | 1422 | **1947** |
| worker-b pushes | 167 | 255 | **401** |

`failover_total 10`, `failover_cross_model_total 10`,
`duplicate_finals_total 0`. Push p50/p99 2.2ms/27.4ms at 20 streams.

Measured node memory against limits: worker-c 310M/1536M (20%, the 130MB
CTC model), worker-a/b 161–211M, worker-mock 73M/512M, gateway 31M/1024M.

### Bugs and findings from building M9

| What | How it surfaced | Resolution |
|---|---|---|
| `make demo` killed a hardcoded `worker-a` | The kill produced `failover_total 0` — a PASS that proved nothing | The router scores on latency as well as outstanding count, so the fastest worker takes most sessions (12 streams → ~9/1/2, two streaming workers picked not at all). `worker-a` held 1 of 12. The victim is now chosen at runtime as the busiest worker — the same trap as M7's `findPinnedWorker`, in a different costume |
| `pkill -f "cmd/loadgen"` left a load generator running | A distribution reading showed 20 outstanding sessions for 10 requested streams | `go run` execs the build output as a child whose argv does not contain `./cmd/loadgen`, so the pattern kills only the wrapper. The stray generator silently doubled the load and made the reading a fiction. `demo.sh` now builds the binary explicitly and holds its PID |
| `resources.py` not in the worker image | Every worker crashed on boot: `ModuleNotFoundError: No module named 'resources'` | `worker/Dockerfile` copies named files, not the directory. Caught immediately because the container exits; worth noting as the cost of the explicit COPY list |
| `/health` reported `queue_depth: 0` always | Writing the queue graph and finding it was a flat line by construction | Now real: `inflight` (accepted) minus `running` (in the executor) is the backlog. A replayed push is counted before the guard, so a failover storm does not read as backlog on an idle worker. `state_bytes` stays an honest 0 |
| `worker-e` reported no memory at all | `mem=None` for one node while five others had numbers | It was running an image built before `resources.py` (it is behind the `models` profile and was not rebuilt). Not a bug — and it accidentally confirmed the nullable design: the node drew a **gap**, not a line at zero |
| **`Worker.Outstanding` went negative** (`worker-a -3`, `worker-c -9`) | The dashboard's topology pane renders the session count per node, so a negative was impossible to miss — it had been invisible in the JSON for three milestones | `HandleBackendFailure` unbound the dead worker **up front**, before knowing whether recovery would succeed. Every failure return restores `State.WorkerID` to that same worker, and the caller's terminal cleanup unbinds it again — one extra decrement per terminally-failed session. Not cosmetic: `Pick` scores on `(Outstanding+1) * latencyP95`, so a worker at -9 looks maximally attractive **forever** and skews routing for the life of the process. Now a transfer — bind the replacement and unbind the dead worker together, only once recovery has actually succeeded |
| `failover_exhausted_total` under-reported | Chasing the count above: -12 across the fleet, but only 4 exhaustions reported | The `no replacement worker available` early return bypassed the counter entirely, so every terminal failure caused by an *empty fleet* went uncounted — 8 of 12 on the run that surfaced it |
| A restored worker never takes traffic again | Killing a node, restoring it, and watching it sit at 0 | Working as designed, but badly surfaced: sessions are pinned for life, and loadgen holds each stream open for the whole `--duration`, so **no new `session.start` ever fires** and nothing re-`Pick`s. A restored worker rejoins only when new sessions open. Ejection recovery has the same shape — `Ejected → probe` needs a `Pick` to carry the probe |
| Port 7000 unavailable on macOS | `bind: address already in use` — `ControlCe` (AirPlay Receiver) | Already documented in the compose file; `GATEWAY_DASHBOARD_PORT=7001` is the workaround, now repeated in DASHBOARD.md |

## M12 — stateless streaming through Bifrost (shared KV tier)

The question this answers: can Bifrost route STREAMING traffic, not just
offline finals? It could not while a session's KV cache lived inside one
worker process — the session was pinned and a load balancer could only
honour the pin. Moving the state out makes affinity an optimization rather
than a correctness requirement. Design and numbers:
[KVCACHE.md](KVCACHE.md).

- [x] `cmd/kvtier` — per-family shared tier: opaque blobs, immutable versions, TTL + LRU byte ceiling, exact and prefix delete. 10 tests
- [x] `worker/kvtier.py` — tier client plus the local hot cache that keeps this from being slower than pinning; `local` / `tier` / `miss` reported per request and cumulatively at `/health`
- [x] `kv_mode=stream|final` with `state_ref`/`state_sink` on the worker's OpenAI-shaped endpoint — the only shape Bifrost routes
- [x] `backend.StatelessClient` — implements the ordinary `backend.Client`, so the session loop, failover, journal and metrics are untouched. 11 tests
- [x] One tier **per model family**, learned by the gateway from `/health`, never configured — same rule as the compatibility key, so the mapping has one home
- [x] `STATELESS_STREAM=1` behind a flag; default stack unchanged
- [x] `scripts/prove_stateless_bifrost.py` — one session's chunks fanned across different workers via Bifrost, transcript byte-identical to a single-worker reference
- [x] `scripts/stateless_ab.sh` / `make stateless-ab` — the same load run both ways

**Measured, not asserted.** Bifrost forwards unknown *request* fields
verbatim but **strips unknown response fields** (verified with a purpose-built
echo provider), so state can travel in but not back. The design never needs
it to: the caller names the output reference up front, and a cache miss is
signalled as an HTTP **424** because status codes survive the hop.

Safety, against the running stack: reference resolves nowhere → **424**
with the worker's error body intact through Bifrost; zipformer blob read by
a CTC worker → **422**, refused *before* deserialization; read by its twin
→ **200**, 1,105,800 bytes moved.

### Bugs this milestone found

| Symptom | How it surfaced | Cause and fix |
|---|---|---|
| **Tier evicting 87% of everything written** (5,682 of 6,528 versions), costing 12 sessions their state | Only under stress — 30 streams for 60s. Invisible at 5 streams | Immutable versions accumulated with nothing reclaiming them until session close, and the 5-minute TTL never fires inside a 60s run. One version per chunk at 160ms is ~375 per session. `StatelessClient.Push` now retires version N once N+1 is durable — safe there because the push has returned. Evictions **5,682 → 0**, peak resident **268 MB → 46 MB** |
| Exact-key `DELETE` could take unrelated versions | Found while writing the retirement fix, not by a failure | It was implemented as a prefix delete, so retiring `s1:10` would also take `s1:100` and `s1:101` — silent state loss for any session reaching three-digit versions, surfacing as a 424 far from its cause. Split into `Delete` (exact) and `DeletePrefix` |
| A cross-family blob appeared to be accepted (HTTP 200) | Ad-hoc probe during the safety checks | **Not a bug** — my own fleet hygiene. The CTC worker was still on a pre-change image, so `kv_mode` was an unknown field and it silently ran the old one-shot path. Caught by the response shape lacking `kv_hit`. Rebuilt; the real answer is 422. Recorded because it is the second time this session a stale container nearly produced a false finding |
| **12 tier misses per 30-stream run**, invisible in the gateway log | Only at stress, and only because the miss counter disagreed with the error count | `finalizeEndpoint` ends a VAD utterance with `Close` then `Open`. `Close` deletes every version in the tier, but `StatelessClient.Open` returned the same handle **without resetting the version counter**, so the next utterance's first push referenced a version that had just been deleted. Invisible in any single-utterance test. The arithmetic confirmed it: 12 misses = 6 failovers x 2 chain members, and `dispatchChunk` hands the error to `HandleBackendFailure` without logging it, so the session silently finished on the pinned path. `Open` now resets. Misses **12 -> 0**, failovers **6 -> 0** |
| **An ejected worker could never be probed back** — "latency stuck at max" on the dashboard | Noticed as a chart frozen at a high value; turned out to be four of six workers ejected with `failover_exhausted_total=27` | `Pick` scored every eligible candidate on `(Outstanding+1) * latencyP95` and called `beginProbe()` only on the WINNER. But an ejected worker's p95 is frozen at whatever ejected it — no traffic reaches it, so no new sample can arrive — while a healthy peer's is live and small: ejected/0 sessions `(0+1)*0.664 = 0.664` vs healthy/5 sessions `(5+1)*0.019 = 0.114`. The peer would need ~34 concurrent sessions to lose. A circuit breaker whose half-open state was unreachable. Proved live by restoring a dead worker: it answered `/health` in 5s and stayed ejected at a frozen 664ms for 40s+ under traffic. Fixed by holding a probe-due worker out of the scoring pass entirely and preferring it — `probing` already bounded it to one trial at a time. Verified live: three permanently-ejected workers recovered within 12s. The existing probe test could not catch this because its fleet had ONE worker, so the ejected one won by default |
| **"Immutable" versions were mutable in the worker's hot cache** | Not by a test or a failure — by review | Adapters mutate state IN PLACE and return the same object (`_consume` writes `bank.pending`, `frames_consumed`, absorbs tensors). The hot cache handed out a live reference and then `store` filed the *same object* under the successor, so the predecessor's entry silently became the state AFTER the chunk. A retry landing back on this worker would apply the chunk twice; the same retry routed to a peer would fetch the correct serialized bytes and be right — **correctness as a function of placement**, the exact property the design exists to remove. Fixed by consuming the entry on load (`_hot_take`) rather than deep-copying 1.09 MB per chunk, plus create-only (`PutIfAbsent`, 409) so two retries cannot both write one version. Regression tests verified to fail with the bug reintroduced |

### Open

- **A probe still needs a new session to carry it.** The scoring trap is fixed, but `Pick` only runs at `session.start`, so an ejected worker recovers only when fresh sessions arrive. With long-lived streams and no churn it waits — observed: one worker stayed ejected while eight 4-minute sessions ran, because none of them ended. A router-side background prober (its own timer, not riding client traffic) is the real fix and is not built
- A session recovered by `coord` onto a new worker reverts to the pinned path — `statelessClientFor` runs only at session start
- Zero-copy (`/dev/shm` + mmap'd safetensors) designed but not built
- **Latency comparison is not established.** p50 favours stateless consistently (partials 40ms vs 20ms) but those are round numbers plausibly quantized to the 20ms frame cadence. The pinned path's p95 varied 30x across four runs (280 / 3,780 / 5,020 / 8,020 ms) — and a control run with `CHECKPOINTS_ENABLED=false` was the *worst* of them, refuting the obvious explanation that `asyncCheckpoint` was the cost. Needs a quiet machine and repeated runs before any latency claim is made

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
| 6 | Unbounded recursion in `handleBackendFailure` | M3 (bounded loop) | [x] explicit `MaxFailoverAttempts` loop, with failed candidates excluded |
| 7 | No `ack` event for client-side replay | M2 | [x] early: `ack` implemented and wired at M1 (piggybacks session_id delivery on session.start's ack too) |
| 8 | `TrimBefore` can discard pre-roll | M2 (current scope) / M4 (full formula) | [x] safe for M1/M2's actual shape — one utterance per session, trimmed once at its own final, no "still-open utterance" case can exist yet; the full `min(committedSeq, openUtteranceStart)` formula is only meaningful once M4 allows a new utterance to already be open when an earlier one's final commits |
| 9 | "Continue without reset" needs unsafe text diffing | M3 (always reset) | [x] every successful recovery emits `partial.reset` before any regenerated partial |
| 10 | No finals-dedupe structure specified | M2 | [x] `Emitter`'s dedupe is now correctly per-utterance (not session-wide — a real bug found and fixed this milestone, see below), proven by `TestUtterancesAreIndependentAfterNewUtterance` |
| 11 | Non-streaming adapters have no capability signal | M5/M6 (`Capabilities`) | [x] early: `Capabilities` struct implemented and returned from `open` at M1, ahead of its M6 router caller |
| 12 | Thesis scheduled as phase 5 of 7 | M3 (moved up) | [x] resequenced in the plan |

## Open risks (not yet decisions — watch these)

- sherpa-onnx real-model RTF on this machine is unmeasured until M5; concurrency target (20 streams) may need revising down once measured.
- **Docker Desktop memory is tight.** Checked via `docker info`: 14 CPUs (fine) but only **~7.75 GiB** RAM allocated to the VM, no CLI to resize it. The planned default fleet (gateway + 5 workers, M0 limits) budgets ~8 GiB at the *limits* — already at the edge before M5 adds real model weights on top of worker-a/b/c/d. Action before M5: raise Docker Desktop's memory allocation (Settings → Resources) to 16 GiB+, or trim per-worker limits once real memory footprints are measured. Not a blocker today (M0's stub workers use a fraction of their limits).
- **Standing gotcha for any future `coder/websocket` client code** (`cmd/loadgen` at M7, the dashboard's control plane at M9, any chaos script that speaks WS directly): canceling a `Read`'s context — including via a short per-call timeout used to "check without blocking" — closes the whole connection, not just that call. Found and fixed once already in `cmd/smoketest` (see M1's bugs-found table). The safe shape is always: one dedicated reader goroutine on the connection's full-lifetime context, feeding a channel; drain that channel with a non-blocking `select`/`default`, never a fresh short-lived context per poll.
- Minor: `worker`'s pytest run emits a `StarletteDeprecationWarning` about `httpx`/`starlette.testclient` (`pip install httpx2` suggested). Not failing anything; worth a look if it becomes a hard error in a future FastAPI/Starlette bump.
