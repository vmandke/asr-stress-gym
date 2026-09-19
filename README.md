# ASR Stress Gym

A fault-tolerant ASR serving system. The audio and model internals are a
black box; the engineering is session state, model-specific inference
cache, routing, replay, failover and observability.

> **Model cache accelerates recovery when compatible. Audio replay
> guarantees recovery when it is not.**

## Status

**Skeleton (M0).** `docker compose up` brings up the gateway and a fleet
of five identity-only workers, all passing health checks. No WebSocket
session handling, no inference, no failover yet — see the build plan below
for what's built versus what's scaffolded.

```bash
./scripts/check_env.sh   # verify go/python/docker/compose are present
make up                  # docker compose up --build
make down                # docker compose down -v
```

### Troubleshooting

**Port 7000 already in use, on macOS.** AirPlay Receiver (ControlCenter)
listens on host port 7000 by default since macOS Monterey. Either disable
it (System Settings → General → AirDrop & Handoff → AirPlay Receiver) or
remap the host port, e.g. `GATEWAY_DASHBOARD_PORT=7001 make up` — the
container's own port is unaffected either way.

## Design and build plan

- [`docs/build-plan.md`](docs/build-plan.md) — the architecture and thesis:
  principles, invariants, protocol, failover algorithms, benchmarks.
- [`docs/implementation-plan.md`](docs/implementation-plan.md) — the
  executable milestone plan (M0–M10), the interface contracts that make it
  buildable, and the model fleet.
- [`docs/DECISIONS.md`](docs/DECISIONS.md) — every question the design left
  open, resolved once, up front.
- [`docs/STATUS.md`](docs/STATUS.md) — living tracker: what's built and
  verified vs. pending, per milestone.

## Repository layout

```
cmd/gateway, cmd/loadgen   Go binaries
internal/audio             ALL sample-level code lives here — see the audio
                            boundary in implementation-plan.md — and nowhere
                            else in this repository
internal/{wire,session,journal,router,backend,coord,metrics,dash}
worker/                    Python worker: HTTP surface + model adapters
corpus/                    committed known-text clips for ground-truth tests
scripts/                   setup, chaos scenarios, benchmarks — checked in,
                            not run ad hoc
```

## Not implemented yet, and why

Tracked live in [`docs/STATUS.md`](docs/STATUS.md). This section will carry
the final list at M10, per the design doc's own requirement that omissions
be stated as choices.
