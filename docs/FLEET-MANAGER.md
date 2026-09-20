# Dynamic demo fleet manager

The dashboard can start an additional worker while the demo is running. It
does not ask Bifrost, the gateway, or browser JavaScript to run Docker.
Those components serve or route audio and must not hold orchestration
credentials.

```text
Dashboard + family button
        │ POST /api/fleet/{zip|ctc}
        ▼
Gateway control plane ── POST /v1/workers ──► fleet-manager
                                                │ Docker API: create/start
                                                ▼
                                        new worker /health
                                                │ POST /api/providers
                                                ▼
                                             Bifrost
        ▲
        │ gateway independently reads /health, verifies key + KVTier,
        │ then adds the worker to its live router cohort
        └──────────────────────────────────────────────────────────────
```

## What the manager is allowed to do

`cmd/fleetmanager` accepts only `zip` and `ctc`. Each maps to a
fixed image, adapter, model identifier, CPU/memory limit, and family KVTier.
It cannot accept an image name, shell command, volume, host port, arbitrary
environment variable, or Docker network from the dashboard request. It
attaches workers only to the Compose network.

After creation it waits up to 75 seconds for the worker's `/health` endpoint
to publish a compatibility key. It then creates a Bifrost custom OpenAI
provider whose base URL is that container. The endpoint becomes available to
the gateway only after that succeeds.

The manager is the only Compose service that mounts `/var/run/docker.sock`.
It has no host port, so the browser cannot reach it directly. The gateway
does **not** get Docker authority; it forwards one family name and validates
the result.

## Gateway admission is a separate correctness check

The manager's health check establishes that a process is alive. It does not
establish that the process can safely consume state from a family. Before
adding the returned endpoint, the gateway compares its advertisement with the
boot worker for the requested family:

1. Its ID must be scoped to that family (`worker-zip-*`, etc.).
2. `compatibility_key_hash` must exactly match the family seed worker.
3. It must advertise a serializable KV cache and the exact same KVTier URL.
4. The router rejects an existing ID rather than replacing it.

Only then does `Router.Add` expose it to new selections. Existing sessions
need no migration: the state reference in later Bifrost requests lets a new
compatible worker load the current version from the shared tier.

## Bifrost's role

Bifrost is still the stateless inference routing plane. It receives the
worker provider registration through `POST /api/providers`, then receives
audio requests with the current `state_ref` and requested `state_sink`.
Bifrost does not create a container or decide cache compatibility. Dynamic
provider changes require its writable config store, so `bifrost/config.json`
enables it and the Compose image is pinned to `v1.5.0` for a repeatable API
contract.

## Demo boundary and production replacement

Mounting Docker's socket is effectively root-equivalent access to the host.
This manager is therefore for a local, trusted demo only. A production
replacement is a Kubernetes controller or platform deployment API with the
same protocol shape: allowlisted `InferencePool` family, create workload,
wait readiness, register endpoint, verify compatibility, add to serving
cohort. Bifrost remains the request router in both designs; it must not be
given cluster-administrator credentials.
