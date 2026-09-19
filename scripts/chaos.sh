#!/usr/bin/env bash
# M3 acceptance runner. Each scenario deliberately kills one or more worker
# children and therefore changes the gateway router's in-memory health view.
# Reset that view between scenarios so this is a reproducible test suite, not
# three stateful manual demos whose order affects their outcome.
set -euo pipefail

# Source (rather than execute) so check_env.sh's Docker Desktop credential
# helper PATH fix applies to the compose calls below too.
source ./scripts/check_env.sh

dashboard_port="${GATEWAY_DASHBOARD_PORT:-7000}"
gateway_url="${GATEWAY_DEBUG_URL:-http://localhost:${dashboard_port}}"

restore_worker() {
  local admin_url="$1" worker_url="$2"
  # A worker that is already healthy reports {"spawned":true} too, so this is
  # safe before every scenario. It also repairs a fleet left faulted by an
  # interrupted prior run.
  curl --fail --silent --show-error -X POST "${admin_url}/admin/restore" >/dev/null

  # Then WAIT for the respawned child to actually serve. /admin/restore
  # returns as soon as the process is spawned, which was indistinguishable
  # from "ready" while every adapter was the mock. From M5 a child loads
  # real weights first — up to a second for the 130MB CTC model — so
  # without this the next scenario can open a session against a worker
  # whose port is not listening yet and fail for reasons that have nothing
  # to do with what it is testing.
  for _ in $(seq 1 60); do
    if curl --fail --silent "${worker_url}/health" >/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  echo "worker at ${worker_url} did not come back after /admin/restore" >&2
  return 1
}

restore_all_workers() {
  restore_worker "${WORKER_A_ADMIN_URL:-http://localhost:19001}" "${WORKER_A_URL:-http://localhost:18001}"
  restore_worker "${WORKER_B_ADMIN_URL:-http://localhost:19002}" "${WORKER_B_URL:-http://localhost:18002}"
  restore_worker "${WORKER_C_ADMIN_URL:-http://localhost:19003}" "${WORKER_C_URL:-http://localhost:18003}"
  restore_worker "${WORKER_D_ADMIN_URL:-http://localhost:19004}" "${WORKER_D_URL:-http://localhost:18004}"
  restore_worker "${WORKER_MOCK_ADMIN_URL:-http://localhost:19000}" "${WORKER_MOCK_URL:-http://localhost:18000}"
}

reset_fleet_for_scenario() {
  restore_all_workers

  # Workers retain their port bindings; only the gateway needs recreating to
  # discard ejection/backoff state left by the preceding scenario.
  GATEWAY_DASHBOARD_PORT="${dashboard_port}" docker compose up -d --force-recreate gateway

  for _ in $(seq 1 30); do
    if curl --fail --silent "${gateway_url}/health" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "gateway did not become healthy at ${gateway_url}" >&2
  return 1
}

for scenario in 2 3 5; do
  reset_fleet_for_scenario
  GATEWAY_DEBUG_URL="${gateway_url}" go run ./cmd/chaostest --scenario "${scenario}"
done

# Leave the local fleet usable after the intentionally destructive tests.
restore_all_workers
