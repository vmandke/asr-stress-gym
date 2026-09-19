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
  local admin_url="$1"
  # A worker that is already healthy reports {"spawned":true} too, so this is
  # safe before every scenario. It also repairs a fleet left faulted by an
  # interrupted prior run.
  curl --fail --silent --show-error -X POST "${admin_url}/admin/restore" >/dev/null
}

restore_all_workers() {
  restore_worker "${WORKER_A_ADMIN_URL:-http://localhost:19001}"
  restore_worker "${WORKER_B_ADMIN_URL:-http://localhost:19002}"
  restore_worker "${WORKER_C_ADMIN_URL:-http://localhost:19003}"
  restore_worker "${WORKER_D_ADMIN_URL:-http://localhost:19004}"
  restore_worker "${WORKER_MOCK_ADMIN_URL:-http://localhost:19000}"
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
