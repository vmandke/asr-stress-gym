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
  # die THEN restore, unconditionally, rather than restore alone.
  #
  # /admin/restore on a live child is a no-op, which leaves that child's
  # accumulated session state in place — and a worker holds a session until
  # the gateway Closes it, so any session that outlived its gateway (this
  # script force-recreates the gateway between scenarios) is counted
  # forever. A later scenario then detects the wrong "pinned" worker and
  # injects its fault somewhere the session under test never was. That is
  # exactly how scenario 7 first failed. Respawning guarantees every
  # scenario starts against workers holding nothing.
  curl --fail --silent --show-error -X POST "${admin_url}/admin/die" >/dev/null || true
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
}

reset_fleet_for_scenario() {
  restore_all_workers

  # Workers retain their port bindings; only the gateway needs recreating to
  # discard ejection/backoff state, rate-limit buckets and admitted-session
  # counts left by the preceding scenario. All of that lives in the gateway
  # process (internal/router, internal/admission), so a restart is the
  # whole reset.
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

# Order matters in exactly one place: scenario 11 kills the entire fleet,
# so it runs last. Everything before it is order-independent because
# reset_fleet_for_scenario respawns every worker and recreates the gateway
# in between — the suite is a test suite, not three stateful demos whose
# outcome depends on what ran before.
#
# Scenario 9 (overload) is not in this list: it needs the gateway restarted
# with a lowered admission envelope, so it owns that lifecycle itself in
# scripts/scenarios/09_overload.sh.
for scenario in 2 3 4 5 6 7 10 11; do
  reset_fleet_for_scenario
  GATEWAY_DEBUG_URL="${gateway_url}" go run ./cmd/chaostest --scenario "${scenario}"
done

reset_fleet_for_scenario
GATEWAY_DEBUG_URL="${gateway_url}" ./scripts/scenarios/09_overload.sh

# Leave the local fleet usable after the intentionally destructive tests.
restore_all_workers
