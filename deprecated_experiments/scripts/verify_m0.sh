#!/usr/bin/env bash
# M0 "done" bar (docs/implementation-plan.md): `docker compose up` -> all
# healthchecks green, `make down` clean. Run from the repo root.
set -euo pipefail
cd "$(dirname "$0")/.."

source ./scripts/check_env.sh # sourced (not exec'd) so its PATH fix applies below too
echo "--- go build ---"
go build ./...
go vet ./...

echo "--- docker compose up --build -d ---"
# Default host mapping (7000) collides with macOS AirPlay Receiver on many
# machines (see README Troubleshooting); use a scratch port here so this
# script verifies the stack rather than a pre-existing host-port conflict.
export GATEWAY_DASHBOARD_PORT="${GATEWAY_DASHBOARD_PORT:-7000}"
if lsof -nP -iTCP:"$GATEWAY_DASHBOARD_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $GATEWAY_DASHBOARD_PORT busy on host, remapping to 17000 for this run"
  export GATEWAY_DASHBOARD_PORT=17000
fi
docker compose up --build -d

cleanup() {
  echo "--- docker compose down -v ---"
  docker compose down -v
}
trap cleanup EXIT

echo "--- waiting for all services to report healthy ---"
deadline=$((SECONDS + 120))
while true; do
  # One line per service: "<name> <health-status-or-'running'>"
  statuses=$(docker compose ps --format '{{.Name}} {{.Health}}')
  echo "$statuses"
  if ! echo "$statuses" | grep -qvE '(healthy|running)$' ; then
    echo "OK: all services healthy"
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "TIMEOUT waiting for healthy services" >&2
    docker compose ps
    exit 1
  fi
  sleep 2
done

echo "--- curl gateway /health ---"
curl -fsS "http://localhost:${GATEWAY_DASHBOARD_PORT}/health" && echo

echo "M0 verification passed."
