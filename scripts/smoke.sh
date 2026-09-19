#!/usr/bin/env bash
# M1 "done when" bar (docs/implementation-plan.md), against the real
# compose stack — not the fake worker cmd/gateway's own tests use. Run
# from the repo root.
set -euo pipefail
cd "$(dirname "$0")/.."

source ./scripts/check_env.sh

if [ ! -f corpus/01_short_greeting.wav ]; then
  echo "corpus/ is empty — run ./scripts/gen_corpus.sh first (macOS only; corpus is committed, so this is a one-time step)" >&2
  exit 1
fi

echo "--- go build ---"
go build ./...
go vet ./...

echo "--- docker compose up --build -d ---"
export GATEWAY_DASHBOARD_PORT="${GATEWAY_DASHBOARD_PORT:-7000}"
if lsof -nP -iTCP:"$GATEWAY_DASHBOARD_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  GATEWAY_DASHBOARD_PORT=17000
fi
export GATEWAY_DASHBOARD_PORT
docker compose up --build -d

cleanup() {
  echo "--- docker compose down -v ---"
  docker compose down -v
}
trap cleanup EXIT

echo "--- waiting for all services to report healthy ---"
deadline=$((SECONDS + 120))
while true; do
  statuses=$(docker compose ps --format '{{.Name}} {{.Health}}')
  if ! echo "$statuses" | grep -qvE '(healthy|running)$'; then
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

echo "--- cmd/smoketest ---"
GATEWAY_WS_URL="ws://localhost:${GATEWAY_WS_PORT:-7070}/ws" go run ./cmd/smoketest --clip corpus/01_short_greeting.wav

echo "M1 smoke test passed."
