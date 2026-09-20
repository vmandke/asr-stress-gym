#!/usr/bin/env bash
# One command to get a working demo: bring the stack up, make sure every
# worker is actually serving, apply load, and print the dashboard URL.
#
#   ./scripts/start.sh            # 5 streams
#   ./scripts/start.sh 20         # 20 streams
#   ./scripts/start.sh 0          # up, but idle
#
# This exists because "docker compose up" alone is not reliably enough on
# a machine that has been used for chaos testing. Three things go wrong,
# all of them recoverable, none of them obvious from the error:
#
#   1. macOS AirPlay Receiver squats on port 7000, so the gateway cannot
#      bind and compose reports a bare "address already in use".
#   2. A worker whose child was SIGKILLed earlier leaves its CONTAINER
#      healthcheck red. `depends_on: service_healthy` then refuses to
#      start the gateway — the fleet blocks its own recovery.
#   3. Restore returns when the child is spawned, not when it serves; a
#      real adapter then loads weights for a second or more.
set -euo pipefail

cd "$(dirname "$0")/.."
source ./scripts/check_env.sh

streams="${1:-5}"
duration="${DEMO_DURATION:-30m}"

# --- pick a dashboard port that is actually free ----------------------
port="${GATEWAY_DASHBOARD_PORT:-}"
if [[ -z "${port}" ]]; then
  port=7000
  if lsof -nP -iTCP:7000 -sTCP:LISTEN >/dev/null 2>&1; then
    port=7001
    echo "note: port 7000 is taken (on macOS this is usually AirPlay Receiver);"
    echo "      using ${port} instead. Disable it in System Settings > General >"
    echo "      AirDrop & Handoff to use 7000."
  fi
fi
export GATEWAY_DASHBOARD_PORT="${port}"
gateway="http://localhost:${port}"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# --- 1. revive any worker whose child was killed by earlier chaos ------
#
# Best-effort and unconditional: /admin/restore on a live child is a
# harmless no-op, and doing this BEFORE compose is what stops an unhealthy
# leftover from blocking the gateway's dependency check.
bold "Reviving any worker left dead by earlier chaos"
revived=0
for admin_port in 19001 19002 19003 19004 19005 19006; do
  if curl --fail --silent --max-time 2 -X POST "http://localhost:${admin_port}/admin/restore" >/dev/null 2>&1; then
    revived=$((revived + 1))
  fi
done
if (( revived > 0 )); then
  echo "  restored ${revived} supervisor(s); waiting for their children to serve"
  sleep 10
else
  echo "  nothing running yet — first start"
fi

# --- 2. bring the stack up --------------------------------------------
bold "Starting the stack (dashboard on ${port})"
docker compose up -d --build

# --- 3. wait for the gateway ------------------------------------------
printf 'waiting for the gateway '
for _ in $(seq 1 90); do
  if curl --fail --silent --max-time 2 "${gateway}/health" >/dev/null 2>&1; then
    echo " ready"
    break
  fi
  printf '.'
  sleep 1
done
if ! curl --fail --silent --max-time 2 "${gateway}/health" >/dev/null 2>&1; then
  echo
  echo "gateway never became healthy. Recent logs:" >&2
  docker compose logs gateway --tail 30 >&2
  exit 1
fi

# --- 4. clear stale fault injection -----------------------------------
#
# A blackhole or 429 left set by a previous session survives a gateway
# restart, because it is state inside the WORKER. Starting a demo on top
# of one produces a fleet that looks broken for no visible reason.
for worker_port in 18001 18002 18003 18004 18005 18006; do
  curl --fail --silent --max-time 2 -X POST "http://localhost:${worker_port}/admin/reset" >/dev/null 2>&1 || true
done

# --- 5. apply load ----------------------------------------------------
if [[ "${streams}" != "0" ]]; then
  bold "Starting ${streams} streams"
  if ! curl --fail --silent -X POST "${gateway}/api/load/${streams}?duration=${duration}"; then
    echo
    echo "could not reach the load generator — is the loadgen container up?" >&2
    echo "the dashboard still works; press a load button once it is." >&2
  fi
  echo
  sleep 12
fi

# --- 6. report --------------------------------------------------------
bold "Fleet"
curl --fail --silent "${gateway}/api/debug/workers" | python3 -c "
import json, sys
d = json.load(sys.stdin)
base = d.get('cluster_p95_ms', 0)
for w in d['workers']:
    kind = 'streaming' if w['streaming'] else 'batch only'
    cache = 'checkpoint' if w['serializable'] else 'replay only'
    print(f\"  {w['id']:12} {w['status']:9} {w['model']:18} {kind:10} {cache:11} sessions={w['outstanding']}\")
print(f\"\n  peer-median p95 baseline: {base:.1f}ms   admitted sessions: {d['admitted_sessions']}\")
"

bold "Dashboard"
echo "  ${gateway}/dashboard/"
echo
echo "  Click a node to kill / drain / slow it. Click a stream to see where"
echo "  its audio goes and which state lives where."
echo "  Stop the load from the page, or:  curl -XPOST ${gateway}/api/load/0"
echo "  Stop everything:                  make down"
