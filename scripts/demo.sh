#!/usr/bin/env bash
# M9's demo: put the fleet under load, kill a worker, and show that the
# system recovered — through the same control-plane endpoints the
# dashboard's buttons drive.
#
# This is the headless twin of "click a node, choose Kill, watch the
# latency spike and come back". Both paths hit POST /api/chaos/{worker}/
# {action}; this one also asserts, so it can fail. The dashboard makes the
# result legible, this makes it verifiable, and neither is allowed to be
# the only proof (docs/build-plan.md, "Scope discipline").
set -euo pipefail

source ./scripts/check_env.sh

dashboard_port="${GATEWAY_DASHBOARD_PORT:-7000}"
gateway="${GATEWAY_DEBUG_URL:-http://localhost:${dashboard_port}}"
ws_url="${DEMO_WS_URL:-ws://localhost:${GATEWAY_WS_PORT:-7070}/ws}"
streams="${DEMO_STREAMS:-12}"
duration="${DEMO_DURATION:-40s}"

# The victim is CHOSEN AT RUNTIME, not hardcoded, because the router does
# not spread load evenly: it scores on latency as well as outstanding
# count, so the fastest worker accumulates most of the fleet's sessions. A
# 12-stream run distributes as roughly 9/1/2 across three workers, with
# two streaming workers picked not at all. Killing a fixed worker-a would
# therefore assert recovery on a worker holding one session out of twelve
# — or none, in which case the demo "passes" having demonstrated nothing.
#
# The same trap as M7's findPinnedWorker bug, in a different costume.
# DEMO_VICTIM overrides it for a deliberate experiment.
pick_victim() {
  curl --fail --silent "${gateway}/api/debug/workers" | python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
busiest = max(workers, key=lambda w: w['outstanding'], default=None)
print(busiest['id'] if busiest and busiest['outstanding'] > 0 else '')
"
}

log() { printf '\n\033[1m%s\033[0m\n' "$*"; }

metric() { curl --fail --silent "${gateway}/api/debug/metrics" | python3 -c "import json,sys;print(json.load(sys.stdin)['$1'])"; }

cleanup() {
  # Always put the victim back, even on failure: a demo that leaves the
  # fleet one worker short poisons every run after it.
  if [[ -n "${victim:-}" ]]; then
    curl --fail --silent -X POST "${gateway}/api/chaos/${victim}/restore" >/dev/null 2>&1 || true
    curl --fail --silent -X POST "${gateway}/api/chaos/${victim}/reset" >/dev/null 2>&1 || true
  fi
  # Kill the compiled BINARY, not the `go run` wrapper. `go run` execs the
  # build output as a child whose argv does not contain "./cmd/loadgen",
  # so killing the wrapper by that pattern leaves a load generator running
  # against the stack — silently doubling the load on the next run and
  # making its numbers a fiction. Build it explicitly and hold its PID.
  [[ -n "${loadgen_pid:-}" ]] && kill "${loadgen_pid}" 2>/dev/null || true
}
trap cleanup EXIT

log "Bringing the stack up"
docker compose up -d --build

for _ in $(seq 1 60); do
  curl --fail --silent "${gateway}/health" >/dev/null && break
  sleep 1
done
curl --fail --silent "${gateway}/health" >/dev/null || { echo "gateway never became healthy" >&2; exit 1; }

echo
echo "  Dashboard: http://localhost:${dashboard_port}/dashboard/"
echo "  Open it now — the rest of this script is what the buttons do."
echo

log "Applying load: ${streams} streams for ${duration}"
loadgen_bin="$(mktemp -t loadgen.XXXXXX)"
go build -o "${loadgen_bin}" ./cmd/loadgen
"${loadgen_bin}" -streams "${streams}" -ramp 6s -duration "${duration}" -ws-url "${ws_url}" &
loadgen_pid=$!

# Let the fleet settle into steady state before breaking it, or the
# "before" latency is really the ramp.
sleep 12

victim="${DEMO_VICTIM:-$(pick_victim)}"
if [[ -z "${victim}" ]]; then
  echo "FAIL: no worker is holding a session — nothing to kill. Is the load generator connecting?" >&2
  exit 1
fi

before_failovers=$(metric failover_total)
before_dupes=$(metric duplicate_finals_total)

log "Killing ${victim} (SIGKILL) via the dashboard's own control plane — it holds the most sessions"
curl --fail --silent -X POST "${gateway}/api/chaos/${victim}/kill" | head -c 200
echo

sleep 10

log "Restoring ${victim}"
curl --fail --silent -X POST "${gateway}/api/chaos/${victim}/restore" >/dev/null

wait "${loadgen_pid}" || true
unset loadgen_pid

after_failovers=$(metric failover_total)
after_dupes=$(metric duplicate_finals_total)
recovered=$(( after_failovers - before_failovers ))
dupes=$(( after_dupes - before_dupes ))

log "Result"
printf '  failovers during the kill : %d\n' "${recovered}"
printf '  duplicate finals          : %d  (must be 0)\n' "${dupes}"
curl --fail --silent "${gateway}/api/debug/metrics" | python3 -m json.tool

fail=0
if (( recovered < 1 )); then
  echo "FAIL: killing ${victim} produced no failover — nothing was pinned there, or recovery did not run" >&2
  fail=1
fi
if (( dupes != 0 )); then
  echo "FAIL: ${dupes} duplicate finals — invariant violated" >&2
  fail=1
fi
(( fail == 0 )) && log "PASS — sessions survived the kill with no duplicate finals"
exit "${fail}"
