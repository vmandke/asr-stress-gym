#!/usr/bin/env bash
# Exercise the two failure properties of the live, stateless demo:
#
#   1. A session whose Bifrost primary dies completes through a dynamically
#      added, cache-compatible peer without a client error or duplicate final.
#   2. Admission rejects excess *new* sessions while admitted sessions finish.
#
# The script deliberately uses the gateway's dashboard API for every worker
# action. Dynamic workers have Compose-network addresses, not invented host
# ports, so a test that curls worker-zip-2:19002 is testing an old topology.
# Run `make live` first. The peer created for the first check remains visible
# in the dashboard afterwards, which makes the recovery easy to inspect.
set -euo pipefail

cd "$(dirname "$0")/.."
source ./scripts/check_env.sh

port="${GATEWAY_DASHBOARD_PORT:-7000}"
gateway="${GATEWAY_DEBUG_URL:-http://localhost:${port}}"
ws="${GATEWAY_WS_URL:-ws://localhost:${GATEWAY_WS_PORT:-7070}/ws}"
seed="worker-zip-1"
load_pid=""

fail() { echo "FAIL chaos: $*" >&2; exit 1; }
api() { curl --fail --silent --show-error "$@"; }

worker_ids() {
  api "${gateway}/api/debug/workers" | python3 -c '
import json, sys
for worker in json.load(sys.stdin)["workers"]:
    print(worker["id"])
'
}

cleanup() {
  [[ -n "${load_pid}" ]] && kill "${load_pid}" 2>/dev/null || true
  for id in $(worker_ids); do
    api -X POST "${gateway}/api/chaos/${id}/drain?on=false" >/dev/null 2>&1 || true
    api -X POST "${gateway}/api/chaos/${id}/reset" >/dev/null 2>&1 || true
  done
  api -X POST "${gateway}/api/chaos/${seed}/restore" >/dev/null 2>&1 || true
}
trap cleanup EXIT

api "${gateway}/health" >/dev/null || fail "gateway is not ready at ${gateway}; run make live first"

peer_for() {
  local family="$1" peer
  local seed_id="worker-${family}-1"
  peer="$(api "${gateway}/api/debug/workers" | python3 -c '
import json, sys
family, seed = sys.argv[1:]
for worker in json.load(sys.stdin)["workers"]:
    if worker["id"].startswith(f"worker-{family}-") and worker["id"] != seed and worker["status"] != "ejected":
        print(worker["id"])
        break
' "${family}" "${seed_id}")"
  if [[ -z "${peer}" ]]; then
    peer="$(api -X POST "${gateway}/api/fleet/${family}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["worker"])')"
  fi
  for _ in $(seq 1 45); do
    if api "${gateway}/api/debug/workers" | python3 -c '
import json, sys
want = sys.argv[1]
raise SystemExit(0 if any(w["id"] == want and w["status"] != "ejected" for w in json.load(sys.stdin)["workers"]) else 1)
' "${peer}"
    then
      echo "${peer}"
      return
    fi
    sleep 1
  done
  fail "${peer} did not become routeable"
}

wait_until_healthy() {
  local id="$1"
  for _ in $(seq 1 45); do
    if api "${gateway}/api/nodes" | python3 -c '
import json, sys
want = sys.argv[1]
raise SystemExit(0 if any(n["node"] == want and n["ok"] for n in json.load(sys.stdin)["latest"]) else 1)
' "${id}"
    then
      return
    fi
    sleep 1
  done
  fail "${id} did not become healthy after restore"
}

echo "--- chaos 1: kill a Bifrost primary; its same-family peer must finish the stream ---"
peer="$(peer_for zip)"
echo "    peer: ${peer}"

# Force this one session to choose the zip seed as its initial primary. Once
# it is open, un-drain only the compatible peer; Bifrost's fallback list was
# captured from that cohort when the session began.
for id in $(worker_ids); do
  [[ "${id}" == "${seed}" ]] || api -X POST "${gateway}/api/chaos/${id}/drain?on=true" >/dev/null
done

out="$(mktemp -t asr-chaos.XXXXXX)"
go run ./cmd/loadgen --streams 1 --duration 30s --ws-url "${ws}" >"${out}" 2>&1 &
load_pid=$!
for _ in $(seq 1 20); do
  if api "${gateway}/api/streams" | python3 -c '
import json, sys
seed = sys.argv[1]
raise SystemExit(0 if any(s["worker"] == seed and s["state"] != "ENDED" for s in json.load(sys.stdin)["streams"]) else 1)
' "${seed}"
  then break; fi
  sleep 1
done
api "${gateway}/api/streams" | python3 -c '
import json, sys
seed = sys.argv[1]
raise SystemExit(0 if any(s["worker"] == seed and s["state"] != "ENDED" for s in json.load(sys.stdin)["streams"]) else 1)
' "${seed}" || fail "the test stream did not select ${seed}"

api -X POST "${gateway}/api/chaos/${peer}/drain?on=false" >/dev/null
api -X POST "${gateway}/api/chaos/${seed}/kill" >/dev/null
wait "${load_pid}"; load_pid=""
api -X POST "${gateway}/api/chaos/${seed}/restore" >/dev/null
wait_until_healthy "${seed}"

summary="$(cat "${out}")"; rm -f "${out}"
echo "${summary}"
field() { printf '%s\n' "${summary}" | tr ' ' '\n' | awk -F= -v key="$1" '$1 == key { print $2; exit }'; }
[[ "$(field errors)" == "0" ]] || fail "primary failure reached the client"
[[ "$(field duplicate_finals)" == "0" ]] || fail "primary failure emitted a duplicate final"
[[ "$(field finals)" -gt 0 ]] || fail "the stream produced no final"
echo "PASS chaos 1: ${seed} failed; ${peer} remained available; client completed cleanly"

echo "--- chaos 2: admission overload protects active streams ---"
CEILING="${CHAOS_CEILING:-12}" SOFT="${CHAOS_SOFT:-9}" \
  GATEWAY_DASHBOARD_PORT="${port}" GATEWAY_DEBUG_URL="${gateway}" GATEWAY_WS_URL="${ws}" \
  ./scripts/scenarios/09_overload.sh
echo "PASS chaos: stateless primary failure and admission overload"
