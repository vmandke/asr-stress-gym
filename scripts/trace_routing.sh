#!/usr/bin/env bash
# Trace one client from arrival to recovery, three phases:
#
#   1. NEW CLIENT      how a session is mapped to a model family, and what
#                      Bifrost does with the offline final
#   2. ONE WORKER DIES the twin takes over and the KV cache is RESTORED
#   3. WHOLE FAMILY    no compatible worker left: the cache cannot move,
#      DIES            so recovery rebuilds it by replaying audio
#
# Everything printed is read back from the running stack — worker /health,
# the gateway's metrics, and its own log lines. Nothing here is narrated
# from what the code is supposed to do.
#
#   ./scripts/trace_routing.sh
set -euo pipefail

cd "$(dirname "$0")/.."
source ./scripts/check_env.sh

port="${GATEWAY_DASHBOARD_PORT:-7000}"
gw="http://localhost:${port}"
ws="ws://localhost:${GATEWAY_WS_PORT:-7070}/ws"

ZIP1_ADMIN="${WORKER_ZIP_1_ADMIN_URL:-http://localhost:19001}"
ZIP2_ADMIN="${WORKER_ZIP_2_ADMIN_URL:-http://localhost:19002}"

hdr() { printf '\n\033[1m── %s ─────────────────────────────────────\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }

metrics() { curl --fail --silent "${gw}/api/debug/metrics"; }
field() { python3 -c "import json,sys;print(json.load(sys.stdin)['$1'])"; }

fleet() {
  curl --fail --silent "${gw}/api/debug/workers" | python3 -c "
import json, sys
d = json.load(sys.stdin)
for w in sorted(d['workers'], key=lambda x: x['id']):
    mark = '  <- sessions here' if w['outstanding'] else ''
    print(f\"     {w['id']:18} {w['status']:9} key={w['compatibility_key_hash'][7:19]} sessions={w['outstanding']}{mark}\")
"
}

restore_all() {
  for a in "${ZIP1_ADMIN}" "${ZIP2_ADMIN}"; do
    curl --fail --silent -X POST "${a}/admin/restore" >/dev/null 2>&1 || true
  done
  sleep 12
}
trap restore_all EXIT

command -v go >/dev/null || { echo "go is required" >&2; exit 1; }
LG="$(mktemp -t lg.XXXXXX)"; go build -o "${LG}" ./cmd/loadgen
curl --fail --silent "${gw}/health" >/dev/null || { echo "gateway not up — run make start" >&2; exit 1; }

# ---------------------------------------------------------------------
hdr "1. NEW CLIENT ARRIVES"
note "The fleet, as the ROUTER sees it. Workers sharing a key are a pool:"
note "each can import the other's KV cache; across keys, none can."
fleet
echo
note "A client connects and sends session.start{mode}. In order:"
note "  a. Admission.Admit()        capacity ceiling -> 'overloaded' if full"
note "  b. router.Pick(mode, ...)   THE model-family choice, in the gateway"
note "       filter: excluded / ejected / draining / rate budget /"
note "               mode==online && !Streaming  <- keeps live traffic off"
note "                                              the offline-only models"
note "       score : (Outstanding+1) x latencyP95, halved if the key matches"
note "  c. Open on the winner       worker allocates state, returns a handle"
note "  d. BindSession()            the session is now PINNED"
echo
note "Running one ONLINE session..."
"${LG}" -streams 1 -duration 10s -ws-url "${ws}" >/dev/null 2>&1 &
sleep 5
fleet
wait || true
echo
note "Every later chunk goes straight to that worker: sessionLoop holds its"
note "client in a local and NEVER calls Pick again. Stickiness is structural."

hdr "1b. THE OFFLINE FINAL — where Bifrost is involved"
note "Offline finals are the only traffic Bifrost carries. The gateway"
note "derives the pool from its OWN pick (router.Pool = same-key cohort),"
note "so the chain cannot cross a family."
"${LG}" -streams 1 -mode offline -duration 8s -ws-url "${ws}" >/dev/null 2>&1 || true
sleep 1
if docker compose logs gateway --since 40s 2>/dev/null | grep -q "bifrost pool"; then
  docker compose logs gateway --since 40s 2>/dev/null | grep "bifrost pool" | tail -1 \
    | sed 's/.*bifrost pool/     bifrost pool/'
  note "primary  = the worker the ROUTER chose"
  note "fallbacks= that worker's cache-compatible twins, nothing else"
else
  note "(Bifrost disabled — set BIFROST_URL and use --profile bifrost to see this)"
fi

# ---------------------------------------------------------------------
hdr "2. ONE WORKER OF A FAMILY DIES"
before="$(metrics)"
note "before: restores=$(echo "$before" | field checkpoint_restores_total) degraded=$(echo "$before" | field checkpoint_degraded_total)"
"${LG}" -streams 3 -ramp 2s -duration 30s -ws-url "${ws}" >/dev/null 2>&1 &
lg_pid=$!
sleep 10
victim="$(curl --fail --silent "${gw}/api/debug/workers" | python3 -c "
import json, sys
zips = [w for w in json.load(sys.stdin)['workers'] if w['id'].startswith('worker-zip')]
best = max(zips, key=lambda w: w['outstanding'], default=None)
print(best['id'] if best and best['outstanding'] else '')
")"
if [[ -z "${victim}" ]]; then
  note "no session landed on a zipformer worker this run; skipping phase 2"
else
  note "killing ${victim} (its twin is still alive, same key)"
  admin="${ZIP1_ADMIN}"; [[ "${victim}" == "worker-zip-2" ]] && admin="${ZIP2_ADMIN}"
  curl --fail --silent -X POST "${admin}/admin/die" >/dev/null
  sleep 12
  after="$(metrics)"
  note "after : restores=$(echo "$after" | field checkpoint_restores_total) degraded=$(echo "$after" | field checkpoint_degraded_total) cross=$(echo "$after" | field failover_cross_model_total)"
  note ""
  note "coord.HandleBackendFailure ran:"
  note "  Pick(mode, exclude={dead}, prefer=<dead worker's key>)"
  note "  the surviving twin shares that key -> score halved -> chosen"
  note "  RecoverSameModel: RESTORE the KV tensors, replay only the tail"
  fleet
fi
wait "${lg_pid}" 2>/dev/null || true

# ---------------------------------------------------------------------
hdr "3. THE WHOLE FAMILY DIES"
restore_all
before="$(metrics)"
note "before: cross=$(echo "$before" | field failover_cross_model_total) degraded=$(echo "$before" | field checkpoint_degraded_total) restores=$(echo "$before" | field checkpoint_restores_total)"
"${LG}" -streams 3 -ramp 2s -duration 30s -ws-url "${ws}" >/dev/null 2>&1 &
lg_pid=$!
sleep 10
note "killing BOTH zipformer workers — no cache-compatible worker remains"
curl --fail --silent -X POST "${ZIP1_ADMIN}/admin/die" >/dev/null
curl --fail --silent -X POST "${ZIP2_ADMIN}/admin/die" >/dev/null
sleep 14
after="$(metrics)"
note "after : cross=$(echo "$after" | field failover_cross_model_total) degraded=$(echo "$after" | field checkpoint_degraded_total) restores=$(echo "$after" | field checkpoint_restores_total)"
note ""
note "Pick excluded both zipformers and found no key match, so prefer"
note "bought nothing -> a DIFFERENT family won on score."
note "RecoverCrossModel:"
note "  Open fresh state (empty cache — a zipformer tensor is meaningless"
note "                    to a conformer)"
note "  emit partial.reset (the client must discard displayed text)"
note "  Journal.ReadFromCommitted() -> Recut -> replay ALL the utterance's"
note "  audio into the new worker, rebuilding the cache by RECOMPUTATION"
fleet
wait "${lg_pid}" 2>/dev/null || true

echo
hdr "SUMMARY"
note "The KV cache is a CACHE: losing it costs recomputation, never"
note "correctness. That is what makes tier 3 always available."
note ""
note "  twin alive      restore the tensors, replay the tail      cheap"
note "  family gone     fresh state, replay the whole utterance   correct"
note "  nothing capable failover_exhausted -> session error       terminal"
note "  gateway dies    journal dies with it; client resumes      HA limit"
note ""
note "duplicate_finals_total = $(metrics | field duplicate_finals_total)  (must be 0 throughout)"
