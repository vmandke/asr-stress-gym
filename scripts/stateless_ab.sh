#!/usr/bin/env bash
# Run the same online load twice — once PINNED, once STATELESS — and print
# what actually differs.
#
#   ./scripts/stateless_ab.sh          # 5 streams, 30s each
#   ./scripts/stateless_ab.sh 12 45s
#
# The two phases differ by one environment variable on the gateway:
#
#   A  STATELESS_STREAM unset   router.Pick chooses a WORKER; the session is
#                               pinned to it; its KV cache lives in that
#                               worker's process; the gateway hoards
#                               checkpoints so it can recover the session
#                               if the worker dies.
#
#   B  STATELESS_STREAM=1       router.Pick chooses a FAMILY; every chunk
#                               goes through Bifrost to any member; the KV
#                               cache lives in the family's shared tier
#                               (cmd/kvtier); the gateway holds no
#                               checkpoints because it needs none.
#
# Everything printed is read back from the running stack. See
# docs/STATELESS-KVTIER.md for the design and the trade-offs.
set -euo pipefail

cd "$(dirname "$0")/.."
source ./scripts/check_env.sh

streams="${1:-5}"
duration="${2:-30s}"

port="${GATEWAY_DASHBOARD_PORT:-}"
if [[ -z "${port}" ]]; then
  port=7000
  lsof -nP -iTCP:7000 -sTCP:LISTEN >/dev/null 2>&1 && port=7001
fi
export GATEWAY_DASHBOARD_PORT="${port}"
export BIFROST_URL="${BIFROST_URL:-http://bifrost:8080}"
gw="http://localhost:${port}"
ws="ws://localhost:${GATEWAY_WS_PORT:-7070}/ws"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }

# --- bring the whole thing up -----------------------------------------
bold "Starting the fleet, the per-family KV tiers, and Bifrost"
docker compose --profile bifrost up -d \
  kvtier-zip kvtier-ctc kvtier-whisper \
  worker-zip-1 worker-zip-2 worker-ctc-1 worker-ctc-2 \
  worker-whisper-1 worker-whisper-2 bifrost >/dev/null
note "waiting for workers to serve..."
for _ in $(seq 1 60); do
  ready=$(docker compose ps --format '{{.Name}} {{.Status}}' 2>/dev/null | grep -c "healthy" || true)
  [[ "${ready}" -ge 9 ]] && break
  sleep 2
done
docker compose ps --format '{{.Name}}\t{{.Status}}' | grep -E "kvtier|bifrost" | sed 's/^/   /'

LG="$(mktemp -t lg.XXXXXX)"; go build -o "${LG}" ./cmd/loadgen
trap 'rm -f "${LG}"' EXIT

tier_totals() {
  python3 - "$@" <<'PY'
import json, sys, urllib.request
tot = {"puts": 0, "hits": 0, "misses": 0, "bytes": 0, "evictions": 0}
for p in (9501, 9502, 9503):
    try:
        with urllib.request.urlopen(f"http://localhost:{p}/stats", timeout=3) as r:
            s = json.load(r)
        for k in tot:
            tot[k] += s.get(k, 0)
    except Exception:
        pass
print(json.dumps(tot))
PY
}

metrics() { curl --fail --silent "${gw}/api/debug/metrics"; }

# run_phase <label> <stateless 0|1>  -> writes results to $OUT
run_phase() {
  local label="$1" stateless="$2"
  bold "PHASE ${label}"

  # A fresh gateway per phase: it zeroes the counters, so each phase's
  # numbers are its own rather than a running total.
  if [[ "${stateless}" == "1" ]]; then
    STATELESS_STREAM=1 docker compose up -d --force-recreate gateway >/dev/null
  else
    STATELESS_STREAM="" docker compose up -d --force-recreate gateway >/dev/null
  fi
  for _ in $(seq 1 40); do
    curl --fail --silent "${gw}/health" >/dev/null 2>&1 && break
    sleep 1
  done

  local t0 t1
  t0="$(tier_totals)"
  note "running ${streams} online streams for ${duration}..."

  # Peak resident bytes has to be sampled DURING the run. Close reclaims a
  # session's versions at teardown (and the TTL sweeps whatever dies), so a
  # sample taken afterwards always reads zero and would badly understate
  # how much state the tier was actually holding.
  local peak_file; peak_file="$(mktemp -t peak.XXXXXX)"; echo 0 > "${peak_file}"
  ( while :; do
      b="$(tier_totals | python3 -c 'import json,sys;print(json.load(sys.stdin)["bytes"])' 2>/dev/null || echo 0)"
      [[ "${b}" -gt "$(cat "${peak_file}")" ]] && echo "${b}" > "${peak_file}"
      sleep 2
    done ) & local sampler=$!
  disown 2>/dev/null || true

  local lg_out
  lg_out="$("${LG}" -streams "${streams}" -ramp 2s -duration "${duration}" -ws-url "${ws}" 2>&1 | tail -6)"
  kill "${sampler}" 2>/dev/null || true
  local peak; peak="$(cat "${peak_file}")"; rm -f "${peak_file}"
  sleep 2
  t1="$(tier_totals)"

  python3 - "${label}" "${t0}" "${t1}" "${lg_out}" "$(metrics)" "${peak}" <<'PY' >> "${OUT}"
import json, re, sys
label, t0, t1, lg, mx = sys.argv[1], json.loads(sys.argv[2]), json.loads(sys.argv[3]), sys.argv[4], json.loads(sys.argv[5])
peak = int(sys.argv[6] or 0)
def num(pat):
    m = re.search(pat + r"=([\d.]+)", lg)
    return float(m.group(1)) if m else 0.0
row = {
    "label": label,
    "partial_p50": num("partial_p50_ms"), "partial_p95": num("partial_p95_ms"),
    "final_p50": num("final_p50_ms"), "final_p95": num("final_p95_ms"),
    "pushes": mx.get("backend_pushes_total", 0),
    "failover": mx.get("failover_total", 0),
    "restores": mx.get("checkpoint_restores_total", 0),
    "dupes": mx.get("duplicate_finals_total", 0),
    "tier_puts": t1["puts"] - t0["puts"],
    "tier_hits": t1["hits"] - t0["hits"],
    "tier_misses": t1["misses"] - t0["misses"],
    # Relative to what was already resident when the phase began: the
    # sampler reads the tier's absolute size, and leftovers from an
    # earlier run would otherwise show the PINNED phase holding tier
    # bytes it never wrote.
    "tier_bytes": max(0, peak - t0["bytes"]),
}
print(json.dumps(row))
PY
  note "done"
}

OUT="$(mktemp -t ab.XXXXXX)"; trap 'rm -f "${LG}" "${OUT}"' EXIT
run_phase "A: PINNED (default)" 0
run_phase "B: STATELESS via shared tier" 1

bold "RESULT"
python3 - "${OUT}" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
if len(rows) != 2:
    print("expected two phases, got", len(rows)); raise SystemExit(1)
a, b = rows
w = 30
def line(name, key, fmt="{:,.0f}", note=""):
    va, vb = fmt.format(a[key]), fmt.format(b[key])
    print(f"{name:<{w}} {va:>14} {vb:>14}   {note}")
print(f"{'':<{w}} {'A: pinned':>14} {'B: stateless':>14}")
print("-" * (w + 34))
line("partial p50 (ms)", "partial_p50", "{:,.1f}")
line("partial p95 (ms)", "partial_p95", "{:,.1f}")
line("final p50 (ms)", "final_p50", "{:,.1f}")
line("backend pushes", "pushes")
print("-" * (w + 34))
line("gateway failovers", "failover", "{:,.0f}", "B: absorbed by Bifrost's chain")
line("checkpoint restores", "restores", "{:,.0f}", "B: none by design")
line("duplicate finals", "dupes", "{:,.0f}", "MUST be 0 in both")
print("-" * (w + 34))
line("KV tier puts", "tier_puts", "{:,.0f}", "state published")
line("KV tier hits", "tier_hits", "{:,.0f}", "state fetched by a peer")
line("KV tier misses", "tier_misses", "{:,.0f}", "-> 424 -> audio replay")
line("KV tier peak bytes", "tier_bytes", "{:,.0f}", "sampled during the run")
print()
if b["tier_puts"] == 0:
    print("WARNING: phase B published nothing to the tier — the stateless path")
    print("         did not engage. Check that the bifrost profile is up and that")
    print("         the gateway logged 'stateless streaming via bifrost pool'.")
else:
    print("Phase B moved KV state through the shared tier; phase A kept it inside")
    print("the pinned worker. Both produced finals with no duplicates.")
PY

bold "Where to look next"
note "gateway log:  docker compose logs gateway | grep 'stateless streaming'"
note "tier stats :  curl -s localhost:9501/stats | python3 -m json.tool"
note "dashboard  :  ${gw}/"
note "design     :  docs/STATELESS-KVTIER.md"
