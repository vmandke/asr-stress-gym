#!/usr/bin/env bash
# One command for the full live setup: the fleet, the per-family KV tiers,
# Bifrost, stateless streaming, background load, and the microphone page.
#
#   ./scripts/live.sh                     # two workers, no background load
#   ./scripts/live.sh 0                   # two workers, no background load
#   NO_BUILD=1 ./scripts/live.sh
#
# This differs from start.sh in one way that matters: start.sh brings up
# the DEFAULT stack, where a session is pinned to one worker and Bifrost
# carries only offline finals. This one turns on STATELESS_STREAM, so a
# session's KV cache lives in its model family's shared tier and every
# chunk is routed by Bifrost within that family
# (docs/KVCACHE.md).
#
# **It rebuilds by default, and that is deliberate.** A stale worker image
# silently ignores the `kv_mode` form field and quietly serves the request
# on the old one-shot path — it does not error, it just does something
# else. That produced three false findings while this was being built, and
# a runner that skips the rebuild to save a minute would keep producing
# them. NO_BUILD=1 when you know the images are current.
#
# Ends by PROVING the stateless path actually engaged rather than assuming
# it, because "the flag is set" and "the flag had an effect" are different
# claims and only the second one is worth anything.
set -euo pipefail

cd "$(dirname "$0")/.."
source ./scripts/check_env.sh

streams="${1:-0}"
duration="${DEMO_DURATION:-30m}"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
fail() { printf '\n\033[1;31m%s\033[0m\n' "$*" >&2; exit 1; }

worker_urls="worker-zip-1=http://worker-zip-1:9000,worker-ctc-1=http://worker-ctc-1:9000"
worker_ports=(18001 18003)
admin_ports=(19001 19003)
export WORKER_URLS="${worker_urls}"

# --- 0. the two settings this script exists to turn on ----------------
export STATELESS_STREAM=1
export BIFROST_URL="${BIFROST_URL:-http://bifrost:8080}"

bold "Launch plan"
note "gateway: WebSocket + VAD + state references"
note "Bifrost: ${BIFROST_URL} (stateless request routing)"
note "KV tiers: kvtier-zip, kvtier-ctc"
note "seed workers: worker-zip-1, worker-ctc-1"
note "fleet manager: enabled — add workers from the dashboard"
note "background load: ${streams} stream(s), duration ${duration}"

port="${GATEWAY_DASHBOARD_PORT:-}"
if [[ -z "${port}" ]]; then
  port=7000
  if lsof -nP -iTCP:7000 -sTCP:LISTEN >/dev/null 2>&1; then
    port=7001
    note "port 7000 is taken (on macOS this is usually AirPlay Receiver); using ${port}"
  fi
fi
export GATEWAY_DASHBOARD_PORT="${port}"
gw="http://localhost:${port}"

# --- 1. revive anything earlier chaos killed --------------------------
#
# Before compose, not after: an unhealthy leftover blocks the gateway's
# own dependency check, so the fleet would otherwise block its own
# recovery. /admin/restore on a live child is a harmless no-op.
bold "Reviving selected workers"
revived=0
for p in "${admin_ports[@]}"; do
  curl --fail --silent --max-time 2 -X POST "http://localhost:${p}/admin/restore" >/dev/null 2>&1 && revived=$((revived + 1))
done
if (( revived > 0 )); then note "restored ${revived} supervisor(s); letting their children load weights"; sleep 10
else note "nothing running yet — first start"; fi

# --- 2. build and start -----------------------------------------------
if [[ -n "${NO_BUILD:-}" ]]; then
  bold "Starting (NO_BUILD set — images assumed current)"
  docker compose up -d
else
  bold "Building and starting everything"
  note "rebuilding so no container silently runs pre-kv_mode code"
  docker compose up -d --build
fi

printf '   waiting for the gateway '
for _ in $(seq 1 120); do
  curl --fail --silent --max-time 2 "${gw}/health" >/dev/null 2>&1 && { echo " ready"; break; }
  printf '.'; sleep 1
done
curl --fail --silent --max-time 2 "${gw}/health" >/dev/null 2>&1 || {
  echo; docker compose logs gateway --tail 30 >&2
  fail "gateway never became healthy"
}

bold "Running services"
docker compose ps --format 'table {{.Name}}\t{{.Status}}' || true

# --- 3. clear fault injection left over from an earlier session -------
#
# A blackhole or a 429 rate survives a gateway restart: it is state inside
# the WORKER. Starting on top of one produces a fleet that looks broken
# for no visible reason.
for p in "${worker_ports[@]}"; do
  curl --fail --silent --max-time 2 -X POST "http://localhost:${p}/admin/reset" >/dev/null 2>&1 || true
done

# --- 4. the per-family KV tiers ---------------------------------------
bold "Seed model families (one worker each)"
curl --fail --silent "${gw}/api/kv" | python3 -c "
import json, sys
d = json.load(sys.stdin)
if not d.get('available'):
    print('   NO TIERS — the gateway sees no worker advertising one'); raise SystemExit(1)
for p in sorted(d['pools'], key=lambda x: x['workers'][0]):
    L, err = p.get('layout') or {}, p.get('error')
    fam = L.get('model_family', '?')
    n, kv, tot = L.get('tensor_count', '?'), L.get('kv_bytes', 0), L.get('total_bytes', 0)
    print(f\"   {','.join(p['workers']):30} {p['tier'] or 'none':16} {fam:16} \"
          f\"{n:>3} tensors  kv={kv/1048576:.2f}MB  state={tot/1048576:.2f}MB\"
          + (f'  ERR {err}' if err else ''))
" || fail "the KV tiers are not reachable — is this the rebuilt image?"

# --- 5. load ----------------------------------------------------------
probe_only=0
if [[ "${streams}" == "0" ]]; then
  # Even with no load requested, a couple of streams have to run for a
  # moment: the only honest way to show the stateless path engaged is to
  # observe a session on it.
  probe_only=1; streams=2
  bold "No load requested — running a 2-stream probe to verify the path"
else
  bold "Starting ${streams} background streams"
fi
curl --fail --silent -X POST "${gw}/api/load/${streams}?duration=${duration}" >/dev/null \
  || note "could not reach the load generator; the page still works, press a load button there"
sleep 12

# --- 6. prove the stateless path actually engaged ---------------------
#
# "STATELESS_STREAM is set" and "sessions are running on the shared tier"
# are different claims. A kv:-prefixed handle is the observable one: it is
# the reference prefix backend.StatelessClient names a session's versions
# under, so it cannot appear unless that client is the one serving.
bold "Verifying"
verdict="$(curl --fail --silent "${gw}/api/streams" | python3 -c "
import json, sys
s = json.load(sys.stdin)['streams']
live = [x for x in s if x['state'] != 'ENDED']
kv = [x for x in live if (x.get('handle') or '').startswith('kv:')]
print(f\"{len(live)}|{len(kv)}|{kv[0]['id'] if kv else ''}|{kv[0]['worker'] if kv else ''}\")
")"
IFS='|' read -r n_live n_kv sid worker <<< "${verdict}"
if [[ "${n_kv:-0}" -gt 0 ]]; then
  note "${n_kv}/${n_live} live sessions use the shared KV tier"
  note "example session ${sid}; family selected at start: ${worker}"
else
  note "WARNING: no session is on the shared tier."
  note "  every live session shows a plain worker handle, so STATELESS_STREAM"
  note "  did not take effect. Usually a stale image, or bifrost not up:"
  note "  docker compose logs gateway | grep -i 'stateless\\|bifrost'"
fi

if command -v node >/dev/null 2>&1; then
  note ""
  note "checking the mic page's own framing code against this gateway..."
  if node scripts/test_mic_protocol.mjs >/tmp/micproto.$$ 2>&1; then
    note "$(grep -E '^final' /tmp/micproto.$$ || echo 'ok')"
    note "the page's encoder is accepted by this gateway — the mic will work"
  else
    note "mic protocol check FAILED:"; sed 's/^/     /' /tmp/micproto.$$ | tail -6
  fi
  rm -f /tmp/micproto.$$
fi

if (( probe_only )); then
  curl --fail --silent -X POST "${gw}/api/load/stop" >/dev/null 2>&1 || true
  note ""
  note "probe finished; background load stopped as requested"
fi

# --- 7. where to go ---------------------------------------------------
bold "Open this"
echo "   🎤  ${gw}/dashboard/mic.html     speak into the fleet, live"
echo "   📊  ${gw}/dashboard/             workers and active streams"
echo
echo "   Microphone capture needs a secure context. localhost counts as one,"
echo "   a LAN IP does not — open the URL exactly as printed."
echo
echo "   The dashboard starts with one worker per model family."
echo "   Use + zip or + ctc in the dashboard to launch a compatible worker."
echo
echo "   Stop the load:   curl -XPOST ${gw}/api/load/stop"
echo "   Stop everything: make down"
