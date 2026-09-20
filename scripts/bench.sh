#!/usr/bin/env bash
# M8's unattended measurement runner. Results are deliberately timestamped
# and ignored by git: a benchmark is evidence tied to a machine and run, not
# source code. See docs/BENCH.md for the loadgen CSV contract.
set -euo pipefail
cd "$(dirname "$0")/.."

source ./scripts/check_env.sh

dashboard_port="${GATEWAY_DASHBOARD_PORT:-7001}"
gateway_url="${GATEWAY_DEBUG_URL:-http://localhost:${dashboard_port}}"
gateway_ws="${GATEWAY_WS_URL:-ws://localhost:${GATEWAY_WS_PORT:-7070}/ws}"
corpus_dir="${BENCH_CORPUS:-corpus/large}"
duration="${BENCH_DURATION:-10s}"
results_dir="${BENCH_OUT:-results/bench-$(date +%Y%m%d-%H%M%S)}"
capacity_ceiling="${BENCH_CAPACITY_CEILING:-16}"
mkdir -p "${results_dir}"

if ! find "${corpus_dir}" -name '*.wav' -print -quit 2>/dev/null | grep -q .; then
  echo "bench: ${corpus_dir} is missing; generating the reproducible large corpus" >&2
  ./scripts/gen_corpus_large.py --count "${BENCH_CORPUS_COUNT:-48}"
fi
clip="$(find "${corpus_dir}" -name '*.wav' | sort | head -1)"
[[ -n "${clip}" ]] || { echo "bench: no WAV files under ${corpus_dir}" >&2; exit 1; }

wait_gateway() {
  for _ in $(seq 1 60); do
    curl --fail --silent "${gateway_url}/health" >/dev/null && return 0
    sleep 1
  done
  echo "bench: gateway did not become healthy at ${gateway_url}" >&2
  return 1
}

restore_worker() {
  local admin="$1" health="$2"
  curl --fail --silent -X POST "${admin}/admin/die" >/dev/null || true
  curl --fail --silent -X POST "${admin}/admin/restore" >/dev/null
  for _ in $(seq 1 60); do
    curl --fail --silent "${health}/health" >/dev/null && return 0
    sleep 0.5
  done
  echo "bench: worker at ${health} did not return" >&2
  return 1
}

restore_default_fleet() {
  restore_worker "${WORKER_A_ADMIN_URL:-http://localhost:19001}" "${WORKER_A_URL:-http://localhost:18001}"
  restore_worker "${WORKER_B_ADMIN_URL:-http://localhost:19002}" "${WORKER_B_URL:-http://localhost:18002}"
  restore_worker "${WORKER_C_ADMIN_URL:-http://localhost:19003}" "${WORKER_C_URL:-http://localhost:18003}"
  restore_worker "${WORKER_D_ADMIN_URL:-http://localhost:19004}" "${WORKER_D_URL:-http://localhost:18004}"
}

restore_default_gateway() {
  GATEWAY_DASHBOARD_PORT="${dashboard_port}" docker compose up -d --force-recreate gateway >/dev/null
  wait_gateway
}

cleanup() {
  # Faults must never leak from a benchmark into the next manual run. Keep
  # the stack up for chart inspection, but restore its ordinary routing.
  restore_default_fleet || true
  restore_default_gateway || true
}
trap cleanup EXIT

echo "bench: starting full fleet plus the M8 mock peer"
GATEWAY_DASHBOARD_PORT="${dashboard_port}" docker compose --profile bench --profile models up -d --build
wait_gateway

printf '{"corpus":"%s","duration":"%s","kill_ms":4000}\n' "${corpus_dir}" "${duration}" >"${results_dir}/metadata.json"

echo "A: cache reuse versus rebuilding accumulated audio"
python3 ./scripts/bench_cache.py --worker-url "${WORKER_A_URL:-http://localhost:18001}" --clip "${clip}" --out "${results_dir}/cache-vs-no-cache.csv"

echo "B: checkpoint recovery versus cold replay (real weights, zipformer_kv)"
printf 'mode,recovery_ms\n' >"${results_dir}/recovery.csv"
for enabled in true 0; do
  # Restrict this gateway incarnation to the two compatible KV workers.
  # This used to be the mock pair, because mock was the only serializable
  # adapter; since M11 `zipformer_kv` is serializable on REAL weights, so
  # this benchmark now measures a real checkpoint restore rather than a
  # pickled integer. Needs the kv profile:
  #   docker compose --profile kv up -d
  WORKER_URLS='worker-a=http://worker-f:9000,worker-b=http://worker-g:9000' CHECKPOINTS_ENABLED="${enabled}" GATEWAY_DASHBOARD_PORT="${dashboard_port}" docker compose up -d --force-recreate gateway >/dev/null
  wait_gateway
  log_file="${results_dir}/recovery-${enabled}.log"
  CHECKPOINTS_ENABLED="${enabled}" \
  WORKER_A_URL="${WORKER_F_URL:-http://localhost:18007}" WORKER_A_ADMIN_URL="${WORKER_F_ADMIN_URL:-http://localhost:19007}" \
  WORKER_B_URL="${WORKER_G_URL:-http://localhost:18008}" WORKER_B_ADMIN_URL="${WORKER_G_ADMIN_URL:-http://localhost:19008}" \
    go run ./cmd/chaostest --scenario 2 --clip "${clip}" 2>&1 | tee "${log_file}"
  metric="$(sed -nE 's/.*BENCH recovery_mode=([^ ]+) recovery_ms=([0-9.]+).*/\1,\2/p' "${log_file}" | tail -1)"
  [[ -n "${metric}" ]] || { echo "bench: recovery metric missing from ${log_file}" >&2; exit 1; }
  printf '%s\n' "${metric}" >>"${results_dir}/recovery.csv"
  restore_worker "${WORKER_F_ADMIN_URL:-http://localhost:19007}" "${WORKER_F_URL:-http://localhost:18007}"
  restore_worker "${WORKER_G_ADMIN_URL:-http://localhost:19008}" "${WORKER_G_URL:-http://localhost:18008}"
done

echo "C: full compatibility matrix plus cross-model recovery"
restore_default_gateway
MATRIX_WORKER_URLS="worker-a=http://localhost:${WORKER_A_PORT:-18001},worker-b=http://localhost:${WORKER_B_PORT:-18002},worker-c=http://localhost:${WORKER_C_PORT:-18003},worker-d=http://localhost:${WORKER_D_PORT:-18004},worker-e=http://localhost:${WORKER_E_PORT:-18005}" \
  go run ./cmd/matrix --out "${results_dir}/compatibility-matrix.md"
WORKER_URLS='worker-a=http://worker-a:9000,worker-b=http://worker-b:9000,worker-c=http://worker-c:9000' GATEWAY_DASHBOARD_PORT="${dashboard_port}" docker compose up -d --force-recreate gateway >/dev/null
wait_gateway
cross_log="${results_dir}/recovery-cross.log"
go run ./cmd/chaostest --scenario 3 --clip "${clip}" 2>&1 | tee "${cross_log}"
metric="$(sed -nE 's/.*BENCH recovery_mode=([^ ]+) recovery_ms=([0-9.]+).*/\1,\2/p' "${cross_log}" | tail -1)"
[[ -n "${metric}" ]] || { echo "bench: cross-model recovery metric missing" >&2; exit 1; }
printf '%s\n' "${metric}" >>"${results_dir}/recovery.csv"
restore_worker "${WORKER_A_ADMIN_URL:-http://localhost:19001}" "${WORKER_A_URL:-http://localhost:18001}"
restore_worker "${WORKER_B_ADMIN_URL:-http://localhost:19002}" "${WORKER_B_URL:-http://localhost:18002}"

echo "D: online latency alone and with offline work"
restore_default_gateway
go run ./cmd/loadgen --streams 4 --duration "${duration}" --corpus "${corpus_dir}" --ws-url "${gateway_ws}" --out "${results_dir}/online-baseline.csv" >"${results_dir}/online-baseline.summary"
go run ./cmd/loadgen --streams 3 --duration "${duration}" --mode offline --corpus "${corpus_dir}" --ws-url "${gateway_ws}" --out "${results_dir}/offline-background.csv" >"${results_dir}/offline-background.summary" &
offline_pid=$!
go run ./cmd/loadgen --streams 4 --duration "${duration}" --corpus "${corpus_dir}" --ws-url "${gateway_ws}" --out "${results_dir}/online-with-offline.csv" >"${results_dir}/online-with-offline.summary"
wait "${offline_pid}"

echo "E: rate-limit fallback evidence"
before="$(curl --fail --silent "${gateway_url}/api/debug/metrics")"
curl --fail --silent -X POST -H 'Content-Type: application/json' -d '{"rate":1.0}' "${WORKER_A_URL:-http://localhost:18001}/admin/429" >/dev/null
go run ./cmd/loadgen --streams 6 --duration "${duration}" --corpus "${corpus_dir}" --ws-url "${gateway_ws}" --out "${results_dir}/rate-limit-events.csv" >"${results_dir}/rate-limit.summary"
curl --fail --silent -X POST "${WORKER_A_URL:-http://localhost:18001}/admin/reset" >/dev/null
after="$(curl --fail --silent "${gateway_url}/api/debug/metrics")"
python3 - "${before}" "${after}" >"${results_dir}/rate-limit.csv" <<'PY'
import json, sys
before, after = map(json.loads, sys.argv[1:])
print("backend_429_delta,failover_delta")
print(f"{after['backend_429_total']-before['backend_429_total']},{after['failover_total']-before['failover_total']}")
PY

echo "F: capacity ramp to the configured admission ceiling (${capacity_ceiling})"
MAX_SESSIONS="${capacity_ceiling}" SOFT_SESSIONS="$((capacity_ceiling * 3 / 4))" GATEWAY_DASHBOARD_PORT="${dashboard_port}" docker compose up -d --force-recreate gateway >/dev/null
wait_gateway
printf 'streams,partial_p95_ms,final_p95_ms,streams_opened,streams_refused,errors\n' >"${results_dir}/capacity.csv"
for streams in 1 2 4 8 12 16 20 24; do
  summary="$(go run ./cmd/loadgen --streams "${streams}" --ramp 2s --duration "${duration}" --corpus "${corpus_dir}" --ws-url "${gateway_ws}" --out "${results_dir}/capacity-${streams}.csv")"
  printf '%s\n' "${summary}" >"${results_dir}/capacity-${streams}.summary"
  field() { printf '%s' "${summary}" | grep -o "$1=[0-9.]*" | head -1 | cut -d= -f2; }
  printf '%s,%s,%s,%s,%s,%s\n' "${streams}" "$(field partial_p95_ms)" "$(field final_p95_ms)" "$(field streams_opened)" "$(field streams_refused)" "$(field errors)" >>"${results_dir}/capacity.csv"
done

echo "Latency-over-time run with a marked worker kill"
restore_default_gateway
WORKER_URLS='worker-a=http://worker-a:9000,worker-b=http://worker-b:9000' GATEWAY_DASHBOARD_PORT="${dashboard_port}" docker compose up -d --force-recreate gateway >/dev/null
wait_gateway
go run ./cmd/loadgen --streams 4 --duration "${duration}" --corpus "${corpus_dir}" --ws-url "${gateway_ws}" --out "${results_dir}/latency-kill.csv" >"${results_dir}/latency-kill.summary" &
load_pid=$!
sleep 4
curl --fail --silent -X POST "${WORKER_A_ADMIN_URL:-http://localhost:19001}/admin/die" >/dev/null
wait "${load_pid}"
restore_worker "${WORKER_A_ADMIN_URL:-http://localhost:19001}" "${WORKER_A_URL:-http://localhost:18001}"

python3 ./scripts/render_bench_charts.py --dir "${results_dir}"
echo "bench: wrote CSVs and four SVG charts to ${results_dir}"
