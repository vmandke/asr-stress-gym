#!/usr/bin/env bash
# Chaos scenario 9 — overload. build-plan.md demo 9: "ramp to 150%
# capacity -> `overloaded` returned, admitted sessions unaffected."
#
# A shell script rather than a cmd/chaostest scenario because it needs the
# gateway restarted with a lowered admission envelope. Ramping to 150% of
# the production default (200 sessions) would mean 300 real streams on a
# laptop to demonstrate a property that holds at any scale; lowering the
# ceiling proves the same thing in seconds.
#
# The two assertions are the two halves of build-plan.md's degradation
# order, and the ORDER is the point:
#
#   step 4 — reject NEW sessions with `overloaded` (retryable)
#   step 5 — never drop an already-admitted session
#
# So: refusals must happen (or the ceiling is not real), AND admitted
# sessions must come through clean (or the system is shedding the wrong
# thing). Passing only the first half is a system that refuses everyone;
# passing only the second is a system with no backpressure at all.
set -euo pipefail
cd "$(dirname "$0")/../.."

source ./scripts/check_env.sh

CEILING="${CEILING:-20}"
SOFT="${SOFT:-15}"
RAMP_TO=$(( CEILING * 3 / 2 ))   # 150% of capacity, per the demo

dashboard_port="${GATEWAY_DASHBOARD_PORT:-7000}"
gateway_url="${GATEWAY_DEBUG_URL:-http://localhost:${dashboard_port}}"
export GATEWAY_WS_URL="${GATEWAY_WS_URL:-ws://localhost:${GATEWAY_WS_PORT:-7070}/ws}"

echo "--- recreating gateway with MAX_SESSIONS=${CEILING} SOFT_SESSIONS=${SOFT} ---"
MAX_SESSIONS="$CEILING" SOFT_SESSIONS="$SOFT" GATEWAY_DASHBOARD_PORT="$dashboard_port" \
  docker compose up -d --force-recreate gateway

restore_default_gateway() {
  echo "--- restoring the gateway's default admission envelope ---"
  GATEWAY_DASHBOARD_PORT="$dashboard_port" docker compose up -d --force-recreate gateway >/dev/null
}
trap restore_default_gateway EXIT

for _ in $(seq 1 30); do
  curl --fail --silent "${gateway_url}/health" >/dev/null && break
  sleep 1
done

metric() {
  curl --fail --silent "${gateway_url}/api/debug/metrics" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"
}

rejected_before="$(metric admission_rejected_total)"

echo "--- ramping to ${RAMP_TO} streams against a ceiling of ${CEILING} ---"
summary="$(go run ./cmd/loadgen --streams "$RAMP_TO" --ramp 3s --duration 12s 2>&1)"
echo "$summary"

rejected_after="$(metric admission_rejected_total)"

# Parse loadgen's key=value summary (docs/BENCH.md).
field() { printf '%s' "$summary" | grep -o "$1=[0-9.]*" | head -1 | cut -d= -f2; }
opened="$(field streams_opened)"
refused="$(field streams_refused)"
errors="$(field errors)"
finals="$(field finals)"
dupes="$(field duplicate_finals)"

fail() { echo "FAIL scenario 9: $*" >&2; exit 1; }

# Step 4: backpressure actually happened.
[ "$refused" -gt 0 ] || fail "no session was refused while ramping to ${RAMP_TO} against a ceiling of ${CEILING} — the ceiling is not being enforced"
[ "$rejected_after" -gt "$rejected_before" ] || fail "admission_rejected_total did not move (${rejected_before} -> ${rejected_after})"

# The ceiling is a HARD limit, not a suggestion — this is what the
# compare-and-swap in internal/admission exists for. A read-then-increment
# would let an unbounded number of concurrent opens all observe the same
# under-limit value and sail past it, which shows up here and nowhere else.
[ "$opened" -le "$CEILING" ] || fail "${opened} sessions were admitted against a ceiling of ${CEILING} — the limit is advisory, not hard"

# Step 5: the sessions that WERE admitted are unharmed. This is the half
# that matters — refusing new callers is only the right trade if it
# actually protects the ones already talking.
[ "$errors" -eq 0 ] || fail "${errors} admitted session(s) errored — step 5 says never drop an already-admitted session"
[ "$dupes" -eq 0 ] || fail "duplicate_finals=${dupes}, must stay zero"
# At least one final per admitted session. NOT exactly one: M4's
# endpointing closes an utterance at every detected pause, and the corpus
# clips are now 10-20s with internal pauses, so one session legitimately
# produces several finals. Asserting equality here failed against a
# perfectly healthy run (20 admitted, 67 finals) — the assertion was
# wrong, not the system.
[ "$finals" -ge "$opened" ] || fail "${opened} sessions admitted but only ${finals} finals — admitted sessions were not served to completion"

echo "PASS scenario 9: ${opened}/${RAMP_TO} admitted (ceiling ${CEILING}), ${refused} refused with overloaded, ${errors} errors, ${finals} finals"
