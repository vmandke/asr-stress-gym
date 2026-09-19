#!/usr/bin/env bash
# M7's headline measurement: what does gating silence actually save?
#
# docs/implementation-plan.md words the bar as "--speech-ratio 1.0 vs 0.5
# shows backend call volume halving". Measured, it does not *exactly*
# halve, and the gap is the interesting part — so this script reports the
# real numbers and the two reference lines that bound them, rather than
# asserting a round figure:
#
#   ungated  = audio_seconds / chunk_seconds     (every chunk dispatched)
#   perfect  = ungated * speech_ratio            (only speech dispatched)
#   actual   = what the gateway really dispatched
#
# `actual` sits above `perfect` because a VAD with pre-roll and hangover
# deliberately dispatches a little silence on either side of speech —
# clipping a word's onset to save a backend call is a bad trade
# (docs/build-plan.md). The recovered fraction below is how much of the
# theoretically available saving the VAD actually captured.
#
# Requires a running stack: make up.
set -euo pipefail
cd "$(dirname "$0")/.."

source ./scripts/check_env.sh

dashboard_port="${GATEWAY_DASHBOARD_PORT:-7000}"
gateway_url="${GATEWAY_DEBUG_URL:-http://localhost:${dashboard_port}}"
export GATEWAY_WS_URL="${GATEWAY_WS_URL:-ws://localhost:${GATEWAY_WS_PORT:-7070}/ws}"

STREAMS="${STREAMS:-4}"
DURATION="${DURATION:-12s}"
# Must match cmd/gateway/conn.go's onlineChunkMs. The gateway decides
# chunking, not the client, so this is read from there — a mismatch would
# silently scale the "ungated" reference line.
CHUNK_MS="${CHUNK_MS:-160}"

pushes() {
  curl --fail --silent "${gateway_url}/api/debug/metrics" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['backend_pushes_total'])"
}

printf '%-8s %10s %10s %10s %10s\n' ratio audio_s ungated perfect actual
for ratio in 1.0 0.75 0.5 0.25; do
  before="$(pushes)"
  out="$(go run ./cmd/loadgen --streams "$STREAMS" --duration "$DURATION" --speech-ratio "$ratio" 2>&1)"
  after="$(pushes)"

  actual=$((after - before))
  audio="$(printf '%s' "$out" | grep -o 'audio_seconds_sent=[0-9.]*' | cut -d= -f2)"
  python3 - "$ratio" "$audio" "$CHUNK_MS" "$actual" <<'PY'
import sys
ratio, audio, chunk_ms, actual = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4])
ungated = audio / (chunk_ms / 1000)
perfect = ungated * ratio
print(f"{ratio:<8} {audio:10.1f} {ungated:10.0f} {perfect:10.0f} {actual:10d}")
if ratio < 1.0:
    available = ungated - perfect
    saved = ungated - actual
    print(f"{'':8} {'':10} {'':10} {'':10} recovered {saved/available*100:.0f}% of the available saving")
PY
done
