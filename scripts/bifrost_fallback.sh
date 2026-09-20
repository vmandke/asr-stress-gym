#!/usr/bin/env bash
# Prove that Bifrost retries a COMPLETE transcription on its fallback provider.
#
# It kills worker-a's child process, submits a one-shot WAV to Bifrost naming
# worker-a as primary and worker-b as fallback, then verifies that worker-b
# produced the result. It never sends a stateful streaming Push through
# Bifrost; see docs/BIFROST-KVCACHE.md.
set -euo pipefail

source ./scripts/check_env.sh

clip_path="${1:-corpus/06_medium_sentence.wav}"
bifrost_port="${BIFROST_PORT:-8080}"
bifrost_url="http://localhost:${bifrost_port}"
primary_admin="${WORKER_A_ADMIN_URL:-http://localhost:19001}"
primary_url="${WORKER_A_URL:-http://localhost:18001}"

if [[ ! -f "${clip_path}" ]]; then
  echo "bifrost_fallback: WAV not found: ${clip_path}" >&2
  echo "usage: $0 [corpus-clip.wav]" >&2
  exit 2
fi

restore_primary() {
  # The supervisor survives /admin/die. Restoring is best-effort in the trap
  # so an interrupted demo does not leave the normal fleet broken.
  curl --fail --silent --show-error -X POST "${primary_admin}/admin/restore" >/dev/null || true
  for _ in $(seq 1 30); do
    if curl --fail --silent "${primary_url}/health" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "bifrost_fallback: worker-a did not return after the demo" >&2
  return 1
}
trap restore_primary EXIT

BIFROST_URL="http://bifrost:8080" docker compose --profile bifrost up -d --build

for _ in $(seq 1 30); do
  if curl --fail --silent "${bifrost_url}/metrics" >/dev/null; then
    break
  fi
  sleep 1
done
if ! curl --fail --silent "${bifrost_url}/metrics" >/dev/null; then
  echo "bifrost_fallback: Bifrost did not become ready at ${bifrost_url}" >&2
  exit 1
fi

echo "Killing Bifrost primary worker-a; worker-b is the ordered fallback."
curl --fail --silent --show-error -X POST "${primary_admin}/admin/die" >/dev/null

response="$(curl --fail --silent --show-error \
  --form "file=@${clip_path};type=audio/wav" \
  --form "model=worker-a/whisper-1" \
  --form "fallbacks=worker-b/whisper-1" \
  "${bifrost_url}/v1/audio/transcriptions")"

echo "Bifrost fallback response: ${response}"
if [[ "${response}" != *'"text"'* ]]; then
  echo "bifrost_fallback: response has no transcript text" >&2
  exit 1
fi
# extra_fields.provider is Bifrost's response evidence for the provider that
# actually served the request. This assertion prevents a 200 from worker-a
# before its child exited from masquerading as a fallback demonstration.
if ! grep -Eq '"provider"[[:space:]]*:[[:space:]]*"worker-b"' <<<"${response}"; then
  echo "bifrost_fallback: expected worker-b to serve the fallback request" >&2
  exit 1
fi

echo "PASS: Bifrost retried the stateless request through worker-b."
