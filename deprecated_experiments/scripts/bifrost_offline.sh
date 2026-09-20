#!/usr/bin/env bash
# Send one complete WAV through Bifrost's stateless transcription path.
#
# This is deliberately NOT a streaming example: Bifrost receives one complete
# audio file and may retry/fall back to another provider. The gateway's live
# Push(handle, generation, chunk) path remains direct to its pinned worker.
# See docs/BIFROST-KVCACHE.md.
set -euo pipefail

source ./scripts/check_env.sh

clip_path="${1:-corpus/06_medium_sentence.wav}"
bifrost_port="${BIFROST_PORT:-8080}"
bifrost_url="http://localhost:${bifrost_port}"
model="${BIFROST_DEMO_MODEL:-worker-a/whisper-1}"

if [[ ! -f "${clip_path}" ]]; then
  echo "bifrost_offline: WAV not found: ${clip_path}" >&2
  echo "usage: $0 [corpus-clip.wav]" >&2
  exit 2
fi

# BIFROST_URL is consumed by the gateway container; this starts the Bifrost
# profile and recreates the gateway with offline-final routing enabled too.
BIFROST_URL="http://bifrost:8080" docker compose --profile bifrost up -d --build

for _ in $(seq 1 30); do
  if curl --fail --silent "${bifrost_url}/metrics" >/dev/null; then
    break
  fi
  sleep 1
done
if ! curl --fail --silent "${bifrost_url}/metrics" >/dev/null; then
  echo "bifrost_offline: Bifrost did not become ready at ${bifrost_url}" >&2
  exit 1
fi

echo "Bifrost offline transcription: ${clip_path} -> ${model}"
curl --fail --silent --show-error \
  --form "file=@${clip_path};type=audio/wav" \
  --form "model=${model}" \
  "${bifrost_url}/v1/audio/transcriptions"
echo
