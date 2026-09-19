#!/usr/bin/env bash
# Fetch the model weights the real adapters (M5) load.
#
# Runs at IMAGE BUILD TIME ONLY (worker/Dockerfile) — never when a
# container starts. docs/build-plan.md's "one command" promise is that
# `docker compose up` brings the stack up hermetically; a worker that
# downloaded 300MB from Hugging Face on boot would break that, would make
# the recovery demo depend on someone else's CDN, and would make the
# startup-latency numbers meaningless.
#
# Every artifact below is int8 where an int8 export exists: the fleet is
# CPU-only by design (docs/build-plan.md "Platform"), and dtype is one of
# the seven fields of the compatibility key (worker/adapters/base.py), so
# it is a fleet identity decision, not just a size one.
#
# Idempotent: a model directory carrying a .complete marker is skipped, so
# re-running costs nothing and a half-finished download is never mistaken
# for a good one.
#
# Plain `case` rather than an associative array: this script runs both in
# the worker image (bash 5) and on a developer's macOS host, where
# /bin/bash is still 3.2 and `declare -A` is a syntax error.
#
# Usage:
#   models/fetch.sh                       # all four
#   models/fetch.sh zipformer-en-20M ...  # just the named ones
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="${MODELS_DIR:-$HERE/download}"
HF="${HF_ENDPOINT:-https://huggingface.co}"

ALL="zipformer-en-20M conformer-ctc-en whisper-tiny.en-ct2 whisper-tiny.en-onnx"

# repo_for / files_for: the file list is deliberately explicit rather than
# a whole-repo clone — these repos also carry fp32 exports and test wavs
# that have no business in the image.
repo_for() {
  case "$1" in
    zipformer-en-20M)     echo "csukuangfj/sherpa-onnx-streaming-zipformer-en-20M-2023-02-17" ;;
    conformer-ctc-en)     echo "csukuangfj/sherpa-onnx-nemo-streaming-fast-conformer-ctc-en-480ms-int8" ;;
    whisper-tiny.en-ct2)  echo "Systran/faster-whisper-tiny.en" ;;
    whisper-tiny.en-onnx) echo "csukuangfj/sherpa-onnx-whisper-tiny.en" ;;
    *)                    echo "" ;;
  esac
}

files_for() {
  case "$1" in
    # The decoder stays fp32 on purpose: it is a 2MB embedding table whose
    # int8 export sherpa-onnx's own examples do not use. Encoder and
    # joiner, the parts that actually cost CPU, are int8.
    zipformer-en-20M)     echo "encoder-epoch-99-avg-1.int8.onnx decoder-epoch-99-avg-1.onnx joiner-epoch-99-avg-1.int8.onnx tokens.txt" ;;
    conformer-ctc-en)     echo "model.int8.onnx tokens.txt" ;;
    whisper-tiny.en-ct2)  echo "config.json model.bin tokenizer.json vocabulary.txt" ;;
    whisper-tiny.en-onnx) echo "tiny.en-encoder.int8.onnx tiny.en-decoder.int8.onnx tiny.en-tokens.txt" ;;
  esac
}

wanted="$*"
[ -n "$wanted" ] || wanted="$ALL"

for slug in $wanted; do
  repo="$(repo_for "$slug")"
  if [ -z "$repo" ]; then
    echo "fetch.sh: unknown model '$slug'; known: $ALL" >&2
    exit 2
  fi
  dir="$DEST/$slug"
  if [ -f "$dir/.complete" ]; then
    echo "fetch.sh: $slug already present, skipping"
    continue
  fi
  echo "fetch.sh: fetching $slug from $repo"
  mkdir -p "$dir"
  for f in $(files_for "$slug"); do
    # --fail so an HTML 404 page is never written out as if it were a model
    # file; the adapter would otherwise fail much later with an opaque
    # onnxruntime parse error instead of here, at fetch time.
    curl --fail --location --silent --show-error \
      --output "$dir/$(basename "$f")" \
      "$HF/$repo/resolve/main/$f"
  done
  echo "$repo" > "$dir/.complete"
  echo "fetch.sh: $slug done ($(du -sh "$dir" | cut -f1))"
done
