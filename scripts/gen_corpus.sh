#!/usr/bin/env bash
# Generates corpus/*.wav from known text via macOS `say`, per
# docs/build-plan.md "The load generator" ("ground truth for free").
# 16kHz mono s16le, matching the locked audio-format decision
# (docs/DECISIONS.md). Run once; output is committed, not regenerated at
# container runtime (docs/build-plan.md "One command" -> "The corpus").
#
# macOS-only (uses `say`). That's fine: this script runs on the dev
# machine, never in the Docker build or at container start.
set -euo pipefail
cd "$(dirname "$0")/.."

command -v say >/dev/null 2>&1 || { echo "this script requires macOS 'say'" >&2; exit 1; }

mkdir -p corpus
transcripts=corpus/transcripts.json
echo "{" > "$transcripts"
first=true

gen() {
  local name="$1" text="$2"
  say -o "corpus/${name}.wav" --data-format=LEI16@16000 --file-format=WAVE --channels=1 "$text"
  if [ "$first" = true ]; then first=false; else echo "," >> "$transcripts"; fi
  # json.dumps-lite: our corpus text is plain ASCII, so naive quoting is safe.
  printf '  "%s.wav": "%s"' "$name" "$text" >> "$transcripts"
  echo "  corpus/${name}.wav"
}

# Short utterances (M1 smoke test: no VAD/silence handling yet, so these
# stream as one continuous utterance each). A couple of longer/paused ones
# are included now so M4's VAD work doesn't need a second corpus pass.
gen 01_short_greeting        "Hello, this is a test of the transcription system."
gen 02_number_transfer       "Transfer five thousand rupees to Ramya."
gen 03_number_transfer_alt   "Transfer five thousand rupees to Ram."
gen 04_short_command         "Start recording now."
gen 05_short_confirm         "Yes, that is correct."
gen 06_medium_sentence       "The quick brown fox jumps over the lazy dog near the river bank."
gen 07_digits                "One two three four five six seven eight nine zero."
gen 08_question               "What is the current account balance?"
gen 09_longer_monologue      "This is a longer utterance meant to exercise chunk accumulation across several inference calls, since it runs well past a single chunk boundary."
gen 10_short_negative        "No, please cancel that request."

echo "}" >> "$transcripts"
python3 -c "import json; json.load(open('$transcripts'))" # validate the JSON we hand-wrote
echo "wrote $(ls corpus/*.wav | wc -l | tr -d ' ') clips + $transcripts"
