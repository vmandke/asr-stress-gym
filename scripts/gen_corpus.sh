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

# gen <name> <spoken text>
#
# The spoken text may contain macOS `say` markup — specifically
# [[slnc <ms>]], which inserts a real silent gap. The transcript records
# what was SAID, so the markup is stripped before it is written: a clip
# whose ground truth contained "[[slnc 1200]]" would fail every assertion
# that compares a transcript against it.
#
# Existing clips are left alone unless FORCE=1. Regenerating is not free:
# `say` output is not bit-identical across runs or system voices, so a
# blanket regeneration would rewrite all ten committed WAVs (and silently
# change what every transcript assertion is comparing against) just to add
# one new clip.
gen() {
  local name="$1" spoken="$2"
  local transcript
  transcript="$(printf '%s' "$spoken" | sed -E 's/\[\[[^]]*\]\]//g; s/  +/ /g; s/^ //; s/ $//')"

  if [ -f "corpus/${name}.wav" ] && [ "${FORCE:-0}" != "1" ]; then
    echo "  corpus/${name}.wav (exists, skipped — FORCE=1 to regenerate)"
  else
    say -o "corpus/${name}.wav" --data-format=LEI16@16000 --file-format=WAVE --channels=1 "$spoken"
    echo "  corpus/${name}.wav"
  fi

  if [ "$first" = true ]; then first=false; else echo "," >> "$transcripts"; fi
  # json.dumps-lite: our corpus text is plain ASCII, so naive quoting is safe.
  printf '  "%s.wav": "%s"' "$name" "$transcript" >> "$transcripts"
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

# --- Long-form clips (M7/M8) ---
#
# Everything above is 1-3 seconds and a SINGLE utterance, which is enough
# to prove the protocol works and is not enough to measure anything about
# scale. Three specific things need length:
#
#   1. M8's "inference ms per call vs utterance length" chart needs an
#      x-axis. Clips clustered between 1.2s and 3.4s give it no range.
#   2. Replay-cost bounding (docs/build-plan.md) replays from the last
#      committed final. With one short utterance per clip, that bound is
#      never under any pressure, so the design is untested by the suite.
#   3. M4's endpointing closes an utterance at a pause. No clip above
#      contains one, so multi-utterance behaviour — per-utterance dedupe,
#      revision reset across utterances — is exercised only in unit tests
#      and never end to end.
#
# [[slnc N]] inserts a real N-millisecond silence; `say` adds its own
# sentence-boundary pause on top, so the gaps come out somewhat longer
# than requested. That is fine: the point is a gap comfortably past the
# VAD's hangover, not a precise duration.
gen 11_long_monologue \
  "Thank you for calling the account services line. I can help you with balance enquiries, recent transactions, standing instructions, and card replacements. \
Before we begin, please note that this call may be recorded for quality and training purposes. \
If at any point you would like to speak to a human representative, simply say representative and I will transfer you. \
To get started, please tell me in a few words what you are calling about today, and I will route you to the right place."

gen 12_multi_utterance \
  "Check my balance. [[slnc 1400]] Now transfer two thousand rupees to Ramya. [[slnc 1600]] No, make that three thousand instead. [[slnc 1400]] Yes, that is correct, please confirm it. [[slnc 1500]] Thank you, that is all for today."

echo "}" >> "$transcripts"
python3 -c "import json; json.load(open('$transcripts'))" # validate the JSON we hand-wrote
echo "wrote $(ls corpus/*.wav | wc -l | tr -d ' ') clips + $transcripts"
