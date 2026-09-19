#!/usr/bin/env python3
"""Generate the large synthetic corpus: varied clip KINDS, not one shape.

Why this is separate from scripts/gen_corpus.sh, and why its output is NOT
committed:

  scripts/gen_corpus.sh produces twelve short clips that ARE committed.
  They are the project's ground truth — small, stable and diffable, so a
  test can name one and mean it forever.

  This produces ~200 clips totalling roughly an hour and a half of audio
  (~200MB). That does not belong in git. It is generated on demand,
  git-ignored (corpus/large/), and reproducible from --seed, so "the same
  corpus" is a number you pass rather than a blob you ship.

WHY KINDS. A corpus of 200 identically-shaped clips is 200 copies of one
test. Each kind below exists because some specific behaviour in this
system is untestable without it:

  monologue      continuous speech, one utterance. The baseline.
  dialogue       two voices taking turns with real gaps. The only thing
                 that exercises endpointing the way a call actually does:
                 several utterances per SESSION, which is where
                 per-utterance dedupe and revision-reset live (M2) and
                 where a session-wide bug hides behind a passing
                 single-utterance test.
  long_form      60-120s. Replay-cost bounding (docs/build-plan.md) caps
                 replay at the last committed final. With 15s clips that
                 bound never binds, so the design is untested; at two
                 minutes the difference between "replay from the last
                 final" and "replay from session start" is the difference
                 between recovering and not.
  silence_heavy  multi-second gaps. The VAD economic argument at a
                 realistic ratio rather than a synthetic one, and the case
                 where pre-roll retention has to survive a journal trim.
  rapid_turns    many 1-3s utterances back to back. Utterance lifecycle
                 churn — finals, resets and utterance ids arriving faster
                 than a chunk boundary.
  noisy          speech with additive noise at a known SNR. A VAD tuned
                 only against clean TTS is tuned against nothing; this is
                 what says whether the threshold survives a noise floor.

Every clip's text is composed from templates against a seeded RNG, so the
transcript is exact ground truth ("ground truth for free",
docs/build-plan.md). The manifest additionally records each clip's
UTTERANCE BOUNDARIES — where every turn starts and ends in milliseconds —
which is what makes it possible to assert that endpointing fired in the
right places rather than merely that it fired.

Duration targeting: macOS `say` runs at roughly 2.9 words/second on the
default voice, so a word budget picks the length. The rate varies with the
words actually chosen, so every clip is MEASURED after synthesis and
regenerated with an adjusted budget if it fell outside its kind's range.
Clips still out of range after the retries are reported rather than
silently kept — a corpus that quietly contains clips outside the range it
advertises would distort exactly the benchmarks it exists to serve.

macOS only (uses `say`). Runs on a developer machine, never in a Docker
build or at container start.

Usage:
    scripts/gen_corpus_large.py                       # the default mix
    scripts/gen_corpus_large.py --count 40 --jobs 4   # a faster subset
    scripts/gen_corpus_large.py --kinds dialogue,long_form
"""

from __future__ import annotations

import argparse
import array
import json
import math
import os
import random
import shutil
import struct
import subprocess
import sys
import tempfile
import wave
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OUT_DIR = REPO / "corpus" / "large"

SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2

# Roughly `say`'s default rate. Only used to pick an initial word budget;
# the real duration is always measured, never assumed.
WORDS_PER_SECOND = 2.9

# Natural-sounding voices only. macOS also ships novelty voices (Bells,
# Boing, Bubbles, Cellos, Jester, Organ, Superstar, Wobble) which are not
# speech in any useful sense and would poison a corpus meant to stand in
# for real callers.
VOICES = ["Samantha", "Daniel", "Karen", "Kathy", "Ralph", "Rishi", "Tara", "Aman"]


# --- text ------------------------------------------------------------------

CUSTOMER_LINES = [
    "Please transfer {amount} to {name} from my {account} account.",
    "What is the current balance on my {account} account?",
    "I would like to report a lost card ending in {digits}.",
    "Can you tell me the last {count} transactions on my {account} account?",
    "I want to set up a standing instruction of {amount} every month to {name}.",
    "There is a charge of {amount} on {date} that I do not recognise.",
    "Please confirm that the payment to {name} for {amount} went through.",
    "I need to update the mobile number registered against my {account} account.",
    "Could you check whether the cheque for {amount} has cleared yet?",
    "My card was declined at {merchant} this morning and I do not know why.",
    "Please block my card ending in {digits} and issue a replacement.",
    "What is the interest rate on my {account} account at the moment?",
    "I am calling about the direct debit to {merchant} that failed on {date}.",
    "Can I increase the daily transfer limit on my {account} account to {amount}?",
    "I would like a statement for my {account} account covering the last {count} months.",
    "The transfer of {amount} to {name} is still showing as pending after {count} days.",
    "Please cancel the standing instruction to {name} with immediate effect.",
    "I want to dispute the transaction at {merchant} for {amount} on {date}.",
    "Has the salary credit for this month landed in my {account} account yet?",
    "Could you read out the registered address on my {account} account?",
]

AGENT_LINES = [
    "Certainly, let me pull that up for you now.",
    "Thank you for confirming. I can see the {account} account here.",
    "I can help with that. May I confirm the last four digits of your card?",
    "That transaction was authorised on {date} for {amount}.",
    "I have raised a dispute for the charge at {merchant}.",
    "The replacement card will reach you within {count} working days.",
    "Your available balance is {amount} as of this morning.",
    "I have cancelled the standing instruction to {name}.",
    "Let me check with our payments team and come straight back to you.",
    "That limit has now been increased. Is there anything else today?",
    "I am sorry about that. Let me see what went wrong with the payment.",
    "The statement is on its way to your registered email address.",
    "I can confirm the salary credit landed on {date}.",
    "For security, could you confirm the registered mobile number?",
    "That cheque is still in clearing and should settle within {count} days.",
]

SHORT_TURNS = [
    "Yes, that is correct.", "No, that is not right.", "Please go ahead.",
    "Could you repeat that?", "Thank you.", "One moment please.",
    "That works for me.", "I am still here.", "Sorry, say again?",
    "Yes please.", "No, cancel that.", "Understood.",
    "That is the one.", "Not that account, the other one.", "Perfect, thank you.",
]

FILLERS = [
    "Thank you for your help with this.",
    "I will hold if you need to check.",
    "Sorry, let me start that again.",
    "That is all I needed today.",
    "Please take your time.",
    "I appreciate you looking into it.",
    "Let me know if you need anything else from me.",
    "I have the reference number in front of me.",
]

NAMES = ["Ramya", "Anil", "Priya", "Vikram", "Meera", "Arjun", "Divya", "Rahul",
         "Kavita", "Suresh", "Nisha", "Farid", "Latha", "Imran", "Gita"]
AMOUNTS = ["five hundred rupees", "two thousand rupees", "three thousand rupees",
           "seven thousand five hundred rupees", "twelve thousand rupees",
           "fifty thousand rupees", "nine hundred rupees", "one lakh rupees"]
ACCOUNTS = ["savings", "current", "salary", "joint", "fixed deposit"]
DIGITS = ["one two three four", "five six seven eight", "nine zero one two",
          "three three seven one", "eight eight four two"]
COUNTS = ["two", "three", "five", "six", "ten", "twelve"]
DATES = ["the third of March", "the tenth of April", "the twenty first of June",
         "the fifth of last month", "the eighteenth of January"]
MERCHANTS = ["the fuel station on Main Street", "the online grocery service",
             "the airline booking site", "the electricity board",
             "the mobile network operator", "the insurance provider"]


def fill(template: str, rng: random.Random) -> str:
    return template.format(
        name=rng.choice(NAMES), amount=rng.choice(AMOUNTS),
        account=rng.choice(ACCOUNTS), digits=rng.choice(DIGITS),
        count=rng.choice(COUNTS), date=rng.choice(DATES),
        merchant=rng.choice(MERCHANTS),
    )


def passage(rng: random.Random, target_words: int, pool: list[str]) -> str:
    """Whole sentences until the word budget is met. Sentences are never
    truncated: half a sentence makes a poor ground truth for anything that
    compares transcripts."""
    parts: list[str] = []
    words = 0
    while words < target_words:
        s = fill(rng.choice(pool), rng) if rng.random() < 0.8 else rng.choice(FILLERS)
        parts.append(s)
        words += len(s.split())
    return " ".join(parts)


# --- kinds -----------------------------------------------------------------
#
# Each kind is (min_seconds, max_seconds, planner). The planner returns a
# list of (text, voice) turns plus the gap in seconds to insert AFTER each
# turn, given a total-duration target.

def plan_monologue(rng, target_s):
    voice = rng.choice(VOICES)
    return [(passage(rng, int(target_s * WORDS_PER_SECOND), CUSTOMER_LINES), voice, 0.0)]


def plan_dialogue(rng, target_s):
    # Two distinct voices, strictly alternating: a caller and an agent.
    a, b = rng.sample(VOICES, 2)
    turns, budget = [], target_s
    speaker = 0
    while budget > 3.0:
        gap = rng.uniform(0.6, 1.6)
        turn_s = min(rng.uniform(2.0, 6.0), max(1.5, budget - gap))
        pool = CUSTOMER_LINES if speaker == 0 else AGENT_LINES
        turns.append((passage(rng, int(turn_s * WORDS_PER_SECOND), pool),
                      a if speaker == 0 else b, gap))
        budget -= turn_s + gap
        speaker ^= 1
    if turns:  # no trailing silence: the last turn ends the clip
        t, v, _ = turns[-1]
        turns[-1] = (t, v, 0.0)
    return turns


def plan_long_form(rng, target_s):
    # One speaker, paragraph-length stretches with short breathing gaps.
    voice = rng.choice(VOICES)
    turns, budget = [], target_s
    while budget > 8.0:
        gap = rng.uniform(0.4, 1.0)
        turn_s = min(rng.uniform(12.0, 20.0), max(6.0, budget - gap))
        turns.append((passage(rng, int(turn_s * WORDS_PER_SECOND), CUSTOMER_LINES), voice, gap))
        budget -= turn_s + gap
    if turns:
        t, v, _ = turns[-1]
        turns[-1] = (t, v, 0.0)
    return turns


def plan_silence_heavy(rng, target_s):
    # Short bursts of speech separated by multi-second holds. Speech is a
    # small fraction of the clip, which is the point.
    voice = rng.choice(VOICES)
    turns, budget = [], target_s
    while budget > 6.0:
        gap = rng.uniform(4.0, 10.0)
        turn_s = min(rng.uniform(2.0, 5.0), max(1.5, budget - gap))
        turns.append((passage(rng, int(turn_s * WORDS_PER_SECOND), SHORT_TURNS), voice, gap))
        budget -= turn_s + gap
    if turns:
        t, v, _ = turns[-1]
        turns[-1] = (t, v, 0.0)
    return turns


def plan_rapid_turns(rng, target_s):
    # Many very short utterances with small gaps: utterance churn.
    a, b = rng.sample(VOICES, 2)
    turns, budget, speaker = [], target_s, 0
    while budget > 1.5:
        gap = rng.uniform(0.35, 0.75)
        turns.append((rng.choice(SHORT_TURNS), a if speaker == 0 else b, gap))
        budget -= 1.6 + gap
        speaker ^= 1
    if turns:
        t, v, _ = turns[-1]
        turns[-1] = (t, v, 0.0)
    return turns


def plan_noisy(rng, target_s):
    return plan_monologue(rng, target_s)


KINDS = {
    #  kind:            (min_s, max_s, planner,          noise_snr_db)
    "monologue":        (10.0, 20.0, plan_monologue, None),
    "dialogue":         (25.0, 50.0, plan_dialogue, None),
    "long_form":        (60.0, 120.0, plan_long_form, None),
    "silence_heavy":    (30.0, 60.0, plan_silence_heavy, None),
    "rapid_turns":      (20.0, 40.0, plan_rapid_turns, None),
    # 15dB SNR: clearly audible noise that a human hears through without
    # effort. Not a torture test — the question is whether the VAD's
    # threshold survives an ordinary noise floor, not whether it survives
    # a hostile one.
    "noisy":            (10.0, 20.0, plan_noisy, 15.0),
}

# The default mix. Weighted towards monologue because it is the baseline
# most runs want, with enough of each other kind to make a per-kind
# measurement meaningful rather than anecdotal.
DEFAULT_MIX = {
    "monologue": 0.40,
    "dialogue": 0.20,
    "long_form": 0.10,
    "silence_heavy": 0.10,
    "rapid_turns": 0.10,
    "noisy": 0.10,
}


# --- audio -----------------------------------------------------------------

def say_to_pcm(text: str, voice: str, tmpdir: str) -> bytes:
    path = Path(tmpdir) / "turn.wav"
    subprocess.run(
        ["say", "-v", voice, "-o", str(path), "--data-format=LEI16@16000",
         "--file-format=WAVE", "--channels=1", text],
        check=True, capture_output=True,
    )
    with wave.open(str(path)) as w:
        if (w.getframerate(), w.getnchannels(), w.getsampwidth()) != (SAMPLE_RATE, 1, 2):
            raise RuntimeError("say produced an unexpected format")
        return w.readframes(w.getnframes())


def silence(seconds: float) -> bytes:
    return b"\x00\x00" * int(SAMPLE_RATE * seconds)


def add_noise(pcm: bytes, snr_db: float, rng: random.Random) -> bytes:
    """Additive white Gaussian noise at a target SNR, computed against the
    clip's own RMS so the SNR means the same thing on a loud clip and a
    quiet one."""
    samples = array.array("h")
    samples.frombytes(pcm)
    if not samples:
        return pcm
    rms = math.sqrt(sum(float(s) * s for s in samples) / len(samples))
    if rms <= 0:
        return pcm
    noise_rms = rms / (10 ** (snr_db / 20.0))
    out = array.array("h")
    for s in samples:
        v = int(s + rng.gauss(0.0, noise_rms))
        # Clamp rather than let it wrap: a wrapped sample is a loud click,
        # which would be a far bigger VAD event than the noise being added.
        out.append(-32768 if v < -32768 else (32767 if v > 32767 else v))
    return out.tobytes()


def write_wav(path: Path, pcm: bytes) -> None:
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(BYTES_PER_SAMPLE)
        w.setframerate(SAMPLE_RATE)
        w.writeframes(pcm)


def render(turns, snr_db, rng, tmpdir):
    """Synthesize each turn and concatenate with its trailing gap.

    Turn-by-turn rather than one `say` call with [[slnc]] markup, for two
    reasons: a gap between two DIFFERENT voices cannot be expressed in one
    call at all, and rendering separately is what yields exact utterance
    boundaries — the manifest can then say where every utterance starts and
    ends, so a test can assert endpointing fired in the right PLACES, not
    merely that it fired.
    """
    pcm = bytearray()
    boundaries = []
    for text, voice, gap in turns:
        start_ms = int(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
        turn_pcm = say_to_pcm(text, voice, tmpdir)
        pcm.extend(turn_pcm)
        end_ms = int(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
        boundaries.append({"start_ms": start_ms, "end_ms": end_ms,
                           "text": text, "voice": voice})
        if gap > 0:
            pcm.extend(silence(gap))
    out = bytes(pcm)
    if snr_db is not None:
        out = add_noise(out, snr_db, rng)
    return out, boundaries


def speech_ratio(boundaries, total_ms) -> float:
    if total_ms <= 0:
        return 0.0
    voiced = sum(b["end_ms"] - b["start_ms"] for b in boundaries)
    return round(voiced / total_ms, 4)


def make_one(args) -> dict:
    idx, kind, seed, max_tries = args
    lo, hi, planner, snr_db = KINDS[kind]
    rng = random.Random(seed * 1_000_003 + idx)

    out_dir = OUT_DIR / kind
    path = out_dir / f"{kind}_{idx:04d}.wav"

    target_s = rng.uniform(lo + (hi - lo) * 0.15, hi - (hi - lo) * 0.15)
    pcm, boundaries, duration = b"", [], 0.0

    with tempfile.TemporaryDirectory() as tmpdir:
        for _ in range(max_tries):
            turns = planner(rng, target_s)
            if not turns:
                turns = plan_monologue(rng, target_s)
            pcm, boundaries = render(turns, snr_db, rng, tmpdir)
            duration = len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE
            if lo <= duration <= hi:
                break
            # Rescale toward the target. Turns are whole, so this
            # converges rather than landing exactly.
            target_s = max(lo, min(hi, target_s * (target_s / duration)))

    out_dir.mkdir(parents=True, exist_ok=True)
    write_wav(path, pcm)
    total_ms = int(duration * 1000)
    return {
        "file": f"{kind}/{path.name}",
        "kind": kind,
        "duration_s": round(duration, 3),
        "in_range": lo <= duration <= hi,
        "utterances": len(boundaries),
        "speech_ratio": speech_ratio(boundaries, total_ms),
        "noise_snr_db": snr_db,
        "boundaries": boundaries,
        "text": " ".join(b["text"] for b in boundaries),
    }


# --- driver ----------------------------------------------------------------

def resolve_mix(count: int, kinds: list[str]) -> list[str]:
    """Expand the mix into a concrete per-clip kind list. Every requested
    kind gets at least one clip even at small --count, so a quick run still
    covers every shape rather than silently degenerating to monologues."""
    weights = {k: DEFAULT_MIX.get(k, 1.0 / len(kinds)) for k in kinds}
    total_w = sum(weights.values())
    plan: list[str] = []
    for k in kinds:
        n = max(1, round(count * weights[k] / total_w))
        plan.extend([k] * n)
    return plan[:count] if len(plan) > count else plan


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--count", type=int, default=200,
                    help="total clips across all kinds (default 200)")
    ap.add_argument("--kinds", default=",".join(KINDS),
                    help=f"comma-separated subset of: {', '.join(KINDS)}")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 4) // 2))
    ap.add_argument("--max-tries", type=int, default=3)
    ap.add_argument("--clean", action="store_true", help="remove any existing corpus/large first")
    args = ap.parse_args()

    if not shutil.which("say"):
        sys.exit("this script requires macOS `say` (same constraint as scripts/gen_corpus.sh)")

    kinds = [k.strip() for k in args.kinds.split(",") if k.strip()]
    unknown = [k for k in kinds if k not in KINDS]
    if unknown:
        sys.exit(f"unknown kind(s): {', '.join(unknown)} — known: {', '.join(KINDS)}")

    if args.clean and OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    plan = resolve_mix(args.count, kinds)
    from collections import Counter
    print(f"generating {len(plan)} clips into {OUT_DIR.relative_to(REPO)} "
          f"(git-ignored) with {args.jobs} jobs", file=sys.stderr)
    for k, n in sorted(Counter(plan).items()):
        lo, hi, _, snr = KINDS[k]
        note = f", {snr:g}dB SNR" if snr is not None else ""
        print(f"  {k:<14} {n:>4} clips  {lo:g}-{hi:g}s{note}", file=sys.stderr)

    work = [(i, kind, args.seed, args.max_tries) for i, kind in enumerate(plan, start=1)]
    results = []
    with ProcessPoolExecutor(max_workers=args.jobs) as pool:
        for n, r in enumerate(pool.map(make_one, work), start=1):
            results.append(r)
            if n % 20 == 0 or n == len(work):
                print(f"  {n}/{len(work)}", file=sys.stderr)

    results.sort(key=lambda r: r["file"])
    (OUT_DIR / "transcripts.json").write_text(
        json.dumps({r["file"]: r["text"] for r in results}, indent=2) + "\n")

    manifest = {
        "seed": args.seed,
        "count": len(results),
        "kinds": {k: {"min_seconds": KINDS[k][0], "max_seconds": KINDS[k][1],
                      "noise_snr_db": KINDS[k][3]} for k in kinds},
        "clips": [{k: r[k] for k in
                   ("file", "kind", "duration_s", "in_range", "utterances",
                    "speech_ratio", "noise_snr_db", "boundaries")} for r in results],
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    total = sum(r["duration_s"] for r in results)
    size_mb = sum(f.stat().st_size for f in OUT_DIR.rglob("*.wav")) / 1e6
    print(f"\nwrote {len(results)} clips, {total/60:.1f} minutes, {size_mb:.0f}MB", file=sys.stderr)
    print(f"{'kind':<15}{'n':>4}{'p50 s':>9}{'utts':>7}{'speech':>9}{'out of range':>14}", file=sys.stderr)
    for k in kinds:
        rs = [r for r in results if r["kind"] == k]
        if not rs:
            continue
        ds = sorted(r["duration_s"] for r in rs)
        utt = sum(r["utterances"] for r in rs) / len(rs)
        sr = sum(r["speech_ratio"] for r in rs) / len(rs)
        bad = sum(1 for r in rs if not r["in_range"])
        print(f"{k:<15}{len(rs):>4}{ds[len(ds)//2]:>9.1f}{utt:>7.1f}{sr:>9.2f}{bad:>14}", file=sys.stderr)


if __name__ == "__main__":
    main()
