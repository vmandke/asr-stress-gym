"""Decoding the opaque payload an adapter is handed.

This is the worker-side mirror of the Go gateway's `internal/audio`
boundary: above this line nothing knows a sample rate, a sample format, or
a byte-per-sample count. `Adapter.infer` receives opaque bytes
(adapters/base.py) and whichever adapter needs floats asks here rather
than open-coding a struct unpack of its own — four adapters open-coding
the same `int16 / 32768.0` is four places for the same off-by-one to hide.

The format itself is fixed for the whole system and justified in
docs/FAQ.md ("Why mono 16kHz s16le PCM for the wire audio format?"); it is
declared in session.start and validated, never guessed.
"""

from __future__ import annotations

import numpy as np

SAMPLE_RATE_HZ = 16000
BYTES_PER_SAMPLE = 2  # s16le, mono


def to_float32(audio: bytes) -> np.ndarray:
    """s16le PCM bytes -> float32 samples in [-1, 1), which is what every
    ASR runtime in this fleet expects.

    A trailing odd byte is dropped rather than raising: by the time a
    payload reaches an adapter, the gateway's wire.ValidateAudioFrame has
    already rejected any frame whose declared duration disagrees with its
    length, so an odd length here would mean a bug upstream — and failing
    a whole session over one orphan byte helps nobody. The gateway is
    where that invariant is enforced; this is just defensive.
    """
    usable = len(audio) - (len(audio) % BYTES_PER_SAMPLE)
    if usable <= 0:
        return np.zeros(0, dtype=np.float32)
    samples = np.frombuffer(audio, dtype="<i2", count=usable // BYTES_PER_SAMPLE)
    return (samples.astype(np.float32) / 32768.0).copy()


def duration_s(audio: bytes) -> float:
    """Wall-clock seconds of audio in an opaque payload — the denominator
    of RTF (server.py's /health rtf_p50)."""
    return (len(audio) // BYTES_PER_SAMPLE) / SAMPLE_RATE_HZ
