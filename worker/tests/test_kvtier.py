"""The shared-KV-tier client, and the aliasing rule its correctness rests on.

Every real adapter mutates inference state IN PLACE and returns the same
object (adapters/zipformer_kv.py: `_consume` writes `st.bank.pending` and
`frames_consumed`, absorbs new tensors, extends the hypothesis). The hot
cache holds live state objects, so "hand out a cached entry and let infer
run" silently turns that entry into the state AFTER the chunk while it is
still filed under the version BEFORE it.

That is not a stale-cache nuisance. A retry of the chunk landing back on
this worker would read its own corrupted entry and apply the chunk twice,
while the same retry routed to a peer would fetch the correct serialized
version from the tier and be right — making correctness depend on
placement, which is the exact property the shared tier exists to remove.
"""

from __future__ import annotations

import pytest

from kvtier import KVTier, TierMiss


class FakeState:
    """Stands in for a real adapter state: identified by a counter."""

    def __init__(self, n: int) -> None:
        self.n = n


class MutatingAdapter:
    """Mirrors every real adapter: infer mutates in place, returns the same
    object. A test adapter that returned a fresh object would not be able
    to reproduce the bug this file exists to prevent."""

    def deserialize(self, blob: bytes) -> FakeState:
        return FakeState(int(blob))

    def serialize(self, st: FakeState) -> bytes:
        return str(st.n).encode()

    def infer(self, audio: bytes, st: FakeState):
        st.n += 1
        return None, st


@pytest.fixture
def tier():
    """A KVTier whose remote half is an in-memory dict, so these tests are
    about the hot-cache contract and nothing else."""
    t = KVTier(base_url="http://fake", hot_max=8)
    remote: dict[str, tuple[bytes, str]] = {}

    def fetch_blob(ref: str):
        if ref not in remote:
            raise TierMiss(ref)
        return remote[ref]

    def put_blob(ref: str, blob: bytes, compat: str):
        if ref in remote:          # create-only, like the real tier
            t.stats["conflicts"] += 1
            return
        remote[ref] = (blob, compat)

    t.fetch_blob = fetch_blob      # type: ignore[assignment]
    t.put_blob = put_blob          # type: ignore[assignment]
    t._remote = remote             # type: ignore[attr-defined]
    return t


KEY = "sha256:fam"


def test_loading_a_version_removes_it_from_the_hot_cache(tier):
    a = MutatingAdapter()
    tier.store("s:100", FakeState(1), a, KEY)
    assert "s:100" in tier._hot

    st, hit = tier.load("s:100", a, KEY)
    assert hit == "local"
    assert "s:100" not in tier._hot, (
        "the entry was handed to inference but left in the cache; the next "
        "reader of s:100 would get whatever infer mutated it into"
    )


def test_a_mutated_state_never_masquerades_as_its_predecessor(tier):
    """The bug, end to end: load 100, mutate it into 200, then ask for 100
    again. The answer must be the state as of 100, not the live object."""
    a = MutatingAdapter()
    tier.store("s:100", FakeState(1), a, KEY)

    st, _ = tier.load("s:100", a, KEY)
    _, st = a.infer(b"audio", st)          # mutates in place: n 1 -> 2
    tier.store("s:200", st, a, KEY)

    again, hit = tier.load("s:100", a, KEY)
    assert again.n == 1, (
        f"re-reading version 100 gave state n={again.n}; the chunk would be "
        "applied twice on a retry that returned to this worker"
    )
    assert hit == "tier", "it must come from the tier's immutable bytes, not the hot cache"


def test_the_successor_is_hot_so_the_next_chunk_stays_local(tier):
    """Taking the predecessor must not cost the locality the cache exists
    for: the version just written is the one the next chunk asks for."""
    a = MutatingAdapter()
    tier.store("s:100", FakeState(1), a, KEY)
    st, _ = tier.load("s:100", a, KEY)
    _, st = a.infer(b"audio", st)
    tier.store("s:200", st, a, KEY)

    _, hit = tier.load("s:200", a, KEY)
    assert hit == "local"
    assert tier.stats["local_hits"] == 2


def test_a_tier_fetch_is_not_filed_under_the_version_it_is_about_to_leave(tier):
    """Same aliasing, the other branch: a freshly deserialized object is
    about to be mutated, so caching it under `ref` would reintroduce the
    bug for any worker that fetched rather than hit locally."""
    a = MutatingAdapter()
    tier._remote["s:100"] = (b"1", KEY)

    st, hit = tier.load("s:100", a, KEY)
    assert hit == "tier"
    assert "s:100" not in tier._hot


def test_a_foreign_compatibility_key_is_refused_before_deserializing(tier):
    a = MutatingAdapter()
    tier._remote["s:100"] = (b"1", "sha256:other-family")
    with pytest.raises(ValueError, match="compatibility key mismatch"):
        tier.load("s:100", a, KEY)


def test_a_missing_reference_raises_rather_than_starting_fresh(tier):
    a = MutatingAdapter()
    with pytest.raises(TierMiss):
        tier.load("s:nope", a, KEY)


def test_rewriting_a_version_is_refused_by_the_tier(tier):
    """Create-only: two retries of one chunk name the same sink, and the
    second must not overwrite the first."""
    a = MutatingAdapter()
    tier.store("s:100", FakeState(1), a, KEY)
    tier.store("s:100", FakeState(99), a, KEY)
    assert tier._remote["s:100"][0] == b"1"
    assert tier.stats["conflicts"] == 1


def test_the_hot_cache_is_bounded(tier):
    a = MutatingAdapter()
    for i in range(20):
        tier.store(f"s:{i}", FakeState(i), a, KEY)
    assert len(tier._hot) <= 8
