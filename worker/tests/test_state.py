import threading

import pytest

from state import HandleNotFound, StaleGeneration, StateStore


def test_open_creates_generation_zero():
    store = StateStore()
    rec = store.open("s1", model_state={"x": 1})
    assert rec.generation == 0
    assert rec.last_seq_applied == 0
    assert store.get(rec.handle) is rec


def test_compare_and_commit_success_increments_generation():
    store = StateStore()
    rec = store.open("s1", model_state=None)
    updated = store.compare_and_commit(
        rec.handle, expected_generation=0, last_seq_applied=10, model_state="next", last_text="hi"
    )
    assert updated.generation == 1
    assert updated.last_seq_applied == 10
    assert updated.last_text == "hi"


def test_compare_and_commit_rejects_stale_generation():
    store = StateStore()
    rec = store.open("s1", model_state=None)
    store.compare_and_commit(rec.handle, expected_generation=0, last_seq_applied=10, model_state="a", last_text="a")
    # Caller still thinks generation is 0 (e.g. it's replaying after a
    # failover, unaware another write already landed) — must be rejected,
    # not silently applied over newer state (invariant 5).
    with pytest.raises(StaleGeneration):
        store.compare_and_commit(rec.handle, expected_generation=0, last_seq_applied=20, model_state="b", last_text="b")


def test_get_unknown_handle_raises():
    store = StateStore()
    with pytest.raises(HandleNotFound):
        store.get("no-such-handle")


def test_close_removes_handle():
    store = StateStore()
    rec = store.open("s1", model_state=None)
    assert store.count() == 1
    store.close(rec.handle)
    assert store.count() == 0
    with pytest.raises(HandleNotFound):
        store.get(rec.handle)


def test_concurrent_commits_only_one_succeeds():
    """Invariant 5 (generation-checked mutation) under real concurrency,
    not just sequential calls: two threads racing compare_and_commit with
    the same expected_generation must yield exactly one winner."""
    store = StateStore()
    rec = store.open("s1", model_state=None)

    results = []
    barrier = threading.Barrier(2)

    def attempt(tag: str) -> None:
        barrier.wait()
        try:
            store.compare_and_commit(rec.handle, expected_generation=0, last_seq_applied=1, model_state=tag, last_text=tag)
            results.append(("ok", tag))
        except StaleGeneration:
            results.append(("stale", tag))

    threads = [threading.Thread(target=attempt, args=(tag,)) for tag in ("A", "B")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    outcomes = sorted(r[0] for r in results)
    assert outcomes == ["ok", "stale"], f"expected exactly one winner, got {results}"
    assert store.get(rec.handle).generation == 1
