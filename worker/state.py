"""handle -> SessionRecord, with generation-checked compare-and-commit —
build-plan.md "Compare-and-commit", adapted to the open/push/flush/close
HTTP surface in docs/PROTOCOL.md. One StateStore per worker process.

This is where `expected_generation` is enforced, and the only place it is
enforced (docs/PROTOCOL.md: "Two fields carry the correctness weight").
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from typing import Any


class StaleGeneration(Exception):
    """The caller's expected_generation no longer matches — someone else's
    write already landed, or the caller's view of state is stale. Reject
    rather than silently apply over newer state."""


class HandleNotFound(Exception):
    pass


@dataclass
class SessionRecord:
    handle: str
    session_id: str
    generation: int = 0
    last_seq_applied: int = 0
    last_text: str = ""
    model_state: Any = None
    lock: threading.Lock = field(default_factory=threading.Lock)


class StateStore:
    def __init__(self) -> None:
        self._records: dict[str, SessionRecord] = {}
        self._registry_lock = threading.Lock()

    def open(self, session_id: str, model_state: Any, *, last_seq_applied: int = 0) -> SessionRecord:
        """last_seq_applied defaults to 0 (a genuinely fresh session) but
        the restore path (worker/server.py's /v1/stream/restore) passes
        the checkpoint's own seq — the new handle starts at generation 0
        either way (it's a fresh record in THIS worker's store; generation
        numbers are worker-local and never need to match the dead
        worker's numbering — the coordinator reads back whatever this
        call returns and uses that from here on), but last_seq_applied
        must reflect how much audio the checkpoint already accounts for,
        or the coordinator would redundantly (though harmlessly, thanks
        to idempotent replay) resend audio this state already reflects.
        """
        handle = str(uuid.uuid4())
        rec = SessionRecord(handle=handle, session_id=session_id, model_state=model_state, last_seq_applied=last_seq_applied)
        with self._registry_lock:
            self._records[handle] = rec
        return rec

    def get(self, handle: str) -> SessionRecord:
        with self._registry_lock:
            rec = self._records.get(handle)
        if rec is None:
            raise HandleNotFound(handle)
        return rec

    def compare_and_commit(
        self,
        handle: str,
        expected_generation: int,
        *,
        last_seq_applied: int,
        model_state: Any,
        last_text: str,
    ) -> SessionRecord:
        rec = self.get(handle)
        with rec.lock:
            if rec.generation != expected_generation:
                raise StaleGeneration(
                    f"handle={handle} expected={expected_generation} actual={rec.generation}"
                )
            rec.generation += 1
            rec.last_seq_applied = last_seq_applied
            rec.model_state = model_state
            rec.last_text = last_text
            return rec

    def close(self, handle: str) -> None:
        with self._registry_lock:
            self._records.pop(handle, None)

    def count(self) -> int:
        with self._registry_lock:
            return len(self._records)

    def all(self) -> list:
        """A snapshot of the live records, for read-only telemetry.

        Returns a copied list rather than the live dict so a caller
        iterating it cannot be tripped by a concurrent open/close — the
        records themselves are shared, which is fine for reading a size.
        """
        with self._registry_lock:
            return list(self._records.values())
