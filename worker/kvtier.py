"""Worker-side client for the shared KV tier (cmd/kvtier), plus the local
hot cache that keeps it from being slower than pinning.

**Why a hot cache at all.** The shared tier exists so that any worker in a
model family can serve any chunk of a session: the request carries a
*reference*, the worker fetches the state behind it. That removes session
affinity as a correctness requirement — which is the whole point.

But a naive implementation makes every chunk a remote round trip, and is
then strictly slower than simply pinning the session. Production systems
do not do that. vLLM's prefix-cache-aware routing, LMCache and Mooncake
all keep a LOCAL copy and treat the shared tier as the fallback, with the
router merely *preferring* the node that already holds the state.

So affinity comes back — not as a requirement, but as an optimization:

    pinned session      affinity is REQUIRED; a miss is data loss
    shared tier         affinity is PREFERRED; a miss costs a fetch

That difference is the entire thesis. A miss here degrades to a fetch, and
a fetch that also misses degrades to replay. Correctness never depends on
where the request lands, which is precisely what lets a load balancer in
front of the fleet do its job.

`hit_kind` is reported per request so the trade is measurable rather than
asserted: "local" means this worker last wrote the state, "tier" means a
peer did and we paid a fetch, "miss" means nobody has it and the caller
must rebuild.

Transport is stdlib urllib on purpose. Every call here runs inside the
worker's `asyncio.to_thread` executor alongside inference, so blocking IO
is correct in context, and the worker gains no new runtime dependency.
Production moves these bytes by RDMA; the mechanism being demonstrated —
reference in the request, bytes on a side channel — is the same either way.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from typing import Any

KVTIER_URL = os.environ.get("KVTIER_URL", "").rstrip("/")
HOT_MAX = int(os.environ.get("KV_HOT_MAX", "32"))

# Match the tier's own header. The tier never interprets it; it round-trips
# it so a reader can tell, before deserializing, whether the blob it just
# fetched could possibly belong to this model family.
COMPAT_HEADER = "X-Compat-Key"

_TIMEOUT_S = 5.0


class TierMiss(Exception):
    """No state behind that reference: expired, evicted, or never written.

    Ordinary, not exceptional. The caller starts fresh or replays.
    """


class TierUnavailable(Exception):
    """The tier itself could not be reached."""


class KVTier:
    """Local hot cache in front of the shared tier."""

    def __init__(self, base_url: str = KVTIER_URL, hot_max: int = HOT_MAX) -> None:
        self.base_url = base_url.rstrip("/")
        self.hot_max = hot_max
        # ref -> live model_state object. Deliberately holds the DESERIALIZED
        # state, not bytes: a local hit then costs nothing at all, which is
        # what makes the locality win worth having.
        self._hot: "OrderedDict[str, Any]" = OrderedDict()
        self.stats = {
            "local_hits": 0,
            "tier_hits": 0,
            "misses": 0,
            "puts": 0,
            "put_bytes": 0,
            "fetch_bytes": 0,
            "conflicts": 0,
            "errors": 0,
        }

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    # --- local hot cache ---------------------------------------------

    def _hot_take(self, ref: str):
        """Remove and return a hot entry — a take, never a peek.

        **This is a correctness requirement, not a cache policy.** Adapters
        mutate inference state IN PLACE and return the same object
        (adapters/zipformer_kv.py: `_consume` writes `st.bank.pending`,
        `frames_consumed`, absorbs new tensors, and extends the
        hypothesis). So handing out a reference to a cached entry and then
        letting `infer` run turns that entry into the state AFTER the
        chunk, while it is still filed under the version BEFORE it.

        The damage is specific: a retry of that chunk landing back on this
        worker would load its own corrupted entry and apply the chunk
        twice, while the same retry routed to a peer would fetch the
        correct serialized version from the tier and be right. Correctness
        would depend on placement — the exact property this design exists
        to remove.

        Taking the entry makes the aliasing harmless: once a version has
        been handed to inference it is gone from here, so a retry must go
        to the tier, which holds the real, immutable bytes. A session is
        served serially, so in the happy path a version is read exactly
        once and this costs nothing. The alternative — deep-copying 1.09 MB
        of tensors on every local hit — would pay a memcpy per chunk to
        preserve an entry nothing reads.
        """
        return self._hot.pop(ref, None)

    def _hot_put(self, ref: str, state: Any) -> None:
        self._hot[ref] = state
        self._hot.move_to_end(ref)
        while len(self._hot) > self.hot_max:
            self._hot.popitem(last=False)

    # --- the shared tier ---------------------------------------------

    def _url(self, ref: str) -> str:
        return f"{self.base_url}/kv/{urllib.parse.quote(ref, safe='')}"

    def fetch_blob(self, ref: str) -> tuple[bytes, str]:
        """Raw bytes plus the compatibility key the writer advertised."""
        if not self.enabled:
            raise TierUnavailable("KVTIER_URL not set")
        req = urllib.request.Request(self._url(ref), method="GET")
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
                blob = resp.read()
                return blob, resp.headers.get(COMPAT_HEADER, "")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise TierMiss(ref) from None
            self.stats["errors"] += 1
            raise TierUnavailable(f"tier GET {e.code}") from None
        except Exception as e:  # network, DNS, timeout
            self.stats["errors"] += 1
            raise TierUnavailable(str(e)) from None

    def put_blob(self, ref: str, blob: bytes, compat_key: str) -> None:
        if not self.enabled:
            raise TierUnavailable("KVTIER_URL not set")
        req = urllib.request.Request(
            self._url(ref),
            data=blob,
            method="PUT",
            headers={"Content-Type": "application/octet-stream", COMPAT_HEADER: compat_key},
        )
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
                resp.read()
        except urllib.error.HTTPError as e:
            # 409 means this version already exists. The tier is create-only
            # because versions are immutable, and a retry of the same chunk
            # names the same sink — so a refusal is confirmation, not
            # failure. All the caller needs is for the version to exist.
            if e.code == 409:
                self.stats["conflicts"] += 1
                return
            self.stats["errors"] += 1
            raise TierUnavailable(f"tier PUT {e.code}") from None
        except Exception as e:
            self.stats["errors"] += 1
            raise TierUnavailable(str(e)) from None

    # --- the two operations the request path actually uses ------------

    def load(self, ref: str, adapter, expect_compat: str) -> tuple[Any, str]:
        """Resolve a reference to live model state.

        Returns (state, hit_kind) where hit_kind is "local" or "tier".
        Raises TierMiss when no version of that reference exists anywhere.

        The compatibility check happens BEFORE deserializing, and refuses
        rather than coerces — the same rule as /v1/stream/restore. A blob
        from another model family is not convertible; treating it as one
        would be the silent corruption this project exists to prevent.
        """
        local = self._hot_take(ref)
        if local is not None:
            self.stats["local_hits"] += 1
            return local, "local"

        blob, advertised = self.fetch_blob(ref)
        if advertised and expect_compat and advertised != expect_compat:
            raise ValueError(
                f"compatibility key mismatch: blob={advertised[:19]}… worker={expect_compat[:19]}…"
            )
        state = adapter.deserialize(blob)
        self.stats["tier_hits"] += 1
        self.stats["fetch_bytes"] += len(blob)
        # Deliberately NOT cached under `ref`: this object is about to be
        # mutated by infer, and filing it under the version it is about to
        # stop being is the same aliasing bug _hot_take exists to prevent.
        # It enters the cache in store(), under the version it becomes.
        return state, "tier"

    def store(self, ref: str, state: Any, adapter, compat_key: str) -> int:
        """Publish state under a reference, and keep it hot locally.

        Synchronous on purpose. A write-behind would let a peer read a
        reference the tier does not have yet; that degrades safely to a
        miss, but it would make the demonstration flaky for no gain at
        this scale. Production overlaps this with the next chunk.
        """
        blob = adapter.serialize(state)
        self.put_blob(ref, blob, compat_key)
        self._hot_put(ref, state)
        self.stats["puts"] += 1
        self.stats["put_bytes"] += len(blob)
        return len(blob)

    def forget(self, ref: str) -> None:
        self._hot.pop(ref, None)
