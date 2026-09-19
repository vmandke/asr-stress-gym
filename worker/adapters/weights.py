"""Where a real adapter finds its baked-in weights.

One lookup, one error message. The weights are fetched by models/fetch.sh
at IMAGE BUILD TIME (worker/Dockerfile) and copied to /app/models inside
the image; on a developer's host they sit in the repo's models/download.
Both are checked, in that order, so the same adapter code runs in a
container and in `make test-worker` with nothing to configure.

Nothing here ever downloads. If a slug is missing, that is a build
problem, and the exception says exactly which command fixes it — a worker
that quietly fetched 300MB on boot would break the hermetic
`docker compose up` promise in docs/build-plan.md and would make every
startup-latency number depend on someone else's CDN.
"""

from __future__ import annotations

import os
from pathlib import Path


class WeightsMissing(RuntimeError):
    pass


def _candidates() -> list[Path]:
    if env := os.environ.get("MODELS_DIR"):
        return [Path(env)]
    return [
        Path("/app/models"),  # inside the worker image
        Path(__file__).resolve().parents[2] / "models" / "download",  # repo checkout
    ]


def available() -> bool:
    """True when at least one weights root exists — used by the conformance
    suite to skip the real adapters on a fresh clone rather than fail them,
    since `models/fetch.sh` has not necessarily been run there."""
    return any(p.is_dir() for p in _candidates())


def resolve(slug: str) -> Path:
    for root in _candidates():
        d = root / slug
        if (d / ".complete").is_file():
            return d
    tried = ", ".join(str(p / slug) for p in _candidates())
    raise WeightsMissing(
        f"model weights for {slug!r} not found (looked in: {tried}). "
        f"Run `models/fetch.sh {slug}` — inside the image this happens at "
        f"build time, so a container hitting this means the image is stale."
    )


def file(slug: str, name: str) -> str:
    p = resolve(slug) / name
    if not p.is_file():
        raise WeightsMissing(f"{slug}: expected file {name} is missing from {p.parent}")
    return str(p)
