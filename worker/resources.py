"""Process and container resource readings for /health.

No psutil. Everything here comes from /proc and the cgroup filesystem,
which are already mounted in every container and cost nothing to add to
the image. On a non-Linux host (a developer running the worker directly on
macOS) every reading returns None rather than raising — the dashboard
draws a gap, which is the truth, instead of the worker failing its own
health check over a metric.

The cgroup readings matter more than the process ones here. docker-compose
pins a hard `memory:` limit per worker (1536M for the real-model workers),
so "how much memory is this worker using" is only actionable against that
ceiling: 900MB is comfortable at 1536M and fatal at 1024M. A dashboard
showing RSS alone cannot tell those apart, and the failure it misses — the
OOM killer taking the worker out — looks exactly like the SIGKILL the
chaos suite injects on purpose.
"""

from __future__ import annotations

import os
import time

_CLOCK_TICKS = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096


def _read(path: str) -> str | None:
    try:
        with open(path) as fh:
            return fh.read().strip()
    except OSError:
        return None


def _read_int(path: str) -> int | None:
    raw = _read(path)
    if raw is None:
        return None
    try:
        return int(raw.split()[0])
    except (ValueError, IndexError):
        return None


def rss_bytes() -> int | None:
    """Resident set size of THIS process (the worker child, not the supervisor)."""
    raw = _read("/proc/self/statm")
    if raw is None:
        return None
    try:
        return int(raw.split()[1]) * _PAGE_SIZE
    except (ValueError, IndexError):
        return None


def cgroup_memory() -> tuple[int | None, int | None]:
    """(current, limit) in bytes, cgroup v2 then v1.

    The limit is None when the cgroup reports `max` — an unlimited
    container. Reported as None rather than as a huge number so the
    dashboard shows "no limit" instead of drawing a bar against 2^63.
    """
    current = _read_int("/sys/fs/cgroup/memory.current")
    raw_max = _read("/sys/fs/cgroup/memory.max")
    if current is None:
        # cgroup v1 fallback
        current = _read_int("/sys/fs/cgroup/memory/memory.usage_in_bytes")
        raw_max = _read("/sys/fs/cgroup/memory/memory.limit_in_bytes")

    limit: int | None = None
    if raw_max is not None and raw_max != "max":
        try:
            parsed = int(raw_max.split()[0])
            # cgroup v1 reports "no limit" as a number near 2^63; anything
            # above a terabyte is that sentinel, not a real limit.
            limit = parsed if parsed < (1 << 40) else None
        except (ValueError, IndexError):
            limit = None
    return current, limit


_last_cpu: tuple[float, float] | None = None  # (wall_seconds, cpu_seconds)


def cpu_percent() -> float | None:
    """CPU use since the previous call, as a percentage of one core.

    Deliberately stateful and delta-based: /proc/self/stat gives cumulative
    CPU seconds since the process started, and reporting that directly
    would produce a chart that only ever goes up. The first call after
    startup has no previous sample and returns None.
    """
    global _last_cpu
    raw = _read("/proc/self/stat")
    if raw is None:
        return None
    try:
        # utime and stime are fields 14 and 15 (1-indexed), but the comm
        # field can contain spaces inside parentheses — split after it.
        after_comm = raw[raw.rindex(")") + 2 :].split()
        cpu_seconds = (int(after_comm[11]) + int(after_comm[12])) / _CLOCK_TICKS
    except (ValueError, IndexError):
        return None

    now = time.monotonic()
    previous, _last_cpu = _last_cpu, (now, cpu_seconds)
    if previous is None:
        return None
    wall_delta = now - previous[0]
    if wall_delta <= 0:
        return None
    return round(100.0 * (cpu_seconds - previous[1]) / wall_delta, 1)


def snapshot() -> dict:
    current, limit = cgroup_memory()
    return {
        "rss_bytes": rss_bytes(),
        "cgroup_memory_bytes": current,
        "cgroup_memory_limit_bytes": limit,
        "cpu_percent": cpu_percent(),
    }
