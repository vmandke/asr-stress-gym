"""Tiny parent process that spawns worker/server.py as a child and
answers process-level fault injection — docs/PROTOCOL.md "Fault
injection", docs/build-plan.md "Killing a worker": "the worker container
runs a tiny parent that spawns the real worker as a child. POST
/admin/die kills the child; POST /admin/restore respawns it. No Docker
socket, no elevated privileges, and it survives on any machine."

This is what the worker's Docker CMD actually runs (not server.py
directly) — see worker/Dockerfile. Listens on a SEPARATE port (9001,
"admin") from the real traffic (9000, "worker"): a killed child can't
answer anything on its own port, which is exactly the "worker died"
signal the gateway's health tracking is supposed to observe, so nothing
here should paper over that by proxying /health through to a dead child.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading

from fastapi import FastAPI
import uvicorn

ADMIN_PORT = int(os.environ.get("ADMIN_PORT", "9001"))
WORKER_ID = os.environ.get("WORKER_ID", "worker-unknown")

app = FastAPI()

_lock = threading.Lock()
_child: subprocess.Popen | None = None


def _spawn() -> None:
    global _child
    with _lock:
        if _child is not None and _child.poll() is None:
            return  # already running
        _child = subprocess.Popen([sys.executable, "server.py"])


def _kill() -> bool:
    global _child
    with _lock:
        if _child is None or _child.poll() is not None:
            return False  # nothing alive to kill
        _child.send_signal(signal.SIGKILL)
        _child.wait(timeout=5)
        return True


@app.post("/admin/die")
def admin_die() -> dict:
    killed = _kill()
    return {"killed": killed}


@app.post("/admin/restore")
def admin_restore() -> dict:
    _spawn()
    return {"spawned": True}


@app.get("/health")
def health() -> dict:
    with _lock:
        alive = _child is not None and _child.poll() is None
    return {"worker_id": WORKER_ID, "component": "supervisor", "child_alive": alive}


def main() -> None:
    _spawn()
    print(f"supervisor[{WORKER_ID}]: admin API on :{ADMIN_PORT}, child on server.py's own port")
    uvicorn.run(app, host="0.0.0.0", port=ADMIN_PORT, log_level="warning")


if __name__ == "__main__":
    main()
