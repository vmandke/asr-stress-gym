"""ASR Stress Gym worker process.

At this milestone (M0) the worker advertises identity and health only,
matching the WorkerAdvert shape in docs/build-plan.md ("Worker
advertisement"). No adapter, no model, no inference: those land at M1
(mock adapter, open/push/flush/close) and M5 (real adapters, registered in
worker/adapters/registry.py — see docs/implementation-plan.md).

Deliberately stdlib-only (http.server) so the image has zero dependencies
until an adapter actually needs one (FastAPI arrives at M1, numpy/onnx/
ctranslate2 at M5, one worker at a time).
"""

from __future__ import annotations

import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STARTED_AT = time.time()

WORKER_ID = os.environ.get("WORKER_ID", "worker-unknown")
MODEL = os.environ.get("MODEL", "unset")
COMPAT_KEY_HASH = os.environ.get("COMPAT_KEY_HASH", "unset")
PORT = int(os.environ.get("HEALTH_PORT", "9000"))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:  # quiet default access log
        pass

    def do_GET(self) -> None:  # noqa: N802 (stdlib method name)
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return

        body = json.dumps(
            {
                "worker_id": WORKER_ID,
                "status": "READY",
                "model": MODEL,
                "compatibility_key_hash": COMPAT_KEY_HASH,
                "active_sessions": 0,
                "state_bytes": 0,
                "queue_depth": 0,
                "rtf_p50": None,
                "last_heartbeat_ms": int(time.time() * 1000),
            }
        ).encode()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"worker[{WORKER_ID}]: health endpoint listening on :{PORT} "
          f"(M0 skeleton — no adapter/inference yet)")
    server.serve_forever()


if __name__ == "__main__":
    main()
