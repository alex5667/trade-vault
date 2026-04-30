from __future__ import annotations

"""Dedicated HTTP health server for execution-projection-worker (P1.2.2).

Endpoints
---------
GET /healthz
    Liveness probe — always 200 OK if the process is alive.

GET /readyz
    Readiness probe — 200 if the worker is leader AND lag < threshold
    503 otherwise. Consumers (Docker Compose health-check) use this to
    route traffic only to the ready worker.

GET /api/execution-projection/health
    Full JSON health snapshot from ``ExecutionProjectionWorker.health_snapshot()``.
    Fields: leader, fencing_token, cursor, cursor_age_ms, lag_ms
    last_batch_ts_ms, ready, lease_enabled, worker_id.

Configuration (ENV)
-------------------
EXEC_PROJECTION_HEALTH_PORT      int   default 8090
EXEC_PROJECTION_LAG_READYZ_MAX_MS int  default 30000
"""

import json
import logging
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable, Dict, Optional

log = logging.getLogger('execution_projection_health_server')

# Registry of worker reference — set once at startup
_WORKER_REF: Optional[Any] = None  # ExecutionProjectionWorker | None
_LAG_READYZ_MAX_MS: int = 30000


def _health_snapshot() -> Dict[str, Any]:
    """Retrieve health snapshot from the registered worker, or a fallback."""
    if _WORKER_REF is None:
        return {
            'leader': False
            'ready': False
            'error': 'worker not registered'
        }
    try:
        return _WORKER_REF.health_snapshot(lag_readyz_max_ms=_LAG_READYZ_MAX_MS)
    except Exception as exc:
        return {
            'leader': False
            'ready': False
            'error': str(exc)
        }


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler for health endpoints."""

    # Silence default stderr logging in production
    def log_message(self, fmt: str, *args: Any) -> None:
        pass

    def _send_json(self, status: int, body: Dict[str, Any]) -> None:
        raw = json.dumps(body, default=str).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_text(self, status: int, text: str) -> None:
        raw = text.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split('?')[0]

        if path == '/healthz':
            # Liveness: always OK while process is running
            self._send_text(200, 'ok\n')

        elif path == '/readyz':
            # Readiness: leader + lag within threshold
            snap = _health_snapshot()
            if snap.get('ready'):
                self._send_text(200, 'ready\n')
            else:
                reason = 'not_leader' if not snap.get('leader') else 'lag_high'
                self._send_text(503, f'not_ready reason={reason}\n')

        elif path == '/api/execution-projection/health':
            snap = _health_snapshot()
            status = 200 if snap.get('ready') else 503
            self._send_json(status, snap)

        else:
            self._send_text(404, 'not found\n')


class ProjectionHealthServer:
    """Threaded HTTP server wrapping ExecutionProjectionWorker health.

    Usage::

        server = ProjectionHealthServer(worker, port=8090)
        server.start()   # non-blocking, daemon thread
        # ... main loop runs normally ...
        server.stop()
    """

    def __init__(
        self
        worker: Any,  # ExecutionProjectionWorker
        *
        port: int = 8090
        lag_readyz_max_ms: int = 30000
    ) -> None:
        global _WORKER_REF, _LAG_READYZ_MAX_MS
        _WORKER_REF = worker
        _LAG_READYZ_MAX_MS = lag_readyz_max_ms
        self.port = port
        self._httpd: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the health HTTP server in a background daemon thread."""
        try:
            self._httpd = HTTPServer(('', self.port), _HealthHandler)
        except OSError as exc:
            log.warning('Could not bind health server on port %d: %s', self.port, exc)
            return
        self._thread = threading.Thread(
            target=self._httpd.serve_forever
            daemon=True
            name=f'projection-health:{self.port}'
        )
        self._thread.start()
        log.info('Projection health server listening on :%d', self.port)

    def stop(self) -> None:
        """Gracefully shut down the server."""
        if self._httpd is not None:
            self._httpd.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


# ---------------------------------------------------------------------------
# Standalone entrypoint — for the separate health sidecar container
# ---------------------------------------------------------------------------

def main() -> int:  # pragma: no cover
    """Run health server as a standalone process (sidecar pattern).

    Requires ENV:
        REDIS_URL
        EXEC_PROJECTION_HEALTH_PORT         (default 8090)
        EXEC_PROJECTION_LAG_READYZ_MAX_MS   (default 30000)
        EXEC_PROJECTION_LEASE_ENABLE        (default 1)
        ... all other worker env vars ...
    """
    logging.basicConfig(
        level=logging.INFO
        format='%(asctime)s %(levelname)s %(name)s: %(message)s'
        stream=sys.stdout
    )

    # Import worker lazily to avoid circular deps at module level
    try:
        from services.execution_projection_worker import (
            _redis_from_env, _worker_from_env
        )
    except ImportError:
        from execution_projection_worker import (  # type: ignore
            _redis_from_env, _worker_from_env
        )

    port = int(os.getenv('EXEC_PROJECTION_HEALTH_PORT', '8090'))
    lag_readyz = int(os.getenv('EXEC_PROJECTION_LAG_READYZ_MAX_MS', '30000'))

    r = _redis_from_env()
    worker = _worker_from_env(r)

    server = ProjectionHealthServer(worker, port=port, lag_readyz_max_ms=lag_readyz)
    server.start()

    log.info('Sidecar health server started (not running projection loop)')
    # Keep the sidecar process alive
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
