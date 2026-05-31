from __future__ import annotations

"""Execution Bootstrap Health Server (P1.2.3 / P1.2.4).

Minimal HTTP server exposing endpoints for orchestration / monitoring:

  GET /healthz                                    — liveness  (uses ok field)
  GET /readyz                                     — readiness (uses ready field, 200 or 503)
  GET /api/execution-bootstrap/health             — full JSON snapshot (same as /readyz)
  GET /api/execution-bootstrap/incident/latest    — P1.2.4: latest persisted block incident
  GET /api/execution-bootstrap/runbook/latest     — P1.2.4: runbook payload with block reason

Both /readyz and /api/execution-bootstrap/health return HTTP 503 when the
combined bootstrap readiness check fails, allowing Docker healthcheck
probes to hold traffic and block dependent service startups.

ENV knobs:
  EXEC_BOOTSTRAP_HEALTH_HOST   default: 0.0.0.0
  EXEC_BOOTSTRAP_HEALTH_PORT   default: 8787
  REDIS_URL                    default: redis://redis-worker-1:6379/0
"""

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

try:  # pragma: no cover
    from services.execution_bootstrap_supervisor import _redis_from_env, _supervisor_from_env
except Exception:  # pragma: no cover
    from execution_bootstrap_supervisor import _redis_from_env, _supervisor_from_env  # type: ignore


class _Handler(BaseHTTPRequestHandler):  # pragma: no cover
    """Minimal request handler — one snapshot per request, no caching."""

    def do_GET(self):
        sup = self.server.supervisor  # type: ignore[attr-defined]
        if self.path in ('/healthz', '/readyz', '/api/execution-bootstrap/health'):
            snap = sup.health_snapshot().to_dict()
            # /healthz uses the general ok flag; /readyz and API use ready
            is_ready_path = self.path in ('/readyz', '/api/execution-bootstrap/health')
            status = 200 if (snap.get('ready') if is_ready_path else snap.get('ok')) else 503
            body = json.dumps(snap, ensure_ascii=False).encode('utf-8')
        elif self.path == '/api/execution-bootstrap/incident/latest':
            # P1.2.4: latest persisted bootstrap block incident (operator runbook support)
            incident = sup.latest_block()
            status = 200 if incident else 404
            body = json.dumps(
                incident or {'reason': 'no_incident_persisted'}, ensure_ascii=False
            ).encode('utf-8')
        elif self.path == '/api/execution-bootstrap/runbook/latest':
            # P1.2.4: combined runbook — current snapshot + latest block + actions
            payload = sup.runbook_snapshot()
            status = 200
            body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # Healthcheck probe closed connection early

    def log_message(self, fmt, *args):
        # Silence default access log — metrics cover observability
        return


def main() -> int:  # pragma: no cover
    r = _redis_from_env()
    sup = _supervisor_from_env(r)
    host = os.getenv('EXEC_BOOTSTRAP_HEALTH_HOST', '0.0.0.0')
    port = int(os.getenv('EXEC_BOOTSTRAP_HEALTH_PORT', '8787'))
    server = HTTPServer((host, port), _Handler)
    server.supervisor = sup  # type: ignore[attr-defined]
    print(f'execution-bootstrap-health listening on {host}:{port}')
    server.serve_forever()
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
