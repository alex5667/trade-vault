#!/usr/bin/env python3
from __future__ import annotations

"""HTTP control-plane for Binance dust cleanup admin, including P14 ACK workflow.

Endpoints
---------
GET  /healthz                        → liveness probe
GET  /api/binance-dust/state         → full denylist/cooldown state
GET  /api/binance-dust/symbol/<sym>  → per-symbol state
GET  /api/binance-dust/audit         → audit stream (symbol= limit=)

POST /api/binance-dust/denylist/add  → {symbol, operator, reason, ticket, ttl_sec?}
POST /api/binance-dust/denylist/remove → {symbol, operator, reason, ticket}
POST /api/binance-dust/cooldown/clear  → {symbol, operator, reason, ticket}

# P14: ACK workflow
GET  /api/binance-dust/ack/dashboard   → list all active ACK states (limit=?)
POST /api/binance-dust/ack             → {kind, symbol, operator, reason, ticket, ttl_sec?, fingerprint?}
POST /api/binance-dust/ack/renew       → {kind, symbol, operator, reason, ticket, ttl_sec?}
POST /api/binance-dust/ack/revoke      → {kind, symbol, operator, reason, ticket}
"""

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(CURRENT_DIR)
for _p in (REPO_ROOT,):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

from services.binance_dust_cleanup_admin import BinanceDustCleanupAdmin
from services.binance_dust_cleanup_admin_ack import (
    ack_reminder
    renew_reminder_ack
    revoke_reminder_ack
    ack_dashboard
)


class _Handler(BaseHTTPRequestHandler):
    admin = BinanceDustCleanupAdmin()

    def _send(self, code: int, payload):
        body = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get('Content-Length', '0') or '0')
        raw = self.rfile.read(length) if length > 0 else b'{}'
        try:
            return json.loads(raw.decode('utf-8') or '{}')
        except Exception:
            return {}

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')
        qs = parse_qs(parsed.query)
        if path == '/healthz':
            return self._send(200, {'ok': True})
        if path == '/api/binance-dust/state':
            return self._send(200, self.admin.current_state())
        if path.startswith('/api/binance-dust/symbol/'):
            symbol = path.split('/')[-1]
            return self._send(200, self.admin.symbol_state(symbol))
        if path == '/api/binance-dust/audit':
            symbol = (qs.get('symbol') or [None])[0]
            limit = int((qs.get('limit') or ['50'])[0] or '50')
            return self._send(200, self.admin.recent_audit(symbol=symbol, limit=limit))
        # P14: ACK dashboard
        if path == '/api/binance-dust/ack/dashboard':
            limit = int((qs.get('limit') or ['200'])[0] or '200')
            return self._send(200, ack_dashboard(self.admin.r, limit=limit))  # type: ignore[attr-defined]
        return self._send(404, {'ok': False, 'error': 'not_found'})

    def do_POST(self):  # noqa: N802
        path = self.path.rstrip('/')
        doc = self._read_json()
        try:
            if path == '/api/binance-dust/denylist/add':
                payload = self.admin.add_denylist_symbol(
                    doc.get('symbol') or ''
                    operator=doc.get('operator') or ''
                    reason=doc.get('reason') or ''
                    ticket=doc.get('ticket') or ''
                    ttl_sec=int(doc.get('ttl_sec') or 0)
                )
                return self._send(200, payload)
            if path == '/api/binance-dust/denylist/remove':
                payload = self.admin.remove_denylist_symbol(
                    doc.get('symbol') or ''
                    operator=doc.get('operator') or ''
                    reason=doc.get('reason') or ''
                    ticket=doc.get('ticket') or ''
                )
                return self._send(200, payload)
            if path == '/api/binance-dust/cooldown/clear':
                payload = self.admin.clear_cooldown(
                    doc.get('symbol') or ''
                    operator=doc.get('operator') or ''
                    reason=doc.get('reason') or ''
                    ticket=doc.get('ticket') or ''
                )
                return self._send(200, payload)
            # P14: ACK workflow endpoints
            if path == '/api/binance-dust/ack':
                return self._send(
                    200
                    ack_reminder(
                        self.admin.r,  # type: ignore[attr-defined]
                        kind=doc.get('kind', '')
                        symbol=doc.get('symbol', '')
                        operator=doc.get('operator', '')
                        reason=doc.get('reason', '')
                        ticket=doc.get('ticket', '')
                        ttl_sec=int(doc.get('ttl_sec', 1800))
                        fingerprint=doc.get('fingerprint', '')
                    )
                )
            if path == '/api/binance-dust/ack/renew':
                return self._send(
                    200
                    renew_reminder_ack(
                        self.admin.r,  # type: ignore[attr-defined]
                        kind=doc.get('kind', '')
                        symbol=doc.get('symbol', '')
                        operator=doc.get('operator', '')
                        reason=doc.get('reason', '')
                        ticket=doc.get('ticket', '')
                        ttl_sec=int(doc.get('ttl_sec', 1800))
                    )
                )
            if path == '/api/binance-dust/ack/revoke':
                return self._send(
                    200
                    revoke_reminder_ack(
                        self.admin.r,  # type: ignore[attr-defined]
                        kind=doc.get('kind', '')
                        symbol=doc.get('symbol', '')
                        operator=doc.get('operator', '')
                        reason=doc.get('reason', '')
                        ticket=doc.get('ticket', '')
                    )
                )
            return self._send(404, {'ok': False, 'error': 'not_found'})
        except Exception as exc:
            return self._send(400, {'ok': False, 'error': str(exc)})


def main() -> int:
    host = os.getenv('BINANCE_DUST_ADMIN_HTTP_HOST', '0.0.0.0')
    port = int(os.getenv('BINANCE_DUST_ADMIN_HTTP_PORT', '8791'))
    server = ThreadingHTTPServer((host, port), _Handler)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
