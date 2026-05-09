from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

try:  # pragma: no cover
    from services.active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics
    from services.active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine
    from services.active_symbol_guard_runbook import ActiveSymbolGuardRunbookExecutor
    from services.binance_futures_client import BinanceFuturesClient
    from services.execution_projection_worker import _redis_from_env
except Exception:  # pragma: no cover
    from active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics  # type: ignore
    from active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine  # type: ignore
    from active_symbol_guard_runbook import ActiveSymbolGuardRunbookExecutor  # type: ignore
    from binance_futures_client import BinanceFuturesClient  # type: ignore
    from execution_projection_worker import _redis_from_env  # type: ignore


class _Handler(BaseHTTPRequestHandler):  # pragma: no cover
    def _diag(self) -> ActiveSymbolGuardDiagnostics:
        return self.server.diagnostics  # type: ignore[attr-defined]

    def _runbook(self) -> ActiveSymbolGuardRunbookExecutor:
        return self.server.runbook  # type: ignore[attr-defined]

    def _write_json(self, *, status: int, payload: dict):
        """Send a JSON response with correct Content-Type and Content-Length."""
        body = json.dumps(payload, ensure_ascii=False, default=str).encode('utf-8')
        self.send_response(int(status))
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json_body(self) -> dict:
        """Read and parse the JSON request body safely."""
        length = int(self.headers.get('Content-Length', '0') or '0')
        raw = self.rfile.read(length) if length > 0 else b'{}'
        try:
            payload = json.loads(raw.decode('utf-8') or '{}')
        except Exception:
            payload = {}
        return payload if isinstance(payload, dict) else {}

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        diag = self._diag()
        policy = self.server.policy  # type: ignore[attr-defined]
        runbook = self._runbook()
        if path == '/metrics':
            body = b'# HELP active_symbol_guard_up Is the exporter running\n# TYPE active_symbol_guard_up gauge\nactive_symbol_guard_up 1.0\n'
            status = 200
            self.send_response(status)
            self.send_header('Content-Type', 'text/plain; version=0.0.4')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        elif path in ('/healthz', '/readyz', '/api/active-symbol-guard/health'):
            snap = diag.snapshot()
            status = 200 if snap.get('ok') else 503
            body = json.dumps(snap, ensure_ascii=False).encode('utf-8')
        elif path == '/api/active-symbol-guard/heatmap':
            body = json.dumps(diag.heatmap(), ensure_ascii=False).encode('utf-8')
            status = 200
        elif path == '/api/active-symbol-guard/runbook/dashboard':
            # P13: operator audit dashboard with active holds, acks, recent history
            body = json.dumps(runbook.runbook_dashboard(), ensure_ascii=False).encode('utf-8')
            status = 200
        elif path.startswith('/api/active-symbol-guard/triage/symbol/'):
            symbol = path.rsplit('/', 1)[-1].strip().upper()
            body = json.dumps(policy.triage_symbol(symbol, include_exchange=True), ensure_ascii=False).encode('utf-8')
            status = 200
        elif path.startswith('/api/active-symbol-guard/triage/sid/'):
            sid = path.rsplit('/', 1)[-1].strip()
            body = json.dumps(policy.triage_sid(sid, include_exchange=True), ensure_ascii=False).encode('utf-8')
            status = 200
        elif path.startswith('/api/active-symbol-guard/runbook/symbol/'):
            symbol = path.rsplit('/', 1)[-1].strip().upper()
            body = json.dumps(runbook.runbook_state_symbol(symbol), ensure_ascii=False).encode('utf-8')
            status = 200
        elif path.startswith('/api/active-symbol-guard/runbook/sid/'):
            sid = path.rsplit('/', 1)[-1].strip()
            body = json.dumps(runbook.runbook_state_sid(sid), ensure_ascii=False).encode('utf-8')
            status = 200
        elif path.startswith('/api/active-symbol-guard/runbook/ticket/'):
            # P13: ticket-linked audit history
            ticket = path.rsplit('/', 1)[-1].strip()
            body = json.dumps({'ticket': ticket, 'history': runbook.audit_history(ticket=ticket, limit=100)}, ensure_ascii=False).encode('utf-8')
            status = 200
        elif path.startswith('/api/active-symbol-guard/incident/symbol/'):
            symbol = path.rsplit('/', 1)[-1].strip().upper()
            body = json.dumps(diag.incident_bundle_symbol(symbol, include_exchange=True), ensure_ascii=False).encode('utf-8')
            status = 200
        elif path.startswith('/api/active-symbol-guard/incident/sid/'):
            sid = path.rsplit('/', 1)[-1].strip()
            body = json.dumps(diag.incident_bundle_sid(sid, include_exchange=True), ensure_ascii=False).encode('utf-8')
            status = 200
        elif path.startswith('/api/active-symbol-guard/symbol/'):
            symbol = path.rsplit('/', 1)[-1].strip().upper()
            body = json.dumps(diag.debug_symbol(symbol, include_exchange=True), ensure_ascii=False).encode('utf-8')
            status = 200
        elif path.startswith('/api/active-symbol-guard/sid/'):
            sid = path.rsplit('/', 1)[-1].strip()
            body = json.dumps(diag.debug_sid(sid, include_exchange=True), ensure_ascii=False).encode('utf-8')
            status = 200
        else:
            self._write_json(status=404, payload={'ok': False, 'error': 'not_found'})
            return
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        """Handle runbook action POST endpoints with full operator/ticket/result audit trail."""
        parsed = urlparse(self.path)
        path = parsed.path
        payload = self._json_body()
        runbook = self._runbook()
        try:
            if path == '/api/active-symbol-guard/actions/hold/apply':
                out = runbook.apply_hold_symbol(symbol=(payload.get('symbol') or ''), operator=(payload.get('operator') or ''), ticket=(payload.get('ticket') or ''), reason=(payload.get('reason') or ''), ttl_sec=payload.get('ttl_sec'))
            elif path == '/api/active-symbol-guard/actions/hold/revoke':
                out = runbook.revoke_hold_symbol(symbol=(payload.get('symbol') or ''), operator=(payload.get('operator') or ''), ticket=(payload.get('ticket') or ''), reason=(payload.get('reason') or ''))
            elif path == '/api/active-symbol-guard/actions/force-release':
                out = runbook.guarded_force_release(symbol=(payload.get('symbol') or ''), operator=(payload.get('operator') or ''), ticket=(payload.get('ticket') or ''), expected_sid=(payload.get('expected_sid') or ''), reason=(payload.get('reason') or ''), dry_run=bool(payload.get('dry_run')))
            elif path == '/api/active-symbol-guard/actions/escalation/ack':
                out = runbook.escalation_ack(symbol=(payload.get('symbol') or ''), sid=(payload.get('sid') or ''), fingerprint=(payload.get('fingerprint') or ''), operator=(payload.get('operator') or ''), ticket=(payload.get('ticket') or ''), reason=(payload.get('reason') or ''), ttl_sec=payload.get('ttl_sec'))
            elif path == '/api/active-symbol-guard/actions/escalation/renew':
                out = runbook.escalation_renew(symbol=(payload.get('symbol') or ''), sid=(payload.get('sid') or ''), fingerprint=(payload.get('fingerprint') or ''), operator=(payload.get('operator') or ''), ticket=(payload.get('ticket') or ''), reason=(payload.get('reason') or ''), ttl_sec=payload.get('ttl_sec'))
            else:
                self._write_json(status=404, payload={'ok': False, 'error': 'not_found'})
                return
            status = 200 if bool(out.get('ok', True)) else 409
            self._write_json(status=status, payload=out)
        except Exception as exc:
            self._write_json(status=400, payload={'ok': False, 'error': str(exc), 'path': path})

    def log_message(self, fmt, *args):
        return


def _client_from_env():  # pragma: no cover
    try:
        return BinanceFuturesClient.from_env()
    except Exception:
        return None


def main() -> int:  # pragma: no cover
    r = _redis_from_env()
    client = _client_from_env()
    diag = ActiveSymbolGuardDiagnostics(
        r,
        client=client,
        active_symbol_key_prefix=os.getenv('ORDERS_ACTIVE_SYMBOL_KEY_PREFIX', 'orders:active_symbol_sid:'),
        state_key_prefix=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'),
        state_ttl_sec=int(os.getenv('ORDERS_STATE_TTL_SEC', '86400')),
        tombstone_ttl_sec=int(os.getenv('ACTIVE_SYMBOL_GUARD_TOMBSTONE_TTL_SEC', '120')),
        stale_tombstone_ms=int(os.getenv('ACTIVE_SYMBOL_GUARD_STALE_TOMBSTONE_MS', '600000')),
        hot_symbol_limit=int(os.getenv('ACTIVE_SYMBOL_GUARD_EXPORTER_HOT_LIMIT', '10')),
    )
    host = os.getenv('ACTIVE_SYMBOL_GUARD_EXPORTER_HOST', '0.0.0.0')
    port = int(os.getenv('ACTIVE_SYMBOL_GUARD_EXPORTER_PORT', '8788'))
    policy = ActiveSymbolGuardIncidentPolicyEngine(r, diag)
    runbook = ActiveSymbolGuardRunbookExecutor(r, diagnostics=diag, policy=policy, client=client)
    server = HTTPServer((host, port), _Handler)
    server.diagnostics = diag  # type: ignore[attr-defined]
    server.policy = policy  # type: ignore[attr-defined]
    server.runbook = runbook  # type: ignore[attr-defined]
    print(f'active-symbol-guard-exporter listening on {host}:{port}')
    server.serve_forever()
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
