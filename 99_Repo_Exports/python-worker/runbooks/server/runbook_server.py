#!/usr/bin/env python3
from __future__ import annotations

"""Tiny static runbook/health server.

Serves Markdown runbooks, generated health reports, and a lightweight index page
without introducing a separate Node/Nginx dependency. This makes it easy to run
inside compose or under systemd on a jump host.

Environment variables
---------------------
RUNBOOK_ROOT         – directory containing *.md runbooks (default: parent of this file)
RUNBOOK_REPORT_DIR   – directory containing JSON report files (default: /var/lib/trade-runbook/reports)
RUNBOOK_SERVER_BIND  – bind address (default: 0.0.0.0)
RUNBOOK_SERVER_PORT  – TCP port (default: 18080)

Routes
------
GET /                       – HTML index listing runbooks and latest health badge
GET /index.html             – same as /
GET /runbooks/<rel>         – raw runbook file (path-traversal protected)
GET /reports/<name>         – raw JSON report file (path-traversal protected)
GET /api/health/latest      – latest_execution_health.json
GET /healthz                – liveness probe (always 200 "ok" if process is alive)
"""

import html
import http.server
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import parse_qs, urlparse


# ---------------------------------------------------------------------------
# P5.6 Audit Chain helpers
# ---------------------------------------------------------------------------

EXEC_AUDIT_REPORT_JSON_PATH = os.getenv(
    "EXEC_AUDIT_REPORT_JSON", "latest_execution_audit_chain.json"
)


def load_audit_chain_report(path: Optional[str] = None) -> Dict[str, Any]:
    """Load the audit chain JSON report from *path*.
    Returns a safe error dict if the file is missing or unreadable.
    """
    p = Path(path or EXEC_AUDIT_REPORT_JSON_PATH)
    if not p.exists():
        return {
            "schema_version": "p5.6.v1",
            "generated_at_ts": None,
            "total_broken": 0,
            "broken_by_kind": {},
            "broken": [],
            "error": f"report not found: {p}",
        }
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "schema_version": "p5.6.v1",
            "generated_at_ts": None,
            "total_broken": 0,
            "broken_by_kind": {},
            "broken": [],
            "error": f"report read error: {exc}",
        }


def filter_audit_chain_report(
    report: Mapping[str, Any], query: Mapping[str, List[str]]
) -> Dict[str, Any]:
    """Filter audit chain broken rows by optional query parameters.

    Supported filters (all optional):
      - sid          – exact match on row["sid"]
      - signal_id    – exact match on row["signal_id"]
      - closed_trade_id – exact match on row["closed_trade_id"]
      - kind         – exact match on row["kind"]
      - limit        – max number of rows to return (default 200)
    """
    rows = list(report.get("broken") or [])

    def first(name: str) -> str:
        vals = query.get(name) or []
        return str(vals[0]).strip() if vals else ""

    sid = first("sid")
    signal_id = first("signal_id")
    closed_trade_id = first("closed_trade_id")
    kind = first("kind")
    limit_raw = first("limit")
    limit = int(limit_raw) if limit_raw.isdigit() else 200

    def ok(row: Mapping[str, Any]) -> bool:
        if sid and str(row.get("sid") or "") != sid:
            return False
        if signal_id and str(row.get("signal_id") or "") != signal_id:
            return False
        if closed_trade_id and str(row.get("closed_trade_id") or "") != closed_trade_id:
            return False
        if kind and str(row.get("kind") or "") != kind:
            return False
        return True

    filtered = [row for row in rows if ok(row)][: max(1, limit)]
    broken_by_kind: Dict[str, int] = {}
    for row in filtered:
        k = str(row.get("kind") or "unknown")
        broken_by_kind[k] = broken_by_kind.get(k, 0) + 1

    out = dict(report)
    out["broken"] = filtered
    out["broken_by_kind"] = dict(sorted(broken_by_kind.items()))
    out["total_broken"] = len(filtered)
    return out


def discover_runbooks(root: Path) -> List[Path]:
    """Return all *.md files under *root*, sorted by relative path."""
    return sorted(p for p in root.rglob('*.md') if p.is_file())


def render_index(runbook_root: Path, report_root: Path) -> str:
    """Generate the HTML index page.

    Displays:
    - A health badge from ``latest_execution_health.json``
    - A list of links to all discovered runbooks
    - A list of links to all generated JSON reports
    """
    reports = sorted(report_root.glob('*.json')) if report_root.exists() else []
    runbooks = discover_runbooks(runbook_root)

    items = []
    for rb in runbooks:
        rel = rb.relative_to(runbook_root)
        items.append(
            f'<li><a href="/runbooks/{html.escape(str(rel))}">{html.escape(str(rel))}</a></li>'
        )

    report_items = []
    for rp in reports:
        report_items.append(
            f'<li><a href="/reports/{html.escape(rp.name)}">{html.escape(rp.name)}</a></li>'
        )

    # Derive badge from latest health report, gracefully degrade on errors
    latest = report_root / 'latest_execution_health.json'
    badge = 'unknown'
    if latest.exists():
        try:
            badge = json.loads(latest.read_text(encoding='utf-8')).get('overall_status', 'unknown')
        except Exception:
            badge = 'invalid'

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Trade Runbooks</title>
<style>
  body{{font-family:sans-serif;margin:2rem;max-width:1100px}}
  code{{background:#f4f4f4;padding:.1rem .3rem}}
  .badge{{padding:.15rem .5rem;border-radius:4px;background:#eee}}
  .ok{{background:#d1fae5}}.warning{{background:#fef3c7}}.critical{{background:#fee2e2}}
</style>
</head><body>
<h1>Trade Execution Runbooks</h1>
<p>Latest health: <span class="badge {badge}">{html.escape(badge)}</span> — <a href="/api/health/latest">Health JSON</a> | <a href="/api/rebuild/latest">Rebuild JSON</a> | <a href="/api/autonomy/latest">Autonomy JSON</a> | <a href="/api/replay-slo/latest">Replay SLO JSON</a> | <a href="/api/risk-canary/latest">Risk Canary JSON</a> | <a href="/api/risk-summary/latest">Risk Summary JSON</a> | <a href="/api/operator-score/latest">Operator Score JSON</a> | <a href="/api/risk-mismatch/latest">Risk Drift JSON</a> | <a href="/api/risk-mismatch-summary/latest">Risk Drift Summary JSON</a> | <a href="/api/risk-mismatch-archive-consistency/latest">Risk Drift Archive Consistency JSON</a> | <a href="/api/risk-drift-autosilence/latest">Risk Drift Auto-Silence JSON</a> | <a href="/api/audit-chain/latest">Audit Chain JSON (P5.6)</a></p>
<h2>Runbooks</h2><ul>{''.join(items) or '<li>no runbooks found</li>'}</ul>
<h2>Reports</h2><ul>{''.join(report_items) or '<li>no reports found</li>'}</ul>
<p><code>/healthz</code> returns 200 if the server process is alive.</p>
</body></html>"""


class Handler(http.server.SimpleHTTPRequestHandler):
    # Class-level defaults; overridden by env at process start
    runbook_root = Path(os.getenv('RUNBOOK_ROOT', str(Path(__file__).resolve().parents[1])))
    report_root = Path(os.getenv('RUNBOOK_REPORT_DIR', '/var/lib/trade-runbook/reports'))

    def log_message(self, fmt: str, *args: object) -> None:  # type: ignore[override]
        """Suppress verbose access logs in production; keep errors."""
        if args and str(args[1]) not in ('200', '204'):
            super().log_message(fmt, *args)

    def _send_bytes(self, body: bytes, ctype: str, status: int = 200) -> None:
        """Helper: send a byte-body response."""
        self.send_response(status)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _safe_resolve(self, base: Path, rel: str) -> Path | None:
        """Resolve *rel* relative to *base* with path-traversal guard."""
        target = (base / rel).resolve()
        if not str(target).startswith(str(base.resolve())):
            return None
        return target if target.exists() else None

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        # --- liveness probe ---
        if path == '/healthz':
            self._send_bytes(b'ok\n', 'text/plain; charset=utf-8')
            return

        # --- P3.3-ops-complete: latest rebuild report JSON ---
        if path == '/api/rebuild/latest':
            latest = self.report_root / 'latest_rebuild_state.json'
            if latest.exists():
                self._send_bytes(latest.read_bytes(), 'application/json; charset=utf-8')
                return
            self.send_error(404, 'rebuild report not found')
            return

        # --- latest canary scoring JSON ---
        if path == '/api/canary/latest':
            latest = self.report_root / 'latest_canary_scoring.json'
            if latest.exists():
                self._send_bytes(latest.read_bytes(), 'application/json; charset=utf-8')
                return
            self.send_error(404, 'canary report not found')
            return

        # --- P3.3-autonomy: auto-scrubber decision report ---
        if path == '/api/autonomy/latest':
            latest = self.report_root / 'latest_auto_scrubber.json'
            if latest.exists():
                self._send_bytes(latest.read_bytes(), 'application/json; charset=utf-8')
                return
            self.send_error(404, 'autonomy report not found')
            return

        # --- P3.3-autonomy: replay/rehydrate SLO summary ---
        if path == '/api/replay-slo/latest':
            latest = self.report_root / 'latest_replay_slo_summary.json'
            if latest.exists():
                self._send_bytes(latest.read_bytes(), 'application/json; charset=utf-8')
                return
            self.send_error(404, 'replay slo summary not found')
            return

        # --- P4.5: risk engine quality canary report ---
        if path == '/api/risk-canary/latest':
            latest = self.report_root / 'latest_risk_engine_canary.json'
            if latest.exists():
                self._send_bytes(latest.read_bytes(), 'application/json; charset=utf-8')
                return
            self.send_error(404, 'risk canary report not found')
            return

        # --- P4.7: merged operator canary score (execution + replay + risk) ---
        if path == '/api/operator-score/latest':
            latest = self.report_root / 'latest_operator_score.json'
            if latest.exists():
                self._send_bytes(latest.read_bytes(), 'application/json; charset=utf-8')
                return
            self.send_error(404, 'operator score report not found')
            return

        # --- P4.6: aggregated risk decision summary (materialized SQL view refresh) ---
        if path == '/api/risk-summary/latest':
            latest = self.report_root / 'latest_risk_decision_summary.json'
            if latest.exists():
                self._send_bytes(latest.read_bytes(), 'application/json; charset=utf-8')
                return
            self.send_error(404, 'risk summary report not found')
            return

        # --- P4.8: risk mismatch quarantine summary (refresh_risk_mismatch_summary.py) ---
        if path == '/api/risk-mismatch/latest':
            latest = self.report_root / 'latest_risk_mismatch_summary.json'
            if latest.exists():
                self._send_bytes(latest.read_bytes(), 'application/json; charset=utf-8')
                return
            self.send_error(404, 'risk mismatch summary report not found')
            return

        # --- P4.9: risk mismatch materialized summary (separate named endpoint for alerting) ---
        if path == '/api/risk-mismatch-summary/latest':
            latest = self.report_root / 'latest_risk_mismatch_summary.json'
            if latest.exists():
                body = latest.read_bytes()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404, 'risk mismatch summary not found')
            return

        # --- P5X: risk drift auto-silence decision report ---
        if path == '/api/risk-drift-autosilence/latest':
            latest = self.report_root / 'latest_risk_drift_autosilence.json'
            if latest.exists():
                body = latest.read_bytes()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404, 'risk drift autosilence report not found')
            return

        # --- P5X: risk mismatch archive consistency report ---
        if path == '/api/risk-mismatch-archive-consistency/latest':
            latest = self.report_root / 'latest_risk_mismatch_archive_consistency.json'
            if latest.exists():
                body = latest.read_bytes()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404, 'risk mismatch archive consistency report not found')
            return

        # --- P5.6 execution audit-chain report (filterable) ---
        if path == '/api/audit-chain/latest':
            qs = parse_qs(parsed.query)
            report_path = os.getenv('EXEC_AUDIT_REPORT_JSON', EXEC_AUDIT_REPORT_JSON_PATH)
            report = load_audit_chain_report(report_path)
            filtered = filter_audit_chain_report(report, qs)
            body = json.dumps(filtered, ensure_ascii=False, indent=2, sort_keys=True).encode('utf-8')
            self._send_bytes(body, 'application/json; charset=utf-8')
            return

        # --- latest health JSON ---
        if path == '/api/health/latest':
            latest = self.report_root / 'latest_execution_health.json'
            if latest.exists():
                self._send_bytes(latest.read_bytes(), 'application/json; charset=utf-8')
                return
            self.send_error(404, 'health report not found')
            return

        # --- index page ---
        if path in ('/', '/index.html'):
            body = render_index(self.runbook_root, self.report_root).encode('utf-8')
            self._send_bytes(body, 'text/html; charset=utf-8')
            return

        # --- static runbook file ---
        if path.startswith('/runbooks/'):
            rel = path[len('/runbooks/'):]
            target = self._safe_resolve(self.runbook_root, rel)
            if target is None:
                self.send_error(404)
                return
            ctype = (
                'text/markdown; charset=utf-8'
                if target.suffix.lower() == '.md'
                else 'application/octet-stream'
            )
            self._send_bytes(target.read_bytes(), ctype)
            return

        # --- report JSON file ---
        if path.startswith('/reports/'):
            rel = path[len('/reports/'):]
            target = self._safe_resolve(self.report_root, rel)
            if target is None:
                self.send_error(404)
                return
            self._send_bytes(target.read_bytes(), 'application/json; charset=utf-8')
            return

        self.send_error(404)


def main() -> int:
    host = os.getenv('RUNBOOK_SERVER_BIND', '0.0.0.0')
    port = int(os.getenv('RUNBOOK_SERVER_PORT', '18080'))
    server = http.server.ThreadingHTTPServer((host, port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
