#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Periodic execution healthcheck for systemd timer / cron.

Runs the SQL/Redis consistency checker and a small set of freshness checks,
writes a stable JSON document consumed by the runbook server, optionally exports
Prometheus textfile metrics, and exits with a severity-driven code suitable for
timer monitoring.

Exit codes
----------
0  – everything ok
1  – warning threshold exceeded or user-stream is stale
2  – critical mismatches detected

The JSON report is written to ``${RUNBOOK_REPORT_DIR}/latest_execution_health.json``
so that the trade-runbook-server can expose it via ``/api/health/latest``.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow direct execution without installing the package
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import check_execution_consistency as consistency

# P3.3-ops-complete: stream retention guard report
try:
    from services.execution_state_replay import stream_retention_guard_report
except Exception:  # pragma: no cover
    try:
        from binance_execution.execution_state_replay import stream_retention_guard_report  # type: ignore
    except Exception:
        from execution_state_replay import stream_retention_guard_report  # type: ignore


def build_autonomy_recommendations(report: Dict[str, Any]) -> Dict[str, Any]:
    """Derive autonomy actions from the health report.

    P3.3-autonomy: exposes whether the checkpoint scrubber and/or the
    retention-guard quarantine policy should be triggered.  Written into the
    health JSON so that ``auto_trigger_checkpoint_scrubber.py`` (and any external
    consumer) can act on it without reimplementing the decision logic.
    """
    consistency_doc = dict(report.get('consistency') or {})
    retention = dict(report.get('retention_guard') or {})
    overall = str(report.get('overall_status') or 'unknown')
    trigger_scrubber = bool(
        overall in {'warning', 'critical'}
        or int(retention.get('breached_checkpoints') or 0) > 0
        or int(consistency_doc.get('critical_mismatches') or 0) > 0
    )
    trigger_retention_quarantine = bool(int(retention.get('breached_checkpoints') or 0) > 0)
    return {
        'trigger_checkpoint_scrubber': trigger_scrubber,
        'trigger_retention_guard_quarantine': trigger_retention_quarantine,
        'reasons': [
            reason for reason, flag in {
                'overall_not_ok': overall in {'warning', 'critical'},
                'retention_guard_breached': int(retention.get('breached_checkpoints') or 0) > 0,
                'critical_mismatches': int(consistency_doc.get('critical_mismatches') or 0) > 0,
            }.items() if flag
        ]
    }


def render_prometheus_textfile(report: Dict[str, Any]) -> str:
    """Convert the health report dict to Prometheus node-exporter textfile format.

    Produces a .prom file suitable for node_exporter's textfile_collector.
    All values are gauges; the numeric status_code encodes ok=0/warning=1/critical=2.
    """
    consistency_doc = dict(report.get('consistency') or {})
    user_stream = dict(report.get('user_stream') or {})
    autonomy = dict(report.get('autonomy_recommendations') or {})
    status = str(report.get('overall_status') or 'unknown')
    code_map = {'ok': 0, 'warning': 1, 'critical': 2}
    lines = [
        '# HELP trade_execution_health_status_code Overall health status as a numeric code (ok=0, warning=1, critical=2).',
        '# TYPE trade_execution_health_status_code gauge',
        f'trade_execution_health_status_code {code_map.get(status, 3)}',
        '# HELP trade_execution_health_status Status flags labelled by level.',
        '# TYPE trade_execution_health_status gauge',
    ]
    for level in ('ok', 'warning', 'critical'):
        lines.append(f'trade_execution_health_status{{level="{level}"}} {1 if level == status else 0}')
    metric_pairs = {
        'trade_execution_consistency_redis_state_count': int(consistency_doc.get('redis_state_count') or 0),
        'trade_execution_consistency_stream_sid_count': int(consistency_doc.get('stream_sid_count') or 0),
        'trade_execution_consistency_sql_order_count': int(consistency_doc.get('sql_order_count') or 0),
        'trade_execution_consistency_mismatches_total': int(consistency_doc.get('mismatches_total') or 0),
        'trade_execution_consistency_critical_mismatches': int(consistency_doc.get('critical_mismatches') or 0),
        'trade_execution_consistency_warning_mismatches': int(consistency_doc.get('warning_mismatches') or 0),
        'trade_execution_user_stream_age_ms': int(user_stream.get('age_ms') or 0),
        'trade_execution_user_stream_keys_checked': int(user_stream.get('keys_checked') or 0),
        'trade_execution_user_stream_stale': 1 if user_stream.get('is_stale') else 0,
        # P3.3-ops-complete: retention guard breach gauge
        'trade_execution_replay_retention_guard_breached': int((report.get('retention_guard') or {}).get('breached_checkpoints') or 0),
        # P3.3-autonomy: autonomy trigger gauges
        'trade_execution_autonomy_trigger_checkpoint_scrubber': 1 if autonomy.get('trigger_checkpoint_scrubber') else 0,
        'trade_execution_autonomy_trigger_retention_quarantine': 1 if autonomy.get('trigger_retention_guard_quarantine') else 0,
        'trade_execution_health_checked_at_ms': int(report.get('checked_at_ms') or 0),
    }
    for name, value in metric_pairs.items():
        lines.append(f'# TYPE {name} gauge')
        lines.append(f'{name} {value}')
    return '\n'.join(lines) + '\n'


def _write_atomic(path: Path, payload: str) -> None:
    """Write payload to path atomically via a .tmp sibling and rename.

    This prevents Prometheus from reading a partially written .prom file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(payload, encoding='utf-8')
    tmp.replace(path)


def _check_user_stream_freshness(
    redis_client: Any,
    *,
    cache_prefix: str,
    stale_ms: int,
) -> Dict[str, Any]:
    """Scan user-stream cache keys and determine if the latest event is stale.

    Returns a dict with:
      - keys_checked   : int
      - newest_event_time_ms : int (0 if no keys found)
      - age_ms         : int
      - is_stale       : bool
    """
    newest = 0
    keys_checked = 0
    for key in redis_client.scan_iter(match=f"{cache_prefix}*"):
        keys_checked += 1
        doc: Dict[str, Any] = {}
        try:
            ktype = redis_client.type(key)
            if ktype == 'hash':
                doc = redis_client.hgetall(key) or {}
            elif ktype == 'string':
                # user_stream:status is stored as a JSON string, not a hash
                raw = redis_client.get(key) or '{}'
                try:
                    import json as _json
                    parsed = _json.loads(raw)
                    if isinstance(parsed, dict):
                        doc = parsed
                except Exception:
                    pass
        except Exception:
            doc = {}
        # Try both field names: event_time_ms (hash) and updated_at_ms (JSON string)
        ts = int(float(doc.get('event_time_ms') or doc.get('updated_at_ms') or 0))
        newest = max(newest, ts)

    now_ms = get_ny_time_millis()
    # If no keys found: treat age as effectively infinite
    age_ms = max(0, now_ms - newest) if newest else 10 ** 12
    return {
        'keys_checked': keys_checked,
        'newest_event_time_ms': newest,
        'age_ms': age_ms,
        'is_stale': bool(age_ms > stale_ms),
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description='Run execution health checks and write JSON report.'
    )
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--journal-dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument('--report-dir', default=os.getenv('RUNBOOK_REPORT_DIR', '/var/lib/trade-runbook/reports'))
    parser.add_argument('--user-stream-cache-prefix', default=os.getenv('USER_STREAM_CACHE_PREFIX', 'orders:user_stream:'))
    parser.add_argument('--user-stream-stale-ms', type=int, default=int(os.getenv('USER_STREAM_STALE_MS', '120000')))
    # P7: path for node-exporter textfile collector (.prom file); empty = disabled
    parser.add_argument('--textfile-output', default=os.getenv('EXEC_HEALTHCHECK_TEXTFILE_PATH', ''))
    args = parser.parse_args(argv)

    import redis  # type: ignore
    r = redis.from_url(args.redis_url, decode_responses=True)

    # --- consistency check (may be skipped if no DSN configured) ---
    summary: Optional[consistency.ConsistencySummary] = None
    consistency_error: Optional[str] = None
    if args.journal_dsn:
        try:
            _raw_prefix = os.getenv('EXEC_CONSISTENCY_SID_PREFIX_ALLOWLIST', '')
            _sid_prefix_allowlist = consistency._parse_prefix_allowlist(_raw_prefix)
            summary = consistency.run_check(
                redis_url=args.redis_url,
                journal_dsn=args.journal_dsn,
                state_prefix=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'),
                exec_stream=os.getenv('EXEC_STREAM', 'orders:exec'),
                stream_count=int(os.getenv('EXEC_CONSISTENCY_STREAM_COUNT', '20000')),
                sid_prefix_allowlist=_sid_prefix_allowlist,
            )
        except Exception as exc:
            consistency_error = str(exc)
    else:
        consistency_error = 'EXECUTION_JOURNAL_DSN not configured – skipping SQL check'

    # --- user-stream freshness ---
    freshness = _check_user_stream_freshness(
        r,
        cache_prefix=args.user_stream_cache_prefix,
        stale_ms=args.user_stream_stale_ms,
    )

    # --- P3.3-ops-complete: stream retention guard ---
    retention_guard = stream_retention_guard_report(
        r,
        exec_stream=os.getenv('EXEC_STREAM', 'orders:exec'),
        checkpoint_prefix=os.getenv('EXEC_REPLAY_CHECKPOINT_KEY_PREFIX', 'orders:exec:replay:cursor:'),
        sample_limit=int(os.getenv('EXEC_REPLAY_RETENTION_GUARD_SAMPLE_LIMIT', '2000')),
    )

    # --- derive overall status ---
    # P3.3-ops-complete: retention guard breach counts as critical
    retention_guard_breached = int((retention_guard or {}).get('breached_checkpoints') or 0) > 0
    if summary is not None:
        if summary.critical_mismatches > 0 or retention_guard_breached:
            overall = 'critical'
        elif summary.warning_mismatches > 0 or freshness['is_stale']:
            overall = 'warning'
        else:
            overall = 'ok'
    elif consistency_error:
        # No DSN: status driven by freshness and retention guard
        overall = 'critical' if retention_guard_breached else ('warning' if freshness['is_stale'] else 'ok')
    else:
        overall = 'unknown'

    report: Dict[str, Any] = {
        'checked_at_ms': get_ny_time_millis(),
        'consistency': consistency.asdict(summary) if summary is not None else None,
        'consistency_error': consistency_error,
        'user_stream': freshness,
        # P3.3-ops-complete: retention guard report included in health snapshot
        'retention_guard': retention_guard,
        'overall_status': overall,
    }
    # P3.3-autonomy: derive autonomy recommendations and embed in report
    report['autonomy_recommendations'] = build_autonomy_recommendations(report)

    # --- write report ---
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    latest = report_dir / 'latest_execution_health.json'
    # P7: write atomically to prevent the runbook server from reading a partial file
    _write_atomic(latest, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + '\n')
    # P7: optional node-exporter textfile export for Prometheus scraping
    if args.textfile_output:
        _write_atomic(Path(args.textfile_output), render_prometheus_textfile(report))
    # Print a compact version to stdout: collapse verbose 'presence' mismatches
    # into a single summary entry to avoid flooding logs on startup / long runs.
    compact_report = dict(report)
    if compact_report.get('consistency') and isinstance(compact_report['consistency'], dict):
        raw_mm = compact_report['consistency'].get('mismatches') or []
        presence_mm = [m for m in raw_mm if m.get('category') == 'presence']
        other_mm = [m for m in raw_mm if m.get('category') != 'presence']
        if len(presence_mm) > 0:
            other_mm.append({
                'category': 'presence',
                'detail': f'{len(presence_mm)} sid(s) missing from at least two mirrors (suppressed)',
                'severity': 'warning',
                'sid': '__summary__',
            })
        compact_report = dict(compact_report)
        compact_report['consistency'] = dict(compact_report['consistency'])
        compact_report['consistency']['mismatches'] = other_mm
    print(json.dumps(compact_report, ensure_ascii=False, sort_keys=True))

    if report['overall_status'] == 'critical':
        return 2
    if report['overall_status'] == 'warning':
        return 1
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
