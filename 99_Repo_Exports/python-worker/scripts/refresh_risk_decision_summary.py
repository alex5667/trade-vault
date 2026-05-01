#!/usr/bin/env python3
from __future__ import annotations
"""P4.6/P4.7: Fetch latest risk decision continuous aggregate and emit JSON + Prometheus textfile.

P4.6: Reads from continuous aggregates (see db/migrations/20260306_11_risk_decisions_cagg.sql)
and writes a JSON report to RISK_DECISION_SUMMARY_REPORT_PATH for consumption by:
  - trade-runbook-server (/api/risk-summary/latest)
  - Alertmanager deep-links
  - Grafana annotations

P4.7 extensions:
  - Writes Prometheus textfile for node_exporter textfile_collector:
      trade_risk_summary_freshness_seconds
      trade_risk_summary_stale
      trade_risk_summary_row_count
  - Atomic writes via temp file to prevent partial reads

Environment variables
---------------------
RISK_AUDIT_SQL_DSN                – PostgreSQL DSN (falls back to EXECUTION_JOURNAL_DSN)
RISK_DECISION_SUMMARY_REPORT_PATH – output JSON path
RISK_SUMMARY_TEXTFILE_PATH        – Prometheus textfile output path (optional)
RISK_SUMMARY_FRESHNESS_MAX_SEC    – stale threshold in seconds (default: 900)

Usage
-----
  python3 scripts/refresh_risk_decision_summary.py
  python3 scripts/refresh_risk_decision_summary.py --dsn postgresql://... --out /tmp/out.json
  python3 scripts/refresh_risk_decision_summary.py --textfile-output /var/lib/node_exporter/textfile_collector/trade_risk_summary.prom
"""
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from pathlib import Path

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover – skipped in unit-test environment
    psycopg = None  # type: ignore


def _write_atomic(path: Path, payload: str) -> None:
    """Write payload to path atomically via a temp file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(payload, encoding='utf-8')
    tmp.replace(path)


def render_prometheus_textfile(report: dict) -> str:
    """Render Prometheus textfile content for node_exporter textfile_collector.

    Metrics exported:
      trade_risk_summary_freshness_seconds – age of latest refresh
      trade_risk_summary_stale             – 1 if older than RISK_SUMMARY_FRESHNESS_MAX_SEC
      trade_risk_summary_row_count         – number of rows in the summary
    """
    now_ms = get_ny_time_millis()
    latest_refreshed_ts_ms = int(report.get('latest_refreshed_ts_ms') or 0)
    freshness_ms = max(0, now_ms - latest_refreshed_ts_ms) if latest_refreshed_ts_ms else 10**12
    freshness_sec = freshness_ms / 1000.0
    stale = 1 if freshness_sec > float(os.getenv('RISK_SUMMARY_FRESHNESS_MAX_SEC', '900')) else 0
    lines = [
        '# HELP trade_risk_summary_freshness_seconds Age of the latest risk summary refresh in seconds.',
        '# TYPE trade_risk_summary_freshness_seconds gauge',
        f'trade_risk_summary_freshness_seconds {freshness_sec}',
        '# HELP trade_risk_summary_stale Whether the risk summary is stale.',
        '# TYPE trade_risk_summary_stale gauge',
        f'trade_risk_summary_stale {stale}',
        '# HELP trade_risk_summary_row_count Number of rows in the latest summary.',
        '# TYPE trade_risk_summary_row_count gauge',
        f'trade_risk_summary_row_count {int(report.get("row_count") or 0)}'],
    return '\n'.join(lines) + '\n',


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Fetch latest risk decision continuous aggregate and emit JSON report.',
    ),
    parser.add_argument(
        '--dsn',
        default=os.getenv('RISK_AUDIT_SQL_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')),
        help='PostgreSQL DSN for risk_decisions database',
    ),
    parser.add_argument(
        '--out',
        default=os.getenv(
            'RISK_DECISION_SUMMARY_REPORT_PATH',
            '/var/lib/trade-runbook/reports/latest_risk_decision_summary.json',
        ),
        help='Output path for the JSON report',
    )
    parser.add_argument(
        '--textfile-output',
        default=os.getenv('RISK_SUMMARY_TEXTFILE_PATH', ''),
        help='Prometheus textfile output path for node_exporter (optional)',
    )
    args = parser.parse_args()

    if not args.dsn or psycopg is None:
        raise RuntimeError(
            'psycopg + DSN required. '
            'Set RISK_AUDIT_SQL_DSN or EXECUTION_JOURNAL_DSN environment variable.'
        )

    report: dict = {'rows': [], 'generated_at_ms': get_ny_time_millis()}

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            with psycopg.connect(args.dsn, connect_timeout=10) as conn:
                with conn.cursor() as cur:
                    # Read latest bucket from each Continuous Aggregate
                    cur.execute(
                        '''
                        with cagg_1h as (
                            select '1h'::text as window_name, tier, level, decision_count, allow_count, deny_count,
                                   clamp_count, confidence_denial_count, avg_clamp_ratio,
                                   decision_latency_avg_ms,
                                   (extract(epoch from latest_created_ts) * 1000)::bigint as latest_created_ts_ms,
                                   (extract(epoch from now()) * 1000)::bigint as refreshed_ts_ms
                            from risk_decision_summary_1h
                            where bucket = (select max(bucket) from risk_decision_summary_1h)
                        )
                        cagg_24h as (
                            select '24h'::text as window_name, tier, level, decision_count, allow_count, deny_count,
                                   clamp_count, confidence_denial_count, avg_clamp_ratio,
                                   decision_latency_avg_ms, 
                                   (extract(epoch from latest_created_ts) * 1000)::bigint as latest_created_ts_ms,
                                   (extract(epoch from now()) * 1000)::bigint as refreshed_ts_ms
                            from risk_decision_summary_24h
                            where bucket = (select max(bucket) from risk_decision_summary_24h)
                        )
                        select * from cagg_1h
                        union all
                        select * from cagg_24h
                        order by window_name, tier, level
                        '''
                    )
                    cols = [d.name for d in cur.description]
                    for row in cur.fetchall():
                        report['rows'].append({k: row[i] for i, k in enumerate(cols)})
                conn.commit()
            break
        except Exception as e:
            print(f"Attempt {attempt}/{max_retries} failed connecting to DB: {e}")
            if attempt == max_retries:
                raise
            time.sleep(5)

    report['row_count'] = len(report['rows'])
    report['latest_refreshed_ts_ms'] = max(
        (int(row.get('refreshed_ts_ms') or 0) for row in report['rows']),
        default=0,
    )

    out = Path(args.out)
    # P4.7: atomic write to prevent partial-read races
    _write_atomic(out, json.dumps(report, ensure_ascii=False, indent=2) + '\n')

    # P4.7: optional Prometheus textfile for node_exporter textfile_collector
    if args.textfile_output:
        _write_atomic(Path(args.textfile_output), render_prometheus_textfile(report))

    print(json.dumps(
        {
            'rows': len(report['rows']),
            'out': str(out),
            'latest_refreshed_ts_ms': report['latest_refreshed_ts_ms'],
        },
        ensure_ascii=False,
    ))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
