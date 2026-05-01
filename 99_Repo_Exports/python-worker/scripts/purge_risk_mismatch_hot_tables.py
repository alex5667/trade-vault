#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""P4.9/P5X: Archive and purge risk_mismatch_quarantine_ledger rows older than retention window.

Calls purge_risk_mismatch_hot_tables(cutoff_ts_ms) SQL function which:
  1. Ensures monthly range partition exists in risk_mismatch_quarantine_ledger_archive.
  2. Copies qualifying rows to archive.
  3. Deletes them from the hot table.

Environment variables
---------------------
RISK_AUDIT_SQL_DSN                      – PostgreSQL DSN (falls back to EXECUTION_JOURNAL_DSN).
RISK_MISMATCH_RETENTION_DAYS            – retention window in days (default: 30).
RISK_MISMATCH_RETENTION_REPORT_PATH     – output JSON report path.
RISK_MISMATCH_RETENTION_TEXTFILE_PATH   – optional Prometheus textfile collector path.
RISK_MISMATCH_RETENTION_STALE_SEC       – freshness stale threshold in seconds (default: 86400).

Usage
-----
  python3 scripts/purge_risk_mismatch_hot_tables.py
  python3 scripts/purge_risk_mismatch_hot_tables.py --dry-run
  python3 scripts/purge_risk_mismatch_hot_tables.py --retention-days 14
"""

import argparse
import json
import os
import time
from pathlib import Path

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore


def _write_atomic(path: Path, payload: str) -> None:
    """Atomically write payload to path via a temp-file rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(payload, encoding='utf-8')
    tmp.replace(path)


def render_textfile(report: dict) -> str:
    """Render Prometheus textfile-compatible metrics from the retention purge report.

    Metrics:
      trade_risk_mismatch_retention_freshness_seconds     – age of report in seconds
      trade_risk_mismatch_retention_stale                 – 1 if age > stale_threshold
      trade_risk_mismatch_retention_last_purged_quarantine – rows purged in latest run
    """
    generated_at_ms = int(report.get('generated_at_ms') or 0)
    freshness_seconds = max(0.0, (get_ny_time_millis() - generated_at_ms) / 1000.0) if generated_at_ms else 0.0
    stale_threshold = int(report.get('freshness_stale_threshold_sec') or 1800)
    lines = [
        '# HELP trade_risk_mismatch_retention_freshness_seconds Freshness of latest mismatch retention purge report.',
        '# TYPE trade_risk_mismatch_retention_freshness_seconds gauge',
        f'trade_risk_mismatch_retention_freshness_seconds {freshness_seconds}',
        '# HELP trade_risk_mismatch_retention_stale Whether mismatch retention purge report is stale.',
        '# TYPE trade_risk_mismatch_retention_stale gauge',
        f'trade_risk_mismatch_retention_stale {1 if freshness_seconds > float(stale_threshold) else 0}',
        '# HELP trade_risk_mismatch_retention_last_purged_quarantine Rows purged in latest mismatch retention run.',
        '# TYPE trade_risk_mismatch_retention_last_purged_quarantine gauge',
        f"trade_risk_mismatch_retention_last_purged_quarantine {int(report.get('purged_quarantine') or 0)}",
    ]
    return '\n'.join(lines) + '\n'


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Archive and purge risk mismatch quarantine ledger rows older than retention window.'
    )
    parser.add_argument(
        '--dsn',
        default=os.getenv('RISK_AUDIT_SQL_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')),
    )
    parser.add_argument(
        '--retention-days',
        type=int,
        default=int(os.getenv('RISK_MISMATCH_RETENTION_DAYS', '30')),
    )
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument(
        '--out',
        default=os.getenv(
            'RISK_MISMATCH_RETENTION_REPORT_PATH',
            '/var/lib/trade-runbook/reports/latest_risk_mismatch_retention.json',
        )
    )
    parser.add_argument(
        '--textfile-output',
        default=os.getenv('RISK_MISMATCH_RETENTION_TEXTFILE_PATH', ''),
    )
    parser.add_argument(
        '--freshness-stale-threshold-sec',
        type=int,
        default=int(os.getenv('RISK_MISMATCH_RETENTION_STALE_SEC', '86400')),
    )
    args = parser.parse_args()
    if not args.dsn or psycopg is None:
        raise RuntimeError('psycopg + DSN required')
    cutoff_ms = int((time.time() - args.retention_days * 86400) * 1000)
    out = {
        'cutoff_ts_ms': cutoff_ms,
        'dry_run': bool(args.dry_run),
        'generated_at_ms': get_ny_time_millis(),
        'freshness_stale_threshold_sec': int(args.freshness_stale_threshold_sec),
    }
    with psycopg.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            if args.dry_run:
                cur.execute(
                    'select count(*) from risk_mismatch_quarantine_ledger where created_ts_ms < %s',
                    (cutoff_ms,),
                )
                out['rows_to_purge'] = int(cur.fetchone()[0])
            else:
                cur.execute('select * from purge_risk_mismatch_hot_tables(%s)', (cutoff_ms,))
                row = cur.fetchone() or (0,)
                out['purged_quarantine'] = int(row[0])
        if not args.dry_run:
            conn.commit()
    # P5X: write JSON report and optional Prometheus textfile for retention observability
    _write_atomic(Path(args.out), json.dumps(out, ensure_ascii=False, indent=2) + '\n')
    if args.textfile_output:
        _write_atomic(Path(args.textfile_output), render_textfile(out))
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
