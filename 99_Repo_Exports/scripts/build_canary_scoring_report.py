#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore


def bucket(score: float) -> str:
    if score >= 90:
        return 'green'
    if score >= 70:
        return 'yellow'
    return 'red'


def main() -> int:
    parser = argparse.ArgumentParser(description='Build canary report for repaired/quarantined sid.')
    parser.add_argument('--dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument(
        '--out',
        default=os.getenv('CANARY_REPORT_PATH', '/var/lib/trade-runbook/reports/latest_canary_scoring.json'),
    )
    args = parser.parse_args()
    if not args.dsn or psycopg is None:
        raise RuntimeError('psycopg + EXECUTION_JOURNAL_DSN required')

    metrics: dict = {
        'quarantined_sid_count': 0,
        'auto_unquarantined_sid_count': 0,
        'repair_run_count': 0,
        'emergency_flatten_count': 0,
    }
    with psycopg.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select count(distinct sid) from execution_quarantine_ledger"
                " where event_type in ('QUARANTINED','AUTO_QUARANTINED')"
            )
            metrics['quarantined_sid_count'] = int((cur.fetchone() or [0])[0])
            cur.execute(
                "select count(distinct sid) from execution_quarantine_ledger where event_type='AUTO_UNQUARANTINED'"
            )
            metrics['auto_unquarantined_sid_count'] = int((cur.fetchone() or [0])[0])
            cur.execute(
                "select count(*) from execution_quarantine_ledger"
                " where event_type in ('REPAIR_APPLIED','AUTO_REPAIR_APPLIED')"
            )
            metrics['repair_run_count'] = int((cur.fetchone() or [0])[0])
            cur.execute("select count(*) from execution_order_events where event_type='EMERGENCY_FLATTENED'")
            metrics['emergency_flatten_count'] = int((cur.fetchone() or [0])[0])

    score = 100.0
    score -= min(metrics['quarantined_sid_count'] * 2.0, 40.0)
    score -= min(metrics['emergency_flatten_count'] * 5.0, 40.0)
    score += min(metrics['auto_unquarantined_sid_count'] * 1.0, 10.0)
    report = {**metrics, 'score': round(score, 2), 'bucket': bucket(score)}

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
