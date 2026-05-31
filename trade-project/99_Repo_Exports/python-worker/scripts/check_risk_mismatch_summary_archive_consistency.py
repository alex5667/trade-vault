#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""P5X: Check consistency between risk_mismatch_summary_mv and the hot+archive mismatch ledger.

Recomputes the aggregate over the union of:
  - risk_mismatch_quarantine_ledger       (hot table)
  - risk_mismatch_quarantine_ledger_archive (archive table)

then full-outer-joins the result against risk_mismatch_summary_mv to detect
any divergence between what the materialized view reports and the actual ledger data.

Writes:
  - latest_risk_mismatch_archive_consistency.json    – full report
  - <textfile_output> (if set)                       – Prometheus textfile metrics

Environment variables
---------------------
RISK_AUDIT_SQL_DSN                              – PostgreSQL DSN (falls back to EXECUTION_JOURNAL_DSN)
RISK_MISMATCH_ARCHIVE_CONSISTENCY_REPORT_PATH   – output JSON report path
RISK_MISMATCH_ARCHIVE_CONSISTENCY_TEXTFILE_PATH – optional Prometheus textfile path
RISK_MISMATCH_ARCHIVE_CONSISTENCY_STALE_SEC     – stale threshold in seconds (default: 1800)

Usage
-----
  python3 scripts/check_risk_mismatch_summary_archive_consistency.py
  python3 scripts/check_risk_mismatch_summary_archive_consistency.py --dsn postgresql://... --textfile-output /path/to/metrics.prom
"""

import argparse
import json
import os
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
    """Render Prometheus textfile-compatible metrics from the consistency report.

    Metrics:
      trade_risk_mismatch_archive_consistency_freshness_seconds – age of report in seconds
      trade_risk_mismatch_archive_consistency_stale             – 1 if report age > stale_threshold
      trade_risk_mismatch_archive_consistency_mismatch_total    – number of mismatching rows
    """
    gen = int(report.get('generated_at_ms') or 0)
    freshness = max(0.0, (get_ny_time_millis() - gen) / 1000.0) if gen else 0.0
    stale_threshold = int(report.get('freshness_stale_threshold_sec') or 1800)
    lines = [
        '# HELP trade_risk_mismatch_archive_consistency_freshness_seconds Freshness of archive consistency report.',
        '# TYPE trade_risk_mismatch_archive_consistency_freshness_seconds gauge',
        f'trade_risk_mismatch_archive_consistency_freshness_seconds {freshness}',
        '# HELP trade_risk_mismatch_archive_consistency_stale Whether archive consistency report is stale.',
        '# TYPE trade_risk_mismatch_archive_consistency_stale gauge',
        f'trade_risk_mismatch_archive_consistency_stale {1 if freshness > float(stale_threshold) else 0}',
        '# HELP trade_risk_mismatch_archive_consistency_mismatch_total Number of mismatching summary rows.',
        '# TYPE trade_risk_mismatch_archive_consistency_mismatch_total gauge',
        f"trade_risk_mismatch_archive_consistency_mismatch_total {int(report.get('mismatch_count') or 0)}",
    ]
    return '\n'.join(lines) + '\n'


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Check consistency between risk_mismatch_summary_mv and hot+archive mismatch ledger.'
    )
    parser.add_argument(
        '--dsn',
        default=os.getenv('RISK_AUDIT_SQL_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')),
    )
    parser.add_argument(
        '--out',
        default=os.getenv(
            'RISK_MISMATCH_ARCHIVE_CONSISTENCY_REPORT_PATH',
            '/var/lib/trade-runbook/reports/latest_risk_mismatch_archive_consistency.json',
        )
    )
    parser.add_argument(
        '--textfile-output',
        default=os.getenv('RISK_MISMATCH_ARCHIVE_CONSISTENCY_TEXTFILE_PATH', ''),
    )
    parser.add_argument(
        '--freshness-stale-threshold-sec',
        type=int,
        default=int(os.getenv('RISK_MISMATCH_ARCHIVE_CONSISTENCY_STALE_SEC', '1800')),
    )
    args = parser.parse_args()

    if not args.dsn or psycopg is None:
        raise RuntimeError('psycopg + DSN required')

    report: dict = {
        'generated_at_ms': get_ny_time_millis(),
        'freshness_stale_threshold_sec': int(args.freshness_stale_threshold_sec),
        'rows': [],
    }

    # Full-outer-join risk_mismatch_summary_mv against the recomputed
    # aggregate over the union of the hot table and archive table.
    # This reveals any divergence between what the MV reports and reality.
    query = """
    with unioned as (
      select * from risk_mismatch_quarantine_ledger
      union all
      select * from risk_mismatch_quarantine_ledger_archive
    ), base as (
      select '1h'::text as window_name, * from unioned where created_ts_ms >= (extract(epoch from now() - interval '1 hour')*1000)::bigint
      union all
      select '24h'::text as window_name, * from unioned where created_ts_ms >= (extract(epoch from now() - interval '24 hours')*1000)::bigint
      union all
      select '7d'::text as window_name, * from unioned where created_ts_ms >= (extract(epoch from now() - interval '7 days')*1000)::bigint
    ), expected as (
      select window_name,
             coalesce(nullif(tier,''), 'UNKNOWN') as tier,
             count(*)::bigint as quarantine_count,
             count(distinct sid)::bigint as distinct_sid_count,
             avg(repeated_count)::double precision as avg_repeated_count,
             max(repeated_count)::integer as max_repeated_count,
             avg(mismatch_rate)::double precision as avg_mismatch_rate
      from base
      group by window_name, coalesce(nullif(tier,''), 'UNKNOWN')
    )
    select
      coalesce(mv.window_name, ex.window_name) as window_name,
      coalesce(mv.tier, ex.tier) as tier,
      mv.quarantine_count as mv_quarantine_count,
      ex.quarantine_count as expected_quarantine_count,
      mv.distinct_sid_count as mv_distinct_sid_count,
      ex.distinct_sid_count as expected_distinct_sid_count,
      mv.avg_repeated_count as mv_avg_repeated_count,
      ex.avg_repeated_count as expected_avg_repeated_count,
      mv.max_repeated_count as mv_max_repeated_count,
      ex.max_repeated_count as expected_max_repeated_count,
      mv.avg_mismatch_rate as mv_avg_mismatch_rate,
      ex.avg_mismatch_rate as expected_avg_mismatch_rate
    from risk_mismatch_summary_mv mv
    full outer join expected ex using(window_name, tier)
    order by 1,2
    """

    try:
        with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
            cur.execute(query)
            cols = [d.name for d in cur.description]
            for row in cur.fetchall():
                item = {k: row[i] for i, k in enumerate(cols)}
                mismatch = False
                # Check integer exact-match fields
                for key in ('quarantine_count', 'distinct_sid_count', 'max_repeated_count'):
                    if (item.get(f'mv_{key}') or 0) != (item.get(f'expected_{key}') or 0):
                        mismatch = True
                # Check float fields with epsilon tolerance
                for key in ('avg_repeated_count', 'avg_mismatch_rate'):
                    a = float(item.get(f'mv_{key}') or 0.0)
                    b = float(item.get(f'expected_{key}') or 0.0)
                    if abs(a - b) > 1e-6:
                        mismatch = True
                item['mismatch'] = mismatch
                report['rows'].append(item)
    except psycopg.OperationalError as e:
        print(f"Warning: Database connection failed (transient): {e}")
        return 1

    report['row_count'] = len(report['rows'])
    report['mismatch_count'] = sum(1 for row in report['rows'] if row.get('mismatch'))

    _write_atomic(Path(args.out), json.dumps(report, ensure_ascii=False, indent=2) + '\n')

    if args.textfile_output:
        _write_atomic(Path(args.textfile_output), render_textfile(report))

    print(json.dumps(
        {'row_count': report['row_count'], 'mismatch_count': report['mismatch_count']},
        ensure_ascii=False,
    ))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
