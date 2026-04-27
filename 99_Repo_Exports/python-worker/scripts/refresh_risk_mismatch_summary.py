#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""P4.8/P4.9: Refresh risk_mismatch_summary_mv materialized view and export JSON report.

P4.9 addition: export Prometheus textfile metrics (freshness, stale flag, avg_rate,
quarantine_count per window/tier) so node-exporter textfile_collector can scrape them.

Calls REFRESH MATERIALIZED VIEW risk_mismatch_summary_mv and exports the rows
as latest_risk_mismatch_summary.json served by the runbook server at
/api/risk-mismatch/latest and /api/risk-mismatch-summary/latest.

Environment variables
---------------------
RISK_AUDIT_SQL_DSN                  – PostgreSQL DSN (falls back to EXECUTION_JOURNAL_DSN).
RISK_MISMATCH_SUMMARY_REPORT_PATH   – output path (default: /var/lib/trade-runbook/reports/latest_risk_mismatch_summary.json).
RISK_MISMATCH_SUMMARY_TEXTFILE_PATH – if set, also writes Prometheus textfile here.
RISK_MISMATCH_SUMMARY_STALE_SEC     – freshness threshold in seconds (default: 1800).

Usage
-----
  python3 scripts/refresh_risk_mismatch_summary.py
  python3 scripts/refresh_risk_mismatch_summary.py --dsn postgresql://... --out /tmp/out.json
  python3 scripts/refresh_risk_mismatch_summary.py --textfile-output /var/lib/node_exporter/textfile_collector/risk_mismatch_summary.prom
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

log = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_SECONDS = (5, 15, 30)


def _connect_with_retry(dsn: str, *, max_retries: int = _MAX_RETRIES) -> "psycopg.Connection":
    """Connect to PostgreSQL with exponential back-off on transient errors."""
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return psycopg.connect(dsn)
        except psycopg.OperationalError as exc:
            last_exc = exc
            if attempt < max_retries:
                delay = _BACKOFF_SECONDS[min(attempt, len(_BACKOFF_SECONDS) - 1)]
                log.warning(
                    'PG connect attempt %d/%d failed (%s), retrying in %ds …',
                    attempt + 1, max_retries + 1, exc, delay,
                )
                time.sleep(delay)
            else:
                log.error(
                    'PG connect failed after %d attempts: %s', max_retries + 1, exc,
                )
    raise last_exc  # type: ignore[misc]


def _write_atomic(path: Path, payload: str) -> None:
    """Write payload atomically via a temp file so readers never see a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(payload, encoding='utf-8')
    tmp.replace(path)


def render_textfile(report: dict) -> str:
    """Render a Prometheus textfile payload from a mismatch summary report dict.

    Emits:
      trade_risk_mismatch_summary_freshness_seconds – seconds since report was generated.
      trade_risk_mismatch_summary_stale             – 1 if stale, 0 if fresh.
      trade_risk_mismatch_summary_row_count         – number of rows in the summary.
      trade_risk_mismatch_summary_avg_rate{window_name,tier}       – per-window avg mismatch rate.
      trade_risk_mismatch_summary_quarantine_count{window_name,tier} – per-window quarantine count.
    """
    generated_at_ms = int(report.get('generated_at_ms') or 0)
    freshness_seconds = max(0.0, (get_ny_time_millis() - generated_at_ms) / 1000.0) if generated_at_ms else 0.0
    stale_threshold = int(report.get('freshness_stale_threshold_sec') or 1800)
    rows = list(report.get('rows') or [])
    lines = [
        '# HELP trade_risk_mismatch_summary_freshness_seconds Freshness of the latest materialized mismatch summary report.',
        '# TYPE trade_risk_mismatch_summary_freshness_seconds gauge',
        f'trade_risk_mismatch_summary_freshness_seconds {freshness_seconds}',
        '# HELP trade_risk_mismatch_summary_stale Whether the mismatch summary report is stale.',
        '# TYPE trade_risk_mismatch_summary_stale gauge',
        f"trade_risk_mismatch_summary_stale {1 if freshness_seconds > float(stale_threshold) else 0}",
        '# HELP trade_risk_mismatch_summary_row_count Number of rows emitted by risk_mismatch_summary_mv.',
        '# TYPE trade_risk_mismatch_summary_row_count gauge',
        f"trade_risk_mismatch_summary_row_count {int(report.get('row_count') or 0)}",
        '# HELP trade_risk_mismatch_summary_avg_rate Average mismatch rate aggregated from risk_mismatch_summary_mv.',
        '# TYPE trade_risk_mismatch_summary_avg_rate gauge',
        '# HELP trade_risk_mismatch_summary_quarantine_count Quarantine count aggregated from risk_mismatch_summary_mv.',
        '# TYPE trade_risk_mismatch_summary_quarantine_count gauge',
    ]
    for row in rows:
        window_name = str(row.get('window_name') or '')
        tier = str(row.get('tier') or 'UNKNOWN')
        avg_rate = float(row.get('avg_mismatch_rate') or 0.0)
        quarantine_count = int(row.get('quarantine_count') or 0)
        lines.append(f'trade_risk_mismatch_summary_avg_rate{{window_name="{window_name}",tier="{tier}"}} {avg_rate}')
        lines.append(f'trade_risk_mismatch_summary_quarantine_count{{window_name="{window_name}",tier="{tier}"}} {quarantine_count}')
    return '\n'.join(lines) + '\n'


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    parser = argparse.ArgumentParser(description='Refresh materialized risk mismatch summary.')
    parser.add_argument(
        '--dsn',
        default=os.getenv('RISK_AUDIT_SQL_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')),
        help='PostgreSQL DSN for risk_mismatch_quarantine_ledger',
    )
    parser.add_argument(
        '--out',
        default=os.getenv(
            'RISK_MISMATCH_SUMMARY_REPORT_PATH',
            '/var/lib/trade-runbook/reports/latest_risk_mismatch_summary.json',
        ),
        help='Output JSON report path (written atomically)',
    )
    parser.add_argument(
        '--textfile-output',
        default=os.getenv('RISK_MISMATCH_SUMMARY_TEXTFILE_PATH', ''),
        help='If set, also write a Prometheus textfile to this path (for node-exporter textfile_collector)',
    )
    parser.add_argument(
        '--freshness-stale-threshold-sec',
        type=int,
        default=int(os.getenv('RISK_MISMATCH_SUMMARY_STALE_SEC', '1800')),
        help='Seconds after generation before report is considered stale (default: 1800)',
    )
    args = parser.parse_args()
    if not args.dsn or psycopg is None:
        raise RuntimeError('psycopg + DSN required. Set RISK_AUDIT_SQL_DSN or EXECUTION_JOURNAL_DSN.')
    report: dict = {
        'generated_at_ms': get_ny_time_millis(),
        'rows': [],
        'freshness_stale_threshold_sec': int(args.freshness_stale_threshold_sec),
    }
    with _connect_with_retry(args.dsn) as conn:
        with conn.cursor() as cur:
            # Refresh the materialized view (non-concurrent; use CONCURRENTLY in production
            # if the unique index risk_mismatch_summary_mv_window_tier_idx is present)
            cur.execute('refresh materialized view risk_mismatch_summary_mv')
            cur.execute(
                '''
                select window_name, tier, quarantine_count, distinct_sid_count,
                       avg_repeated_count, max_repeated_count, avg_mismatch_rate,
                       latest_created_ts_ms, refreshed_ts_ms
                from risk_mismatch_summary_mv
                order by window_name, tier
                '''
            )
            cols = [d.name for d in cur.description]
            for row in cur.fetchall():
                report['rows'].append({k: row[i] for i, k in enumerate(cols)})
        conn.commit()
    report['row_count'] = len(report['rows'])
    _write_atomic(Path(args.out), json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    # P4.9: optionally export Prometheus textfile for node-exporter textfile_collector
    if args.textfile_output:
        _write_atomic(Path(args.textfile_output), render_textfile(report))
    print(json.dumps({'row_count': report['row_count']}, ensure_ascii=False))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
