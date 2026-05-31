#!/usr/bin/env python3
from __future__ import annotations

"""Refresh trade execution replay/rehydrate SLO materialized summary.

P3.3-autonomy: calls REFRESH MATERIALIZED VIEW on
``execution_replay_slo_summary_mv`` (created by the 20260306_07 migration),
then fetches the rows and writes them to
``{RUNBOOK_REPORT_DIR}/latest_replay_slo_summary.json``.

Runs under the ``trade-execution-replay-slo-refresh.timer`` every 15 min.

ENV
---
  EXECUTION_JOURNAL_DSN  – PostgreSQL DSN (required)
  RUNBOOK_REPORT_DIR     – directory for JSON output (default: /var/lib/trade-runbook/reports)
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


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    parser = argparse.ArgumentParser(
        description='Refresh execution replay/rehydrate SLO materialized summary.'
    )
    parser.add_argument('--dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument('--report-dir', default=os.getenv('RUNBOOK_REPORT_DIR', '/var/lib/trade-runbook/reports'))
    args = parser.parse_args()

    if not args.dsn or psycopg is None:
        raise RuntimeError('psycopg + EXECUTION_JOURNAL_DSN required')

    rows = []
    with _connect_with_retry(args.dsn) as conn:
        with conn.cursor() as cur:
            # Refresh materialised view (concurrent refresh not used to keep migration simple)
            cur.execute('refresh materialized view execution_replay_slo_summary_mv')
            cur.execute(
                'select window_name, rehydrate_total, rehydrate_stream_total, rehydrate_sql_total, '
                'replay_truncated_total, retention_guard_total, replay_mismatch_quarantine_total, '
                'retention_guard_quarantine_total, replay_latency_p95_ms '
                'from execution_replay_slo_summary_mv order by window_name'
            )
            for row in cur.fetchall() or []:
                rows.append({
                    'window_name': row[0],
                    'rehydrate_total': int(row[1] or 0),
                    'rehydrate_stream_total': int(row[2] or 0),
                    'rehydrate_sql_total': int(row[3] or 0),
                    'replay_truncated_total': int(row[4] or 0),
                    'retention_guard_total': int(row[5] or 0),
                    'replay_mismatch_quarantine_total': int(row[6] or 0),
                    'retention_guard_quarantine_total': int(row[7] or 0),
                    'replay_latency_p95_ms': float(row[8] or 0.0),
                })
        conn.commit()

    out = {'items': rows}
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / 'latest_replay_slo_summary.json').write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8'
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
