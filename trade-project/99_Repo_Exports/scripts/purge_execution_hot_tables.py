#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Archive and purge hot execution/quarantine rows older than retention window.'
    )
    parser.add_argument('--dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument(
        '--retention-days',
        type=int,
        default=int(os.getenv('EXECUTION_RETENTION_DAYS', '14')),
    )
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if not args.dsn or psycopg is None:
        raise RuntimeError('psycopg + EXECUTION_JOURNAL_DSN required')

    cutoff_ms = int((time.time() - args.retention_days * 86400) * 1000)
    out: dict = {'cutoff_ts_ms': cutoff_ms, 'dry_run': bool(args.dry_run)}

    with psycopg.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            if args.dry_run:
                cur.execute(
                    'select count(*) from execution_order_events where event_ts_ms < %s', (cutoff_ms,)
                )
                out['events_to_purge'] = int(cur.fetchone()[0])
                cur.execute(
                    'select count(*) from execution_quarantine_ledger where created_at_ms < %s', (cutoff_ms,)
                )
                out['quarantine_to_purge'] = int(cur.fetchone()[0])
            else:
                cur.execute('select * from purge_execution_hot_tables(%s)', (cutoff_ms,))
                row = cur.fetchone() or (0, 0)
                out['purged_events'] = int(row[0])
                out['purged_quarantine'] = int(row[1])
        if not args.dry_run:
            conn.commit()

    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
