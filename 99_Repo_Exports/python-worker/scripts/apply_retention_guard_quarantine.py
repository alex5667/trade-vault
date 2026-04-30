#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Retention-guard quarantine policy for P3.3-autonomy.

Identifies SIDs whose replay checkpoint has fallen behind the Redis stream
retention window (detected by stream_retention_guard_report) and for which
stream-only rebuild no longer returns events.  Those SIDs are quarantined:

  - Redis key  ``{quarantine_prefix}{sid}`` – full state JSON
  - Redis set  ``{quarantine_prefix}sids``  – membership set
  - Redis stream ``{quarantine_prefix}events`` – audit trail
  - SQL ledger ``execution_quarantine_ledger`` – persistent record

SIDs that can still be recovered from the stream are skipped.

ENV
---
  REDIS_URL                          (default redis://localhost:6379/0)
  EXEC_STREAM                        (default orders:exec)
  EXEC_REPLAY_CHECKPOINT_KEY_PREFIX  (default orders:exec:replay:cursor:)
  ORDERS_STATE_KEY_PREFIX            (default orders:state:)
  EXECUTION_JOURNAL_DSN              (optional SQL journal/fallback DSN)
  ORDERS_QUARANTINE_PREFIX           (default orders:quarantine:state:)
  EXECUTION_QUARANTINE_LEDGER_DSN    (falls back to EXECUTION_JOURNAL_DSN)
  EXEC_REPLAY_RETENTION_GUARD_SAMPLE_LIMIT (default 2000)
  EXEC_REPLAY_SCAN_COUNT             (default 20000)
"""

import argparse
import json
import os
import time
from typing import Any, Dict, List

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from services.execution_state_replay import stream_retention_guard_report, rebuild_state_with_fallback
except Exception:  # pragma: no cover
    try:
        from binance_execution.execution_state_replay import stream_retention_guard_report, rebuild_state_with_fallback  # type: ignore
    except Exception:
        from execution_state_replay import stream_retention_guard_report, rebuild_state_with_fallback  # type: ignore

try:
    from services.quarantine_ledger import QuarantineLedgerSink
except Exception:  # pragma: no cover
    try:
        from binance_execution.quarantine_ledger import QuarantineLedgerSink  # type: ignore
    except Exception:
        from quarantine_ledger import QuarantineLedgerSink  # type: ignore


def run_policy(
    redis_client: Any
    *
    exec_stream: str
    checkpoint_prefix: str
    state_prefix: str
    journal_dsn: str
    quarantine_prefix: str
    ledger_dsn: str
    sample_limit: int
    scan_count: int
    dry_run: bool
) -> Dict[str, Any]:
    """Apply quarantine policy for breached retention guards.

    For each SID in the breach list:
    - If stream rebuild succeeds → skip (sid can still be recovered)
    - Otherwise → quarantine the SID in Redis and record in SQL ledger

    Returns a summary dict with checked/breached/quarantined counts and items list.
    """
    cprefix = checkpoint_prefix.rstrip(':') + ':'
    qprefix = quarantine_prefix.rstrip(':') + ':'
    ledger = QuarantineLedgerSink(dsn=ledger_dsn) if ledger_dsn else None

    guard = stream_retention_guard_report(
        redis_client
        exec_stream=exec_stream
        checkpoint_prefix=cprefix
        sample_limit=sample_limit
    )

    items: List[Dict[str, Any]] = []
    quarantined = 0

    for ex in list(guard.get('breached_examples') or []):
        sid = str(ex.get('sid') or '')
        checkpoint_id = str(ex.get('checkpoint_id') or '')

        # Attempt stream-only rebuild; if it succeeds the SID is still recoverable
        result = rebuild_state_with_fallback(
            redis_client
            exec_stream=exec_stream
            sid=sid
            scan_count=scan_count
            checkpoint_id=checkpoint_id
            sql_dsn=journal_dsn
        )
        if result.source == 'stream' and result.state_doc:
            items.append({'sid': sid, 'action': 'skip_stream_recovered'})
            continue

        # Compose the quarantine payload from existing state (if any)
        state_doc = dict(result.state_doc or {})
        payload = dict(state_doc)
        payload.update({
            'sid': sid
            'quarantine_reason': 'retention_guard_breach'
            'retention_guard': ex
            'rehydrate_source': result.source
        })

        items.append({'sid': sid, 'action': 'quarantine', 'source': result.source})

        if not dry_run:
            # Write quarantine state to Redis
            redis_client.set(f'{qprefix}{sid}', json.dumps(payload, ensure_ascii=False, default=str))
            redis_client.sadd(f'{qprefix}sids', sid)
            # Append to the quarantine audit stream (RETENTION_GUARD_QUARANTINED)
            redis_client.xadd(
                f'{qprefix}events'
                {'sid': sid, 'event': 'RETENTION_GUARD_QUARANTINED', 'ts_ms': str(get_ny_time_millis())}
                maxlen=10000
                approximate=True
            )
            # Record in SQL ledger if available
            if ledger is not None:
                try:
                    ledger.record_quarantine_event({
                        'sid': sid
                        'symbol': str(payload.get('symbol') or '')
                        'action': 'RETENTION_GUARD_QUARANTINED'
                        'severity': 'critical'
                        'reason': 'retention_guard_breach'
                        'source': 'retention_guard_policy'
                        'quarantine_key': f'{qprefix}{sid}'
                        'state': payload
                    })
                except Exception:
                    pass  # ledger failure is non-fatal; Redis event is the primary record
            quarantined += 1

    return {
        'checked': int(guard.get('checked_checkpoint_keys') or 0)
        'breached': int(guard.get('breached_checkpoints') or 0)
        'quarantined': quarantined
        'items': items
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Quarantine SIDs whose replay checkpoint fell behind Redis stream retention.'
    )
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--exec-stream', default=os.getenv('EXEC_STREAM', 'orders:exec'))
    parser.add_argument('--checkpoint-prefix', default=os.getenv('EXEC_REPLAY_CHECKPOINT_KEY_PREFIX', 'orders:exec:replay:cursor:'))
    parser.add_argument('--state-prefix', default=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'))
    parser.add_argument('--journal-dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument('--quarantine-prefix', default=os.getenv('ORDERS_QUARANTINE_PREFIX', 'orders:quarantine:state:'))
    parser.add_argument('--ledger-dsn', default=os.getenv('EXECUTION_QUARANTINE_LEDGER_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')))
    parser.add_argument('--sample-limit', type=int, default=int(os.getenv('EXEC_REPLAY_RETENTION_GUARD_SAMPLE_LIMIT', '2000')))
    parser.add_argument('--scan-count', type=int, default=int(os.getenv('EXEC_REPLAY_SCAN_COUNT', '20000')))
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if redis is None:
        raise RuntimeError('redis package required')
    r = redis.from_url(args.redis_url, decode_responses=True)
    report = run_policy(
        r
        exec_stream=args.exec_stream
        checkpoint_prefix=args.checkpoint_prefix
        state_prefix=args.state_prefix
        journal_dsn=args.journal_dsn
        quarantine_prefix=args.quarantine_prefix
        ledger_dsn=args.ledger_dsn
        sample_limit=args.sample_limit
        scan_count=args.scan_count
        dry_run=bool(args.dry_run)
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
