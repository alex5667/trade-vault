#!/usr/bin/env python3
from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""Compare Redis ``orders:state:*`` materialized views against state replayed from ``orders:exec``.

Scans every ``orders:state:{sid}`` key in Redis, re-runs the stream replay for
that SID and reports field-level mismatches for the key execution fields.

**Performance optimizations (capacity-audit follow-up):**
- ``--batch-size N`` limits the number of SIDs processed per run (default: 100).
  A cursor file tracks progress so each hourly run processes the next batch.
- ``--max-runtime-sec`` hard-caps wall-clock time per run (default: 300s / 5 min).
  Previously the script could run 37+ min at 49% CPU with many SIDs.

P3.3-ops-complete additions:
- --quarantine-on-critical writes a QuarantineLedger event in addition to Redis quarantine
- mismatch report now includes retention_guard_triggered and replay_latency_ms per SID
- --ledger-dsn flag (or EXECUTION_QUARANTINE_LEDGER_DSN env) to specify ledger DB

Exit codes:
  0 — no mismatches (or fewer than --critical-threshold)
  2 — mismatches >= critical-threshold

Usage
-----
  python3 scripts/check_execution_replay_consistency.py
  python3 scripts/check_execution_replay_consistency.py --batch-size 50 --max-runtime-sec 120
  python3 scripts/check_execution_replay_consistency.py --critical-threshold 5

ENV
---
  REDIS_URL                    (default redis://localhost:6379/0)
  EXEC_STREAM                  (default orders:exec)
  ORDERS_STATE_KEY_PREFIX      (default orders:state:)
  EXEC_REPLAY_SCAN_COUNT       (default 20000)
  EXEC_REPLAY_CRITICAL_THRESHOLD (default 1)
  EXEC_REPLAY_BATCH_SIZE       (default 100)
  EXEC_REPLAY_MAX_RUNTIME_SEC  (default 300)
  EXECUTION_QUARANTINE_LEDGER_DSN (or EXECUTION_JOURNAL_DSN for fallback)
"""

import argparse
import json
import os
import time
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from services.execution_state_replay import compare_replayed_state, rebuild_state_with_fallback
except Exception:  # pragma: no cover
    try:
        from binance_execution.execution_state_replay import (  # type: ignore
            compare_replayed_state,
            rebuild_state_with_fallback,
        )
    except Exception:
        from execution_state_replay import compare_replayed_state, rebuild_state_with_fallback  # type: ignore


# P3.3-ops-complete: QuarantineLedger for structured quarantine records
try:
    from services.quarantine_ledger import QuarantineLedgerSink
except Exception:  # pragma: no cover
    try:
        from binance_execution.quarantine_ledger import QuarantineLedgerSink  # type: ignore
    except Exception:
        from quarantine_ledger import QuarantineLedgerSink  # type: ignore


# ---------------------------------------------------------------------------
# Cursor persistence — remembers where we stopped so the next run picks up
# ---------------------------------------------------------------------------
_CURSOR_PATH = '/tmp/exec_replay_consistency_cursor.txt'


def _load_cursor() -> str:
    """Load the Redis SCAN cursor from disk (default '0' = start from beginning)."""
    try:
        with open(_CURSOR_PATH) as f:
            return f.read().strip() or '0'
    except FileNotFoundError:
        return '0'


def _save_cursor(cursor: str) -> None:
    """Persist Redis SCAN cursor for the next run."""
    try:
        with open(_CURSOR_PATH, 'w') as f:
            f.write(cursor)
    except OSError:
        pass  # Non-critical — next run will start from 0


def _loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')
    try:
        obj = json.loads(value)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _quarantine(
    redis_client: Any,
    *,
    sid: str,
    mismatch: dict[str, Any],
    state_doc: dict[str, Any],
    prefix: str,
    ledger: Any = None,
) -> None:
    """Write Redis quarantine entries and optionally the SQL ledger.

    P3.3-ops-complete: ledger parameter now accepted; writes structured event
    when a QuarantineLedgerSink is passed.
    """
    qprefix = prefix.rstrip(':') + ':'
    payload = dict(state_doc or {})
    payload.update({'sid': sid, 'quarantine_reason': 'replay_mismatch', 'replay_mismatch': mismatch})
    pipe = redis_client.pipeline()
    pipe.set(f'{qprefix}{sid}', json.dumps(payload, ensure_ascii=False, default=str))
    pipe.sadd(f'{qprefix}sids', sid)
    pipe.xadd(f'{qprefix}events', {'sid': sid, 'event': 'REPLAY_MISMATCH_QUARANTINED'}, maxlen=10000, approximate=True)
    pipe.execute()
    try:
        if ledger is not None:
            ledger.record_quarantine_event({
                'sid': sid,
                'symbol': str((state_doc or {}).get('symbol') or ''),
                'action': 'REPLAY_MISMATCH_QUARANTINED',
                'severity': 'critical' if any(k in {'status', 'fsm_state'} for k in mismatch) else 'warning',
                'reason': 'replay_mismatch',
                'source': 'replay_consistency_checker',
                'quarantine_key': f'{qprefix}{sid}',
                'state': payload,
            })
    except Exception:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description='Compare Redis materialized state keys against state replayed from orders:exec.')
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--exec-stream', default=os.getenv('EXEC_STREAM', RS.ORDERS_EXEC))
    parser.add_argument('--state-prefix', default=(os.getenv('ORDERS_STATE_KEY_PREFIX') or 'orders:state:'))
    parser.add_argument('--checkpoint-prefix', default=os.getenv('EXEC_REPLAY_CHECKPOINT_KEY_PREFIX', 'orders:exec:replay:cursor:'))
    parser.add_argument('--scan-count', type=int, default=int(os.getenv('EXEC_REPLAY_SCAN_COUNT', '20000')))
    parser.add_argument('--critical-threshold', type=int, default=int(os.getenv('EXEC_REPLAY_CRITICAL_THRESHOLD', '1')))
    parser.add_argument('--quarantine-on-critical', action='store_true')
    parser.add_argument('--quarantine-prefix', default=os.getenv('ORDERS_QUARANTINE_PREFIX', 'orders:quarantine:state:'))
    # P3.3-ops-complete: ledger DSN for structured event recording
    parser.add_argument('--ledger-dsn', default=os.getenv('EXECUTION_QUARANTINE_LEDGER_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')))
    # Performance: batch size and max runtime
    parser.add_argument('--batch-size', type=int,
                        default=int(os.getenv('EXEC_REPLAY_BATCH_SIZE', '100')),
                        help='Max SIDs to check per run (default 100)')
    parser.add_argument('--max-runtime-sec', type=int,
                        default=int(os.getenv('EXEC_REPLAY_MAX_RUNTIME_SEC', '300')),
                        help='Hard cap on wall-clock time per run in seconds (default 300)')
    args = parser.parse_args()
    if redis is None:
        raise RuntimeError('redis package required')
    r = redis.from_url(args.redis_url, decode_responses=True)
    # P3.3-ops-complete: build ledger if DSN provided
    ledger = QuarantineLedgerSink(dsn=args.ledger_dsn) if args.ledger_dsn else None
    prefix = args.state_prefix.rstrip(':') + ':'
    cprefix = args.checkpoint_prefix.rstrip(':') + ':'
    mismatches: list[dict[str, Any]] = []

    # ---------------------------------------------------------------------------
    # Batched scanning: resume from stored cursor, process up to batch_size SIDs
    # ---------------------------------------------------------------------------
    start_time = time.monotonic()
    max_runtime = args.max_runtime_sec
    batch_limit = args.batch_size
    processed = 0
    skipped_by_time = False

    cursor = _load_cursor()
    scan_match = f'{prefix}*'

    # Use manual SCAN instead of scan_iter to control cursor persistence
    while True:
        # Time guard
        elapsed = time.monotonic() - start_time
        if elapsed >= max_runtime:
            skipped_by_time = True
            print(f'[p33-consistency] Time limit reached ({max_runtime}s), stopping after {processed} SIDs')
            break

        # Batch guard
        if processed >= batch_limit:
            print(f'[p33-consistency] Batch limit reached ({batch_limit}), stopping')
            break

        # SCAN one page
        cursor, keys = r.scan(cursor=cursor, match=scan_match, count=50)

        for key in keys:
            # Re-check limits inside loop
            if processed >= batch_limit:
                break
            if time.monotonic() - start_time >= max_runtime:
                skipped_by_time = True
                break

            processed += 1
            sid = str(key).split(prefix, 1)[-1]
            redis_state = _loads(r.get(key))
            checkpoint_id = (r.get(f'{cprefix}{sid}') or '')
            replayed = rebuild_state_with_fallback(
                r,
                exec_stream=args.exec_stream,
                sid=sid,
                scan_count=args.scan_count,
                checkpoint_id=checkpoint_id,
            )
            diff = compare_replayed_state(redis_state, replayed.state_doc)
            if diff:
                severity = 'critical' if any(k in {'status', 'fsm_state'} for k in diff) else 'warning'
                item = {
                    'sid': sid,
                    'severity': severity,
                    'mismatches': diff,
                    'fsm_state': replayed.state_doc.get('fsm_state'),
                    'replay_source': replayed.source,
                    'replay_truncated': bool(replayed.truncated),
                    'checkpoint_id': replayed.checkpoint_id,
                    # P3.3-ops-complete: retention guard + latency fields
                    'retention_guard_triggered': bool(replayed.retention_guard_triggered),
                    'replay_latency_ms': int(replayed.latency_ms),
                }
                mismatches.append(item)
                if severity == 'critical' and args.quarantine_on_critical:
                    _quarantine(r, sid=sid, mismatch=diff, state_doc=replayed.state_doc, prefix=args.quarantine_prefix, ledger=ledger)

        # If cursor returned to 0, we've scanned all keys — reset
        if str(cursor) == '0':
            break

    # Persist cursor for next run
    _save_cursor(str(cursor))

    elapsed_total = time.monotonic() - start_time
    out = {
        'checked_state_keys': processed,
        'mismatches_total': len(mismatches),
        'critical_threshold': args.critical_threshold,
        'elapsed_sec': round(elapsed_total, 1),
        'batch_limit': batch_limit,
        'cursor_completed': str(cursor) == '0',
        'stopped_by_time': skipped_by_time,
        'items': mismatches,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    crit = sum(1 for x in mismatches if x['severity'] == 'critical')
    return 2 if crit >= args.critical_threshold else 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
