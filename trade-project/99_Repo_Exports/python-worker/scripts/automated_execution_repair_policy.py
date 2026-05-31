#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""Automated repair/quarantine policy for execution mirrors.

Flow
----
1. Run consistency check.
2. If critical mismatches are within the configured budget, repair SQL mirror.
3. Re-run consistency check.
4. Quarantine remaining critical sid values.
5. Mirror run summary into SQL quarantine ledger.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import check_execution_consistency as consistency
import quarantine_inconsistent_sid as quarantine_mod
import repair_execution_inconsistencies as repair_mod

try:
    from binance_execution.quarantine_ledger import QuarantineLedgerSink
except Exception:
    try:
        from quarantine_ledger import QuarantineLedgerSink  # type: ignore
    except Exception:  # pragma: no cover
        QuarantineLedgerSink = None  # type: ignore


def run_policy(*, redis_url: str, journal_dsn: str, state_prefix: str, exec_stream: str, stream_count: int, max_auto_repair_critical: int, quarantine_min_severity: str, dry_run: bool = False, ledger_dsn: str = '') -> dict[str, Any]:
    started_at_ms = get_ny_time_millis()
    before = consistency.run_check(redis_url=redis_url, journal_dsn=journal_dsn, state_prefix=state_prefix, exec_stream=exec_stream, stream_count=stream_count)
    repaired = None
    if before.critical_mismatches <= int(max_auto_repair_critical):
        repaired = repair_mod.run_repair(
            redis_url=redis_url,
            journal_dsn=journal_dsn,
            state_prefix=state_prefix,
            exec_stream=exec_stream,
            stream_count=stream_count,
            dry_run=dry_run,
            ledger_dsn=ledger_dsn,
        )
    after = consistency.run_check(redis_url=redis_url, journal_dsn=journal_dsn, state_prefix=state_prefix, exec_stream=exec_stream, stream_count=stream_count)
    mismatches = [consistency.ConsistencyMismatch(**m) for m in after.mismatches]
    targets = quarantine_mod.build_quarantine_targets(mismatches, severity=quarantine_min_severity)
    quarantine_results: list[dict[str, Any]] = []
    if targets:
        import redis  # type: ignore
        r = redis.from_url(redis_url, decode_responses=True)
        ledger = QuarantineLedgerSink(dsn=ledger_dsn) if QuarantineLedgerSink and ledger_dsn else None
        for item in targets:
            quarantine_results.append(quarantine_mod.quarantine_sid(
                r,
                item['sid'],
                state_prefix=state_prefix,
                quarantine_prefix=os.getenv('ORDERS_QUARANTINE_PREFIX', 'orders:quarantine:state:'),
                reason=item['reason'],
                severity=str(item.get('severity') or quarantine_min_severity),
                dry_run=dry_run,
                ledger=ledger,
                source='automated_repair_policy',
            ))
    summary = {
        'before': before.to_dict(),
        'repaired': repaired,
        'after': after.to_dict(),
        'quarantine_results': quarantine_results,
    }
    finished_at_ms = get_ny_time_millis()
    if ledger_dsn and QuarantineLedgerSink is not None:
        QuarantineLedgerSink(dsn=ledger_dsn).record_repair_run({
            'run_kind': 'automated_repair_policy',
            'source': 'automated_execution_repair_policy',
            'status': 'dry_run' if dry_run else 'applied',
            'summary': summary,
            'started_at_ms': started_at_ms,
            'finished_at_ms': finished_at_ms,
        })
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Run automated repair/quarantine policy for execution mirrors.')
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--journal-dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument('--ledger-dsn', default=os.getenv('EXECUTION_QUARANTINE_LEDGER_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')))
    parser.add_argument('--state-prefix', default=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'))
    parser.add_argument('--exec-stream', default=os.getenv('EXEC_STREAM', RS.ORDERS_EXEC))
    parser.add_argument('--stream-count', type=int, default=int(os.getenv('EXEC_CONSISTENCY_STREAM_COUNT', '20000')))
    parser.add_argument('--max-auto-repair-critical', type=int, default=int(os.getenv('EXEC_AUTO_REPAIR_MAX_CRITICAL', '25')))
    parser.add_argument('--quarantine-min-severity', default=os.getenv('EXEC_QUARANTINE_MIN_SEVERITY', 'critical'))
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    if not args.journal_dsn:
        raise SystemExit('EXECUTION_JOURNAL_DSN/--journal-dsn is required')
    print(json.dumps(run_policy(
        redis_url=args.redis_url,
        journal_dsn=args.journal_dsn,
        state_prefix=args.state_prefix,
        exec_stream=args.exec_stream,
        stream_count=args.stream_count,
        max_auto_repair_critical=args.max_auto_repair_critical,
        quarantine_min_severity=args.quarantine_min_severity,
        dry_run=args.dry_run,
        ledger_dsn=args.ledger_dsn,
    ), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
