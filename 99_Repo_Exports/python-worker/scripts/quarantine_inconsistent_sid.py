#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Soft-quarantine inconsistent execution state in Redis.

The tool does not cancel positions or touch Binance. It marks suspicious `sid`
values so the rest of the stack can stop acting on them while operators review
runbooks and reports.

What it does
------------
1. Runs the SQL/Redis consistency check.
2. Collects sids with mismatches at or above the threshold severity.
3. For each sid, copies the current state into ``orders:quarantine:state:<sid>``,
   adds the sid to the ``orders:quarantine:state:sids`` set, and emits an event
   into the ``orders:quarantine:state:events`` stream.

What it does NOT do
-------------------
* Does not cancel any open orders on Binance.
* Does not close any positions.
* Does not mutate the live ``orders:state:<sid>`` key.
* Does not write to SQL (use repair_execution_inconsistencies.py for that).

Usage
-----
Dry-run (no writes):
    python scripts/quarantine_inconsistent_sid.py --dry-run

Apply (critical sids only, default):
    python scripts/quarantine_inconsistent_sid.py --severity critical

Apply (warning+ sids):
    python scripts/quarantine_inconsistent_sid.py --severity warning

ENV vars consumed:
    REDIS_URL                     – redis connection (default: redis://localhost:6379/0)
    EXECUTION_JOURNAL_DSN         – postgres DSN (required)
    ORDERS_STATE_KEY_PREFIX       – state key prefix (default: orders:state:)
    ORDERS_QUARANTINE_PREFIX      – quarantine key prefix (default: orders:quarantine:state:)
    EXEC_STREAM                   – exec events stream (default: orders:exec)
    EXEC_CONSISTENCY_STREAM_COUNT – stream scan window (default: 20000)
    EXEC_QUARANTINE_MIN_SEVERITY  – minimum severity to quarantine (default: critical)
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

# Allow direct execution without installing the package
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import check_execution_consistency as consistency

try:
    from binance_execution.quarantine_ledger import QuarantineLedgerSink
except Exception:
    try:
        from quarantine_ledger import QuarantineLedgerSink  # type: ignore
    except Exception:  # pragma: no cover
        QuarantineLedgerSink = None  # type: ignore



def _s(v: Any) -> str:
    """Safe str cast; None → empty string."""
    return '' if v is None else str(v)


def build_quarantine_targets(
    mismatches: Iterable[consistency.ConsistencyMismatch],
    *,
    severity: str = 'critical',
) -> List[Dict[str, Any]]:
    """Group mismatches by sid and filter by minimum severity.

    Returns list of dicts: {sid, reason, severity}
    sorted by sid for deterministic output.
    """
    # Numeric severity order – higher = more severe
    order = {'critical': 2, 'warning': 1}
    min_sev = order.get(severity, 2)
    by_sid: Dict[str, List[consistency.ConsistencyMismatch]] = {}
    for mm in mismatches:
        if order.get(mm.severity, 0) >= min_sev:
            by_sid.setdefault(mm.sid, []).append(mm)
    out: List[Dict[str, Any]] = []
    for sid, items in sorted(by_sid.items()):
        out.append({
            'sid': sid,
            # Combine all distinct mismatch categories into one reason string
            'reason': '; '.join(sorted({m.category for m in items})),
            'severity': max((m.severity for m in items), key=lambda s: order.get(s, 0)),
        })
    return out


def quarantine_sid(
    redis_client: Any,
    sid: str,
    *,
    state_prefix: str,
    quarantine_prefix: str,
    reason: str,
    severity: str = 'critical',
    source: str = 'consistency_checker',
    dry_run: bool = False,
    ledger: Any = None,
) -> Dict[str, Any]:
    """Soft-quarantine a single sid in Redis using a pipeline for atomicity.

    Steps (when not dry_run):
    1. GET the current orders:state:<sid> value.
    2. Annotate the copy with quarantine metadata.
    3. PIPELINE: SET quarantine key, SADD sids set, XADD events stream.

    Returns a result dict with 'applied' flag and key names.
    """
    now_ms = get_ny_time_millis()
    state_key = f'{state_prefix}{sid}'
    quarantine_key = f'{quarantine_prefix}{sid}'
    existing = redis_client.get(state_key)
    state_doc: Dict[str, Any]
    try:
        state_doc = json.loads(existing) if existing else {}
        if not isinstance(state_doc, dict):
            state_doc = {}
    except Exception:
        state_doc = {}
    # Annotate the snapshot copy with quarantine metadata
    state_doc.setdefault('sid', sid)
    state_doc['quarantined_at_ms'] = now_ms
    state_doc['quarantine_reason'] = reason
    state_doc['quarantine_source'] = source
    result = {
        'sid': sid,
        'symbol': _s(state_doc.get('symbol')),
        'state_key': state_key,
        'quarantine_key': quarantine_key,
        'reason': reason,
        'severity': severity,
        'applied': not dry_run,
    }
    if dry_run:
        return result
    # Atomic pipeline: copy → mark set → emit event
    payload = json.dumps(state_doc, ensure_ascii=False, default=str)
    pipe = redis_client.pipeline()
    pipe.set(quarantine_key, payload)
    pipe.sadd(f'{quarantine_prefix}sids', sid)
    # Cap the event stream to 10 000 entries to bound memory
    pipe.xadd(
        f'{quarantine_prefix}events',
        {'sid': sid, 'reason': reason, 'ts_ms': now_ms, 'source': source},
        maxlen=10000,
        approximate=True,
    )
    pipe.execute()
    if ledger is not None:
        ledger.record_quarantine_event({
            'sid': sid,
            'symbol': _s(state_doc.get('symbol')),
            'action': 'QUARANTINED',
            'severity': severity,
            'reason': reason,
            'source': source,
            'quarantine_key': quarantine_key,
            'applied': True,
            'state': state_doc,
            'event_ts_ms': now_ms,
            'created_at_ms': now_ms,
        })
    return result


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description='Soft-quarantine inconsistent sid values in Redis.')
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--journal-dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument('--state-prefix', default=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'))
    parser.add_argument('--exec-stream', default=os.getenv('EXEC_STREAM', 'orders:exec'))
    parser.add_argument('--stream-count', type=int, default=int(os.getenv('EXEC_CONSISTENCY_STREAM_COUNT', '20000')))
    parser.add_argument('--quarantine-prefix', default=os.getenv('ORDERS_QUARANTINE_PREFIX', 'orders:quarantine:state:'))
    parser.add_argument('--severity', default=os.getenv('EXEC_QUARANTINE_MIN_SEVERITY', 'critical'),
                        choices=['critical', 'warning'], help='Minimum mismatch severity to quarantine')
    parser.add_argument('--ledger-dsn', default=os.getenv('EXECUTION_QUARANTINE_LEDGER_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')))
    parser.add_argument('--dry-run', action='store_true', help='Plan only – do not write to Redis')
    args = parser.parse_args(argv)

    if not args.journal_dsn:
        raise SystemExit('EXECUTION_JOURNAL_DSN/--journal-dsn is required')

    # Run consistency check to find mismatching sids
    summary = consistency.run_check(
        redis_url=args.redis_url,
        journal_dsn=args.journal_dsn,
        state_prefix=args.state_prefix,
        exec_stream=args.exec_stream,
        stream_count=args.stream_count,
    )
    mismatches = [consistency.ConsistencyMismatch(**m) for m in summary.mismatches]
    targets = build_quarantine_targets(mismatches, severity=args.severity)

    import redis  # type: ignore
    redis_client = redis.from_url(args.redis_url, decode_responses=True)
    ledger = QuarantineLedgerSink(dsn=args.ledger_dsn) if QuarantineLedgerSink and args.ledger_dsn else None
    results = [
        quarantine_sid(
            redis_client,
            item['sid'],
            state_prefix=args.state_prefix,
            quarantine_prefix=args.quarantine_prefix,
            reason=item['reason'],
            severity=str(item.get('severity') or args.severity),
            dry_run=args.dry_run,
            ledger=ledger,
        )
        for item in targets
    ]
    print(json.dumps({'targets': targets, 'results': results}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
