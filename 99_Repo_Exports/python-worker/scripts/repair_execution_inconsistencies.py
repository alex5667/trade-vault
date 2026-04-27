#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Repair SQL execution journal rows from Redis state/stream mirrors.

Purpose
-------
P7 adds a conservative repair utility that treats Redis ``orders:state:*`` and
recent ``orders:exec`` events as the operational source of truth for *current*
execution state, while SQL remains the durable mirror used by BI, dashboards,
and incident review.

The tool is intentionally narrow:
* it repairs only the SQL mirror;
* it never mutates live Binance/position state;
* it prefers Redis state snapshots over stream events when both are present;
* it records a JSON summary for auditability.

Usage
-----
Dry-run (safe, no writes):
    python scripts/repair_execution_inconsistencies.py --dry-run

Apply mode (writes to SQL mirror only):
    python scripts/repair_execution_inconsistencies.py

ENV vars consumed:
    REDIS_URL                     – redis connection (default: redis://localhost:6379/0)
    EXECUTION_JOURNAL_DSN         – postgres DSN (required)
    ORDERS_STATE_KEY_PREFIX       – Redis state key prefix (default: orders:state:)
    EXEC_STREAM                   – Redis exec stream (default: orders:exec)
    EXEC_CONSISTENCY_STREAM_COUNT – how many stream entries to scan (default: 20000)
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

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



# Fields representing protective algo order IDs stored in a separate SQL table
PROTECTION_FIELDS = (
    'sl_algo_id',
    'sl_client_algo_id',
    'tp1_algo_id',
    'tp2_algo_id',
    'tp3_algo_id',
    'trail_algo_id',
    'trail_client_algo_id',
)


def _i(v: Any, default: int = 0) -> int:
    """Safe int cast with fallback default."""
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return int(default)


def _s(v: Any) -> str:
    """Safe str cast; None → empty string."""
    return '' if v is None else str(v)


def select_best_source(
    redis_doc: Mapping[str, Any],
    stream_doc: Mapping[str, Any],
    sql_doc: Mapping[str, Any],
) -> Tuple[str, Dict[str, Any]]:
    """Choose the least lossy mirror for SQL repair.

    Preference order:
    1. Redis state snapshot (already materialized by executor)
    2. Latest stream event (for partially rolled out or freshly restarted nodes)
    3. Existing SQL row (only when the other mirrors are absent)
    """
    if redis_doc:
        return 'redis', dict(redis_doc)
    if stream_doc:
        return 'stream', dict(stream_doc)
    return 'sql', dict(sql_doc)


def build_repair_plan(
    mismatches: Iterable[consistency.ConsistencyMismatch],
    redis_state: Mapping[str, Mapping[str, Any]],
    stream_latest: Mapping[str, Mapping[str, Any]],
    sql_orders: Mapping[str, Mapping[str, Any]],
    sql_refs: Mapping[str, Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Build an ordered list of repair steps per sid.

    Each step contains:
      sid          – the affected signal id
      source       – which mirror was chosen ('redis' | 'stream' | 'sql')
      actions      – sorted list of SQL operations to perform
      source_doc   – the document that will be written to SQL
      source_ref_doc – protection-ref fields (from Redis or SQL fallback)
      categories   – set of mismatch categories involved
    """
    by_sid: Dict[str, List[consistency.ConsistencyMismatch]] = {}
    for mm in mismatches:
        by_sid.setdefault(mm.sid, []).append(mm)

    plan: List[Dict[str, Any]] = []
    for sid, items in sorted(by_sid.items()):
        redis_doc = dict(redis_state.get(sid) or {})
        stream_doc = dict(stream_latest.get(sid) or {})
        sql_doc = dict(sql_orders.get(sid) or {})
        sql_ref_doc = dict(sql_refs.get(sid) or {})
        source_name, source_doc = select_best_source(redis_doc, stream_doc, sql_doc)
        categories = {m.category for m in items}
        actions: List[str] = []
        if 'sql_missing' in categories:
            actions.append('upsert_execution_order')
        if any(c.endswith('_mismatch') for c in categories):
            actions.append('sync_execution_order')
        if any(c.startswith(f'{field}_') for c in categories for field in PROTECTION_FIELDS):
            actions.append('sync_protection_refs')
        if not actions and source_doc:
            # Presence-only issues still benefit from re-seeding SQL when it is missing.
            if not sql_doc:
                actions.append('upsert_execution_order')
        plan.append({
            'sid': sid,
            'source': source_name,
            'actions': sorted(set(actions)),
            'source_doc': source_doc,
            'source_ref_doc': redis_doc or sql_ref_doc,
            'categories': sorted(categories),
        })
    return plan


class SQLRepairWriter:
    """Applies a repair plan to the SQL execution mirror using UPSERT semantics.

    Important:
    - Never deletes rows.
    - Uses GREATEST() for updated_at_ms to avoid rolling back timestamps.
    - Uses COALESCE in refs table to keep existing non-NULL values when source is incomplete.
    """

    def __init__(self, conn: Any):
        self.conn = conn

    def apply(self, plan: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
        """Execute the plan; returns counters dict with orders_upserted/orders_synced/refs_synced."""
        counters = {
            'orders_upserted': 0,
            'orders_synced': 0,
            'refs_synced': 0,
        }
        with self.conn:
            with self.conn.cursor() as cur:
                for step in plan:
                    doc = dict(step.get('source_doc') or {})
                    ref_doc = dict(step.get('source_ref_doc') or {})
                    sid = _s(step.get('sid'))
                    actions = set(step.get('actions') or [])
                    if not sid or not actions:
                        continue
                    if 'upsert_execution_order' in actions or 'sync_execution_order' in actions:
                        # UPSERT into execution_orders; updated_at_ms uses GREATEST to be
                        # monotone even when the source doc has an older timestamp.
                        cur.execute(
                            """
                            INSERT INTO execution_orders (sid, symbol, action, status, fsm_state, execution_policy, venue, position_mode, position_side, working_type_policy, state_jsonb, created_at_ms, updated_at_ms)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)
                            ON CONFLICT (sid) DO UPDATE SET
                              symbol = EXCLUDED.symbol,
                              action = EXCLUDED.action,
                              status = EXCLUDED.status,
                              fsm_state = EXCLUDED.fsm_state,
                              execution_policy = EXCLUDED.execution_policy,
                              venue = EXCLUDED.venue,
                              position_mode = EXCLUDED.position_mode,
                              position_side = EXCLUDED.position_side,
                              working_type_policy = EXCLUDED.working_type_policy,
                              state_jsonb = EXCLUDED.state_jsonb,
                              updated_at_ms = GREATEST(execution_orders.updated_at_ms, EXCLUDED.updated_at_ms)
                            """,
                            (
                                sid,
                                _s(doc.get('symbol')),
                                _s(doc.get('action')),
                                _s(doc.get('status')),
                                _s(doc.get('fsm_state')),
                                _s(doc.get('execution_policy')),
                                _s(doc.get('venue') or 'binance'),
                                _s(doc.get('position_mode')),
                                _s(doc.get('position_side')),
                                _s(doc.get('working_type_policy')),
                                json.dumps(doc, ensure_ascii=False, default=str),
                                _i(doc.get('created_at_ms') or doc.get('ts_ms') or get_ny_time_millis()),
                                _i(doc.get('updated_at_ms') or doc.get('ts_ms') or get_ny_time_millis()),
                            ),
                        )
                        if 'upsert_execution_order' in actions:
                            counters['orders_upserted'] += 1
                        else:
                            counters['orders_synced'] += 1
                    if 'sync_protection_refs' in actions:
                        # Merge all ref fields from both source mirrors.
                        # COALESCE in SQL ensures existing non-NULL values survive partial docs.
                        src = {**doc, **ref_doc}
                        cur.execute(
                            """
                            INSERT INTO execution_protection_refs (sid, symbol, sl_algo_id, sl_client_algo_id, tp1_algo_id, tp2_algo_id, tp3_algo_id, trail_algo_id, trail_client_algo_id, updated_at_ms)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (sid) DO UPDATE SET
                              symbol = EXCLUDED.symbol,
                              sl_algo_id = COALESCE(EXCLUDED.sl_algo_id, execution_protection_refs.sl_algo_id),
                              sl_client_algo_id = COALESCE(NULLIF(EXCLUDED.sl_client_algo_id, ''), execution_protection_refs.sl_client_algo_id),
                              tp1_algo_id = COALESCE(EXCLUDED.tp1_algo_id, execution_protection_refs.tp1_algo_id),
                              tp2_algo_id = COALESCE(EXCLUDED.tp2_algo_id, execution_protection_refs.tp2_algo_id),
                              tp3_algo_id = COALESCE(EXCLUDED.tp3_algo_id, execution_protection_refs.tp3_algo_id),
                              trail_algo_id = COALESCE(EXCLUDED.trail_algo_id, execution_protection_refs.trail_algo_id),
                              trail_client_algo_id = COALESCE(NULLIF(EXCLUDED.trail_client_algo_id, ''), execution_protection_refs.trail_client_algo_id),
                              updated_at_ms = GREATEST(execution_protection_refs.updated_at_ms, EXCLUDED.updated_at_ms)
                            """,
                            (
                                sid,
                                _s(src.get('symbol')),
                                _i(src.get('sl_algo_id'), 0) or None,
                                _s(src.get('sl_client_algo_id')),
                                _i(src.get('tp1_algo_id'), 0) or None,
                                _i(src.get('tp2_algo_id'), 0) or None,
                                _i(src.get('tp3_algo_id'), 0) or None,
                                _i(src.get('trail_algo_id'), 0) or None,
                                _s(src.get('trail_client_algo_id')),
                                _i(src.get('updated_at_ms') or src.get('ts_ms') or get_ny_time_millis()),
                            ),
                        )
                        counters['refs_synced'] += 1
        return counters


def run_repair(
    *,
    redis_url: str,
    journal_dsn: str,
    state_prefix: str,
    exec_stream: str,
    stream_count: int,
    dry_run: bool = False,
    ledger_dsn: str = '',
) -> Dict[str, Any]:
    """Orchestrate the full repair flow: read mirrors → diff → plan → (optionally) apply."""
    import redis as redislib  # type: ignore

    redis_client = redislib.from_url(redis_url, decode_responses=True)
    reader = consistency.RedisExecutionReader(redis_client, state_prefix, exec_stream)
    redis_state = {sid: doc for sid, doc in reader.iter_state()}
    stream_latest = reader.latest_stream_events(count=stream_count)

    sql_conn = consistency._connect_pg(journal_dsn)
    sql_reader = consistency.SQLExecutionReader(sql_conn)
    sql_orders = sql_reader.load_orders()
    sql_refs = sql_reader.load_protection_refs()

    mismatches, _, _ = consistency.compare_execution_views(redis_state, stream_latest, sql_orders, sql_refs)
    plan = build_repair_plan(mismatches, redis_state, stream_latest, sql_orders, sql_refs)
    result: Dict[str, Any] = {
        'checked_at_ms': get_ny_time_millis(),
        'mismatches_total': len(mismatches),
        'repair_steps_total': len([p for p in plan if p.get('actions')]),
        'plan': plan,
        'applied': False,
        'counters': {'orders_upserted': 0, 'orders_synced': 0, 'refs_synced': 0},
    }
    if not dry_run:
        writer = SQLRepairWriter(sql_conn)
        result['counters'] = writer.apply(plan)
        result['applied'] = True
    if ledger_dsn and QuarantineLedgerSink is not None:
        now_ms = get_ny_time_millis()
        QuarantineLedgerSink(dsn=ledger_dsn).record_repair_run({
            'run_kind': 'manual_repair',
            'source': 'repair_execution_inconsistencies',
            'status': 'dry_run' if dry_run else 'applied',
            'summary': result,
            'started_at_ms': int(result.get('checked_at_ms') or now_ms),
            'finished_at_ms': now_ms,
        })
    return result


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description='Repair SQL execution journal from Redis mirrors.')
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--journal-dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument('--state-prefix', default=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'))
    parser.add_argument('--exec-stream', default=os.getenv('EXEC_STREAM', 'orders:exec'))
    parser.add_argument('--stream-count', type=int, default=int(os.getenv('EXEC_CONSISTENCY_STREAM_COUNT', '20000')))
    parser.add_argument('--dry-run', action='store_true', help='Plan only – do not write to SQL')
    parser.add_argument('--ledger-dsn', default=os.getenv('EXECUTION_QUARANTINE_LEDGER_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')))
    args = parser.parse_args(argv)

    if not args.journal_dsn:
        raise SystemExit('EXECUTION_JOURNAL_DSN/--journal-dsn is required')

    result = run_repair(
        redis_url=args.redis_url,
        journal_dsn=args.journal_dsn,
        state_prefix=args.state_prefix,
        exec_stream=args.exec_stream,
        stream_count=args.stream_count,
        dry_run=args.dry_run,
        ledger_dsn=args.ledger_dsn,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
