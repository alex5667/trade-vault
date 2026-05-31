from __future__ import annotations

"""Unit tests for repair_execution_inconsistencies.py.

Loads the script directly without import so it can run from any directory.
All external I/O (Redis, Postgres) is replaced with simple fakes.
"""

import importlib.util
import json
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent.parent / 'scripts' / 'repair_execution_inconsistencies.py'
SPEC = importlib.util.spec_from_file_location('repair_execution_inconsistencies', SCRIPT)
mod = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


class _FakeCursor:
    """Fake psycopg2 cursor that records executed statements."""

    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.sink.append((sql, params))


class _FakeConn:
    """Fake psycopg2 connection that records executed statements."""

    def __init__(self):
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeCursor(self.statements)


def _make_mismatch(sid, severity='critical', category='sql_missing', detail='missing'):
    """Helper to create a ConsistencyMismatch using the script's imported module."""
    return mod.consistency.ConsistencyMismatch(sid, severity, category, detail)


def test_build_repair_plan_prefers_redis_and_requests_upsert():
    """Redis state present → source=redis, upsert_execution_order action."""
    mismatches = [_make_mismatch('sid-1', 'critical', 'sql_missing', 'missing')]
    plan = mod.build_repair_plan(
        mismatches,
        # redis_state: sid-1 present with full doc
        {'sid-1': {'sid': 'sid-1', 'symbol': 'BTCUSDT', 'status': 'open', 'fsm_state': 'PROTECTED'}},
        {},  # stream_latest: empty
        {},  # sql_orders: empty (simulates missing row)
        {},  # sql_refs: empty
    )
    assert len(plan) == 1
    assert plan[0]['source'] == 'redis'
    assert 'upsert_execution_order' in plan[0]['actions']


def test_build_repair_plan_uses_stream_when_redis_absent():
    """No Redis state → fallback to stream event."""
    mismatches = [_make_mismatch('sid-2', 'warning', 'sql_missing', 'missing')]
    plan = mod.build_repair_plan(
        mismatches,
        {},  # redis_state: empty
        {'sid-2': {'sid': 'sid-2', 'symbol': 'ETHUSDT', 'status': 'open'}},  # stream
        {},  # sql_orders
        {},  # sql_refs
    )
    assert plan[0]['source'] == 'stream'
    assert 'upsert_execution_order' in plan[0]['actions']


def test_build_repair_plan_sync_on_mismatch():
    """fsm_state_mismatch → sync_execution_order appended."""
    mismatches = [_make_mismatch('sid-3', 'warning', 'fsm_state_mismatch', 'differ')]
    plan = mod.build_repair_plan(
        mismatches,
        {'sid-3': {'sid': 'sid-3', 'fsm_state': 'PROTECTED'}},
        {},
        {'sid-3': {'sid': 'sid-3', 'fsm_state': 'OPEN'}},  # sql has old state
        {},
    )
    assert 'sync_execution_order' in plan[0]['actions']


def test_sql_repair_writer_applies_order_and_refs():
    """SQLRepairWriter runs two SQL statements for upsert + refs."""
    conn = _FakeConn()
    writer = mod.SQLRepairWriter(conn)
    counters = writer.apply([
        {
            'sid': 'sid-1',
            'actions': ['upsert_execution_order', 'sync_protection_refs'],
            'source_doc': {
                'sid': 'sid-1',
                'symbol': 'BTCUSDT',
                'status': 'open',
                'fsm_state': 'PROTECTED',
                'sl_algo_id': 11,
            },
            'source_ref_doc': {'sid': 'sid-1', 'symbol': 'BTCUSDT', 'sl_algo_id': 11},
        }
    ])
    assert counters['orders_upserted'] == 1
    assert counters['refs_synced'] == 1
    assert len(conn.statements) == 2


def test_sql_repair_writer_skips_empty_actions():
    """Steps with no actions should be silently skipped."""
    conn = _FakeConn()
    writer = mod.SQLRepairWriter(conn)
    counters = writer.apply([
        {'sid': 'sid-x', 'actions': [], 'source_doc': {}, 'source_ref_doc': {}},
    ])
    assert counters['orders_upserted'] == 0
    assert counters['refs_synced'] == 0
    assert len(conn.statements) == 0
