from __future__ import annotations

import importlib.util
from pathlib import Path
import json
import sys

SCRIPT = Path(__file__).resolve().parent.parent.parent / 'scripts' / 'backfill_execution_journal_from_orders_exec.py'
SPEC = importlib.util.spec_from_file_location('backfill_execution_journal_from_orders_exec', SCRIPT)
mod = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


def test_parse_exec_stream_entry_uses_action_as_default_event_type():
    row = mod.parse_exec_stream_entry('1-0', {'sid': 's1', 'symbol': 'BTCUSDT', 'action': 'open', 'ts_ms': '1000'})
    assert row.sid == 's1'
    assert row.event_type == 'open'
    assert row.event_ts_ms == 1000


def test_parse_exec_stream_entry_explicit_event_type():
    row = mod.parse_exec_stream_entry('2-0', {'sid': 's2', 'symbol': 'ETHUSDT', 'event_type': 'protect', 'action': 'open', 'ts_ms': '2000'})
    assert row.event_type == 'protect'


def test_derive_snapshot_rows_merges_latest_by_sid():
    events = [
        mod.ExecEventRow('1-0', 's1', 'BTCUSDT', 'open', 1000, json.dumps({'sid': 's1', 'symbol': 'BTCUSDT', 'action': 'open', 'status': 'submitted', 'fsm_state': 'ENTRY_SUBMITTED'})),
        mod.ExecEventRow('2-0', 's1', 'BTCUSDT', 'protect', 2000, json.dumps({'sid': 's1', 'symbol': 'BTCUSDT', 'sl_algo_id': 11, 'tp1_algo_id': 12, 'fsm_state': 'PROTECTED'})),
    ]
    snapshots, refs = mod.derive_snapshot_rows(events)
    assert len(snapshots) == 1
    assert snapshots[0].sid == 's1'
    assert snapshots[0].fsm_state == 'PROTECTED'
    assert len(refs) == 1
    assert refs[0].sl_algo_id == 11
    assert refs[0].tp1_algo_id == 12


def test_derive_snapshot_rows_multiple_sids():
    events = [
        mod.ExecEventRow('1-0', 's1', 'BTCUSDT', 'open', 1000, json.dumps({'sid': 's1', 'symbol': 'BTCUSDT', 'fsm_state': 'ENTRY_SUBMITTED'})),
        mod.ExecEventRow('2-0', 's2', 'ETHUSDT', 'open', 2000, json.dumps({'sid': 's2', 'symbol': 'ETHUSDT', 'fsm_state': 'ENTRY_SUBMITTED'})),
    ]
    snapshots, refs = mod.derive_snapshot_rows(events)
    assert len(snapshots) == 2
    sids = {s.sid for s in snapshots}
    assert 's1' in sids
    assert 's2' in sids


def test_derive_snapshot_rows_empty():
    snapshots, refs = mod.derive_snapshot_rows([])
    assert snapshots == []
    assert refs == []


def test_derive_snapshot_rows_skips_empty_sid():
    events = [
        mod.ExecEventRow('1-0', '', 'BTCUSDT', 'open', 1000, json.dumps({'symbol': 'BTCUSDT', 'fsm_state': 'X'})),
    ]
    snapshots, refs = mod.derive_snapshot_rows(events)
    assert snapshots == []


def test_parse_exec_stream_entry_missing_ts():
    row = mod.parse_exec_stream_entry('1-0', {'sid': 'abc', 'symbol': 'SOLUSDT'})
    assert row.event_ts_ms == 0
    assert row.event_type == 'event'
