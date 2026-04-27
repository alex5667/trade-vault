"""P3.3 — unit tests for execution_state_replay module.

Tests cover:
  - normalize_stream_rows: bytes/str normalization
  - extract_sid_events: filtering by sid, sorted by stream_id
  - replay_sid_state: FSM state build, rehydration metadata
  - compare_replayed_state: mismatch detection
"""
from __future__ import annotations

from pathlib import Path
import importlib.util
import sys
import json

# Load module directly so tests can run without installing the package
mod_path = Path(__file__).parent.parent / "python-worker" / "services" / "execution_state_replay.py"
spec = importlib.util.spec_from_file_location("execution_state_replay", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def test_replay_sid_state_builds_materialized_snapshot():
    """replay_sid_state must build a correct state document from ordered events."""
    rows = [
        ('1-0', {'sid': 'sid-1', 'symbol': 'BTCUSDT', 'action': 'open', 'event_type': 'state_transition', 'fsm_state': 'ENTRY_SUBMITTED', 'ts_ms': '100'}),
        ('2-0', {'sid': 'sid-1', 'symbol': 'BTCUSDT', 'action': 'open', 'event_type': 'state_transition', 'fsm_state': 'ENTRY_ACKED', 'binance_order_id': '123', 'ts_ms': '110'}),
        ('3-0', {'sid': 'sid-1', 'symbol': 'BTCUSDT', 'action': 'open', 'event_type': 'state_transition', 'fsm_state': 'PROTECTED', 'sl_algo_id': '456', 'tp1_algo_id': '457', 'execution_policy': 'SAFETY_FIRST', 'status': 'filled', 'ts_ms': '120'}),
    ]
    events = mod.extract_sid_events(rows, 'sid-1')
    state = mod.replay_sid_state(events)
    assert state['sid'] == 'sid-1'
    assert state['fsm_state'] == 'PROTECTED'
    assert str(state['binance_order_id']) == '123'
    assert str(state['sl_algo_id']) == '456'
    assert state['execution_policy'] == 'SAFETY_FIRST'
    assert state['stream_replayed_events'] == 3
    assert state['rehydrated_from_stream'] is True


def test_compare_replayed_state_detects_mismatch():
    """compare_replayed_state should return mismatches when hot state differs from replay."""
    diff = mod.compare_replayed_state({'fsm_state': 'ENTRY_ACKED'}, {'fsm_state': 'PROTECTED'})
    assert 'fsm_state' in diff
    assert diff['fsm_state']['redis_state'] == 'ENTRY_ACKED'
    assert diff['fsm_state']['replayed_state'] == 'PROTECTED'


def test_compare_replayed_state_no_mismatch():
    """compare_replayed_state should return empty dict when states match."""
    state = {'fsm_state': 'PROTECTED', 'symbol': 'BTCUSDT', 'binance_order_id': '123'}
    diff = mod.compare_replayed_state(state, state)
    assert diff == {}


def test_normalize_stream_rows_handles_bytes():
    """normalize_stream_rows must decode bytes keys/values to str."""
    rows = [(b'1-0', {b'sid': b'sid-1', b'fsm_state': b'PROTECTED'})]
    norm = mod.normalize_stream_rows(rows)
    assert norm[0]['sid'] == 'sid-1'
    assert norm[0]['stream_id'] == '1-0'


def test_extract_sid_events_sort_order():
    """extract_sid_events must sort events oldest→newest regardless of input order."""
    rows = [
        ('3-0', {'sid': 'sid-1', 'fsm_state': 'PROTECTED'}),
        ('1-0', {'sid': 'sid-1', 'fsm_state': 'ENTRY_SUBMITTED'}),
        ('2-0', {'sid': 'sid-1', 'fsm_state': 'ENTRY_ACKED'}),
    ]
    events = mod.extract_sid_events(rows, 'sid-1')
    assert [e['fsm_state'] for e in events] == ['ENTRY_SUBMITTED', 'ENTRY_ACKED', 'PROTECTED']


def test_transient_fields_excluded():
    """TRANSIENT_FIELDS should not leak into the materialized state snapshot."""
    rows = [
        ('1-0', {'sid': 's', 'event_type': 'state_transition', 'fsm_state': 'PROTECTED',
                 'severity': 'warning', 'msg': 'test', 'reason': 'noop'}),
    ]
    events = mod.extract_sid_events(rows, 's')
    state = mod.replay_sid_state(events)
    for field in mod.TRANSIENT_FIELDS:
        assert field not in state, f"transient field {field!r} leaked into state"
