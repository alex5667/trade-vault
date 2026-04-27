"""P3.3 — integration tests: rebuild_state_from_stream + persist_state_snapshot consistency flow.

Tests cover:
  - rebuild_state_from_stream produces correct snapshot from XREVRANGE rows
  - persist_state_snapshot writes JSON with TTL
  - compare_replayed_state correctly flags divergent fields
  - full roundtrip: stream → rebuild → persist → load → compare (no mismatch)
"""
from __future__ import annotations

from pathlib import Path
import importlib.util
import sys
import json

mod_path = Path(__file__).parent.parent / "python-worker" / "services" / "execution_state_replay.py"
spec = importlib.util.spec_from_file_location("execution_state_replay", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


class DummyRedis:
    def __init__(self):
        self.kv = {}
        self.stream = []

    def set(self, key, value, ex=None):
        self.kv[key] = value

    def get(self, key):
        return self.kv.get(key)

    def xrevrange(self, key, start, end, count=100):
        return list(reversed(self.stream))[:count]


def test_rebuild_state_from_stream_and_persist():
    """Full rebuild → persist roundtrip should produce rehydrated_from_stream=True."""
    r = DummyRedis()
    r.stream = [
        ('1-0', {'sid': 'sid-1', 'symbol': 'BTCUSDT', 'event_type': 'state_transition', 'fsm_state': 'ENTRY_ACKED', 'binance_order_id': '12'}),
        ('2-0', {'sid': 'sid-1', 'symbol': 'BTCUSDT', 'event_type': 'state_transition', 'fsm_state': 'PROTECTED', 'sl_algo_id': '88'}),
    ]
    state = mod.rebuild_state_from_stream(r, exec_stream='orders:exec', sid='sid-1', scan_count=100)
    assert state['fsm_state'] == 'PROTECTED'
    mod.persist_state_snapshot(r, state_key='orders:state:sid-1', state_doc=state, ttl_sec=60)
    saved = json.loads(r.get('orders:state:sid-1'))
    assert saved['rehydrated_from_stream'] is True
    assert saved['stream_replayed_events'] == 2


def test_rebuild_state_empty_for_unknown_sid():
    """rebuild_state_from_stream returns {} when no events match the SID."""
    r = DummyRedis()
    r.stream = [('1-0', {'sid': 'other-sid', 'fsm_state': 'PROTECTED'})]
    state = mod.rebuild_state_from_stream(r, exec_stream='orders:exec', sid='unknown-sid', scan_count=100)
    assert state == {}


def test_consistency_check_no_mismatch_after_rebuild():
    """After rebuild, compare_replayed_state should report no mismatches."""
    r = DummyRedis()
    r.stream = [
        ('1-0', {'sid': 'sid-2', 'symbol': 'ETHUSDT', 'event_type': 'state_transition', 'fsm_state': 'PROTECTED', 'sl_algo_id': '55', 'binance_order_id': '7'}),
    ]
    state = mod.rebuild_state_from_stream(r, exec_stream='orders:exec', sid='sid-2', scan_count=100)
    mod.persist_state_snapshot(r, state_key='orders:state:sid-2', state_doc=state, ttl_sec=0)
    import json as _json
    redis_state = _json.loads(r.get('orders:state:sid-2'))
    replayed = mod.rebuild_state_from_stream(r, exec_stream='orders:exec', sid='sid-2', scan_count=100)
    diff = mod.compare_replayed_state(redis_state, replayed)
    assert diff == {}


def test_compare_replayed_state_detects_mismatch():
    """compare_replayed_state should surface divergence in key execution fields."""
    diff = mod.compare_replayed_state(
        {'fsm_state': 'ENTRY_ACKED', 'sl_algo_id': '10'},
        {'fsm_state': 'PROTECTED', 'sl_algo_id': '10'},
    )
    assert 'fsm_state' in diff
    assert 'sl_algo_id' not in diff
