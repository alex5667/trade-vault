"""P3.3 — unit tests for BinanceExecutor rehydrate-on-miss behaviour.

Tests live in python-worker/services/tests/ so that `services.*` imports
resolve via the path bootstrap in python-worker/conftest.py.

Tests cover:
  - _load_order_state rehydrates from orders:exec when orders:state:{sid} missing
  - rehydrated state is persisted back to Redis
  - _resume_open_from_state works correctly on rehydrated snapshots
  - rehydrate is skipped when EXEC_REHYDRATE_ON_STATE_MISS=False
"""
from __future__ import annotations

from pathlib import Path
import importlib.util
import sys
import json

# Load executor from services/ dir (parent of services/tests/)
mod_path = Path(__file__).parent.parent / "binance_executor.py"
spec = importlib.util.spec_from_file_location("binance_executor", mod_path)
mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)  # type: ignore[union-attr]


class DummyRedis:
    """Minimal in-memory Redis stub supporting get/set/xadd/xrevrange."""

    def __init__(self):
        self.kv = {}
        self.stream = []  # list of (stream_id, fields_dict)

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value
        return True

    def xadd(self, key, fields):
        sid = str(len(self.stream) + 1) + '-0'
        self.stream.append((sid, dict(fields)))
        return sid

    def xrevrange(self, key, start, end, count=100):
        # Return newest-first slice of the stream
        rows = [(sid, fields) for sid, fields in self.stream]
        return list(reversed(rows))[:count]


class DummyClient:
    def query_plain_order(self, symbol, order_id=None, client_order_id=None):
        return {"symbol": symbol, "orderId": order_id, "clientOrderId": client_order_id, "status": "FILLED"}


def _make_exec():
    """Build a bare-bones BinanceExecutor with a DummyRedis backend."""
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.r = DummyRedis()
    ex.exec_stream = 'orders:exec'
    ex.state_key_prefix = 'orders:state:'
    ex.state_ttl = 60
    ex.user_stream_cache_prefix = 'orders:user_stream:'
    ex.reconcile_enable = True
    ex._next_time_sync_due_ms = 0
    ex.binance_time_sync_interval_ms = 30000
    ex.max_clock_drift_ms = 250
    ex.exec_replay_scan_count = 20000
    ex.exec_rehydrate_on_state_miss = True
    ex.execution_journal = None
    return ex


def test_load_order_state_rehydrates_from_exec_stream_when_state_missing():
    """When orders:state:{sid} key is absent, executor must rehydrate from orders:exec."""
    ex = _make_exec()
    # Write execution facts directly to the exec stream (simulates events from a previous run)
    ex._exec_event({'sid': 'sid-1', 'symbol': 'BTCUSDT', 'action': 'open', 'event_type': 'state_transition', 'fsm_state': mod.FSM_ENTRY_ACKED, 'binance_order_id': 111})
    ex._exec_event({'sid': 'sid-1', 'symbol': 'BTCUSDT', 'action': 'open', 'event_type': 'state_transition', 'fsm_state': mod.FSM_PROTECTED, 'sl_algo_id': 222, 'tp1_algo_id': 333})
    # State key does NOT exist yet — executor must rehydrate
    state = ex._load_order_state('sid-1')
    assert state['fsm_state'] == mod.FSM_PROTECTED
    assert str(state['sl_algo_id']) == '222'
    # Rehydrated snapshot must have been persisted back to Redis
    assert 'orders:state:sid-1' in ex.r.kv


def test_load_order_state_returns_hot_state_when_present():
    """When orders:state:{sid} is present, return it immediately without replay."""
    ex = _make_exec()
    hot = {'fsm_state': mod.FSM_PROTECTED, 'sid': 'sid-2', 'symbol': 'ETHUSDT'}
    ex.r.kv['orders:state:sid-2'] = json.dumps(hot)
    state = ex._load_order_state('sid-2')
    assert state['fsm_state'] == mod.FSM_PROTECTED


def test_resume_open_from_rehydrated_state_returns_snapshot():
    """_resume_open_from_state must work correctly on a rehydrated snapshot."""
    ex = _make_exec()
    ex._exec_event({'sid': 'sid-3', 'symbol': 'ETHUSDT', 'action': 'open', 'event_type': 'state_transition', 'fsm_state': mod.FSM_PROTECTED, 'binance_order_id': 44})
    out = ex._resume_open_from_state('sid-3', symbol='ETHUSDT', client=DummyClient())
    assert out is not None
    assert out['recovered_from_state'] is True
    assert out['fsm_state'] == mod.FSM_PROTECTED


def test_rehydrate_disabled_when_flag_off():
    """When EXEC_REHYDRATE_ON_STATE_MISS=False, _recover_state_from_exec_stream returns {}."""
    ex = _make_exec()
    ex.exec_rehydrate_on_state_miss = False
    ex._exec_event({'sid': 'sid-4', 'symbol': 'BTCUSDT', 'action': 'open', 'event_type': 'state_transition', 'fsm_state': mod.FSM_PROTECTED})
    state = ex._load_order_state('sid-4')
    assert state == {}
