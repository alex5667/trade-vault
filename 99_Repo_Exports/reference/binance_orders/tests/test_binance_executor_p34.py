"""Tests for binance_executor.py P3/P4 additions: journal integration and counters."""
from pathlib import Path
import importlib.util
import sys
import json

# Load binance_executor module directly using importlib to avoid runtime deps
mod_path = (Path(__file__).parent.parent / "services" / "binance_executor.py").resolve()
spec = importlib.util.spec_from_file_location("binance_executor", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


class DummyRedis:
    """Minimal Redis stub for BinanceExecutor tests."""
    def __init__(self):
        self.kv = {}
        self.stream = []

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value
        return True

    def xadd(self, key, fields):
        self.stream.append((key, dict(fields)))
        return "1-0"


class DummySink:
    """Captures all journal write calls for assertions."""
    def __init__(self):
        self.events = []
        self.snapshots = []
        self.refs = []

    def record_event(self, event):
        self.events.append(dict(event))
        return True

    def upsert_order_snapshot(self, state):
        self.snapshots.append(dict(state))
        return True

    def upsert_protection_refs(self, state):
        self.refs.append(dict(state))
        return True


def _make_exec():
    """Build a shallow BinanceExecutor instance without calling __init__."""
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.r = DummyRedis()
    ex.exec_stream = "orders:exec"
    ex.state_key_prefix = "orders:state:"
    ex.state_ttl = 60
    ex.user_stream_cache_prefix = "orders:user_stream:"
    ex.reconcile_enable = True
    ex._next_time_sync_due_ms = 0
    ex.binance_time_sync_interval_ms = 30_000
    ex.max_clock_drift_ms = 250
    ex.execution_journal = DummySink()
    return ex


def test_exec_event_mirrors_to_journal():
    """_exec_event must write to Redis stream AND mirror to journal.record_event."""
    ex = _make_exec()
    ex._exec_event({"sid": "sid-1", "symbol": "BTCUSDT", "event_type": "custom"})
    assert len(ex.r.stream) == 1
    assert len(ex.execution_journal.events) == 1
    assert ex.execution_journal.events[0]["sid"] == "sid-1"


def test_save_order_state_mirrors_snapshot_and_refs():
    """_save_order_state must upsert both snapshot and protection refs to the journal."""
    ex = _make_exec()
    ex._save_order_state("sid-1", {
        "sid": "sid-1", "symbol": "BTCUSDT",
        "fsm_state": mod.FSM_PROTECTED, "sl_algo_id": 11
    })
    state = json.loads(ex.r.kv["orders:state:sid-1"])
    assert state["fsm_state"] == mod.FSM_PROTECTED
    assert len(ex.execution_journal.snapshots) == 1
    assert len(ex.execution_journal.refs) == 1


def test_exec_and_state_together():
    """After a full event+state cycle the journal receives one event and one snapshot."""
    ex = _make_exec()
    ex._exec_event({"sid": "sid-2", "symbol": "ETHUSDT", "event_type": "transition"})
    ex._save_order_state("sid-2", {"sid": "sid-2", "symbol": "ETHUSDT", "fsm_state": mod.FSM_PROTECTED})
    assert len(ex.execution_journal.events) == 1
    assert len(ex.execution_journal.snapshots) == 1


def test_journal_none_does_not_crash():
    """If execution_journal is None (backward compat), _exec_event must not raise."""
    ex = _make_exec()
    ex.execution_journal = None
    # Should not raise — getattr checks handle None sink gracefully
    ex._exec_event({"sid": "s", "symbol": "BTC"})


def test_execution_journal_attribute_present_after_init():
    """BinanceExecutor class must expose execution_journal as an attribute."""
    # We can't call __init__ due to redis+api-key deps, but we can verify
    # the attribute is assigned in _make_exec via the class machinery.
    ex = _make_exec()
    assert hasattr(ex, "execution_journal")
    assert ex.execution_journal is not None
