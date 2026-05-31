from pathlib import Path
import importlib.util
import sys
import json

mod_path = Path(__file__).parent.parent / "services" / "binance_executor.py"
spec = importlib.util.spec_from_file_location("binance_executor", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


class DummyRedis:
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


class DummyClient:
    def query_plain_order(self, symbol, order_id=None, client_order_id=None):
        return {"symbol": symbol, "orderId": order_id, "clientOrderId": client_order_id, "status": "FILLED"}


def _make_exec():
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.r = DummyRedis()
    ex.exec_stream = "orders:exec"
    ex.state_key_prefix = "orders:state:"
    ex.state_ttl = 60
    ex.user_stream_cache_prefix = "orders:user_stream:"
    ex.reconcile_enable = True
    ex._next_time_sync_due_ms = 0
    ex.binance_time_sync_interval_ms = 30000
    ex.max_clock_drift_ms = 250
    return ex


def test_transition_state_is_idempotent():
    ex = _make_exec()
    ex._transition_state("sid-1", symbol="BTCUSDT", action="open", next_state=mod.FSM_RECEIVED)
    ex._transition_state("sid-1", symbol="BTCUSDT", action="open", next_state=mod.FSM_RECEIVED)
    assert len(ex.r.stream) == 1
    state = json.loads(ex.r.kv["orders:state:sid-1"])
    assert state["fsm_state"] == mod.FSM_RECEIVED


def test_resume_open_from_state_returns_terminal_snapshot():
    ex = _make_exec()
    ex._save_order_state("sid-2", {"symbol": "BTCUSDT", "fsm_state": mod.FSM_PROTECTED, "binance_order_id": 10})
    out = ex._resume_open_from_state("sid-2", symbol="BTCUSDT", client=DummyClient())
    assert out["recovered_from_state"] is True
    assert out["fsm_state"] == mod.FSM_PROTECTED
