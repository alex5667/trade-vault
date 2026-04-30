from pathlib import Path
import importlib.util
import sys

mod_path = Path(__file__).parent.parent / "binance_executor.py"
spec = importlib.util.spec_from_file_location("binance_executor", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

pol_path = Path(__file__).parent.parent / "execution_policy.py"
pspec = importlib.util.spec_from_file_location("execution_policy", pol_path)
pol = importlib.util.module_from_spec(pspec)
sys.modules[pspec.name] = pol
assert pspec.loader is not None
pspec.loader.exec_module(pol)


class DummyFilters:
    class Obj:
        tick_size = 0.1
        step_size = 0.001

    def get(self, symbol):
        return self.Obj()


class DummyClient:
    def __init__(self):
        self.calls = []
        self._n = 100

    def post_algo_order(self, params):
        self.calls.append(dict(params))
        self._n += 1
        return {"algoId": self._n}

    def post_plain_order(self, params):
        self.calls.append(dict(params))
        self._n += 1
        return {"orderId": self._n, "status": "FILLED"}

    def get_working_price(self, symbol, working_type):
        return 100.0


def _make_exec():
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.position_mode = "oneway"
    ex.sl_working_type = "MARK_PRICE"
    ex.tp_market_working_type = "MARK_PRICE"
    ex.tp_limit_trigger_working_type = "MARK_PRICE"
    ex.tp_limit_time_in_force = "GTX"
    ex.tp_limit_price_offset_bps = 0.0
    ex.trail_working_type = "MARK_PRICE"
    ex.trail_activate_price_bps = 5.0
    ex._local_headroom_check = lambda **kwargs: None
    ex._split_tp_qtys = lambda symbol, total_qty, n, filters: [total_qty / n] * n
    ex._quantize = lambda symbol, qty, price, filters: (f"{qty}", f"{price}" if price is not None else None)
    ex._validate_exit_contract = lambda **kwargs: None
    ex._emit_tp_state = lambda *args, **kwargs: None
    return ex


def test_place_protective_maker_first_uses_take_profit_limit():
    ex = _make_exec()
    client = DummyClient()
    filters = DummyFilters()
    policy = pol.ExecutionPolicyDecision(
        name=pol.MAKER_FIRST
        reason="test"
        tp_order_type="TAKE_PROFIT"
        tp_working_type="MARK_PRICE"
        tp_limit_time_in_force="GTX"
        tp_watchdog_enabled=True
        tp_watchdog_timeout_ms=4000
    )
    out = ex._place_protective(
        sid="sid-1"
        symbol="BTCUSDT"
        logical_side="LONG"
        qty=1.0
        sl=95.0
        tps=[101.0, 102.0]
        policy=policy
        client=client
        filters=filters
    )
    assert out["sl_algo_id"]
    tp_calls = [c for c in client.calls if c.get("type") in {"TAKE_PROFIT", "TAKE_PROFIT_MARKET"}]
    assert tp_calls[0]["type"] == "TAKE_PROFIT"
    assert tp_calls[0]["timeInForce"] == "GTX"
    assert "price" in tp_calls[0]
    assert out["tp1_state"] == "TP1_ARMED"


def test_place_protective_safety_first_uses_take_profit_market():
    ex = _make_exec()
    client = DummyClient()
    filters = DummyFilters()
    policy = pol.ExecutionPolicyDecision(
        name=pol.SAFETY_FIRST
        reason="test"
        tp_order_type="TAKE_PROFIT_MARKET"
        tp_working_type="MARK_PRICE"
        tp_limit_time_in_force=None
        tp_watchdog_enabled=False
        tp_watchdog_timeout_ms=0
    )
    out = ex._place_protective(
        sid="sid-2"
        symbol="BTCUSDT"
        logical_side="LONG"
        qty=1.0
        sl=95.0
        tps=[101.0]
        policy=policy
        client=client
        filters=filters
    )
    tp_calls = [c for c in client.calls if c.get("type") in {"TAKE_PROFIT", "TAKE_PROFIT_MARKET"}]
    assert tp_calls[0]["type"] == "TAKE_PROFIT_MARKET"
    assert out["tp1_order_type"] == "TAKE_PROFIT_MARKET"


def test_trailing_activate_price_guard_for_long_exit():
    px = mod.compute_trailing_activate_price(
        "LONG", latest_price=100.0, tick_size=0.1, buffer_bps=5.0
    )
    assert px > 100.0


def test_submit_reduce_only_market_exit_uses_plain_order_path():
    ex = _make_exec()
    client = DummyClient()
    filters = DummyFilters()
    close = ex._submit_reduce_only_market_exit(
        sid="sid-3"
        symbol="BTCUSDT"
        logical_side="LONG"
        qty=0.5
        reason_tag="emerg"
        client=client
        filters=filters
    )
    assert close["close_order_id"]
    last = client.calls[-1]
    assert last["type"] == "MARKET"
    assert last["reduceOnly"] is True
