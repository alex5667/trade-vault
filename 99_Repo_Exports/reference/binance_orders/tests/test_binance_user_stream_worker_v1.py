from pathlib import Path
import importlib.util
import sys
import json

mod_path = Path(__file__).parent.parent / "services" / "binance_user_stream_worker.py"
spec = importlib.util.spec_from_file_location("binance_user_stream_worker", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


class DummyRedis:
    def __init__(self):
        self.stream = []
        self.kv = {}

    def xadd(self, key, fields):
        self.stream.append((key, dict(fields)))
        return "1-0"

    def set(self, key, value, ex=None):
        self.kv[key] = value
        return True


def _make_worker():
    w = mod.BinanceUserStreamWorker.__new__(mod.BinanceUserStreamWorker)
    w.stream_key = "orders:user_stream"
    w.cache_prefix = "orders:user_stream:"
    w.cache_ttl_sec = 60
    w.r = DummyRedis()
    w._last_event_time_ms = 0
    return w


def test_normalize_order_trade_update():
    w = _make_worker()
    event = w._normalise({
        "e": "ORDER_TRADE_UPDATE",
        "E": 1234,
        "o": {"s": "BTCUSDT", "S": "BUY", "X": "FILLED", "x": "TRADE", "i": 55, "c": "cid-1"},
    })
    assert event is not None
    assert event.event_type == "ORDER_TRADE_UPDATE"
    assert event.client_order_id == "cid-1"
    assert event.order_id == 55


def test_apply_event_orders_by_event_time():
    w = _make_worker()
    newer = mod.NormalizedUserStreamEvent(
        event_type="ORDER_TRADE_UPDATE",
        event_time_ms=2000,
        symbol="BTCUSDT",
        side="BUY",
        status="FILLED",
        execution_type="TRADE",
        order_id=1,
        client_order_id="cid-1",
        algo_id=None,
        client_algo_id=None,
        raw={"o": {"i": 1, "c": "cid-1"}},
    )
    older = mod.NormalizedUserStreamEvent(
        event_type="ORDER_TRADE_UPDATE",
        event_time_ms=1000,
        symbol="BTCUSDT",
        side="BUY",
        status="NEW",
        execution_type="NEW",
        order_id=1,
        client_order_id="cid-1",
        algo_id=None,
        client_algo_id=None,
        raw={"o": {"i": 1, "c": "cid-1"}},
    )
    assert w._apply_event(newer) is True
    assert w._apply_event(older) is False
    assert len(w.r.stream) == 1


def test_handle_message_caches_algo_event():
    w = _make_worker()
    raw = json.dumps({
        "e": "ALGO_UPDATE",
        "E": 3000,
        "ao": {"s": "ETHUSDT", "S": "SELL", "X": "TRIGGERED", "x": "TRIGGERED", "algoId": 77, "clientAlgoId": "algo-1"},
    })
    assert w.handle_message(raw) is True
    assert "orders:user_stream:algo:algo-1" in w.r.kv
