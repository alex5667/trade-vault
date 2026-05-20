from types import SimpleNamespace


def _make_handler(redis_stub):
    """Instantiate a minimal concrete BaseOrderFlowHandler for unit tests."""
    from orderflow.base_handler_legacy import BaseOrderFlowHandler

    class _ConcreteHandler(BaseOrderFlowHandler):
        def _get_symbol_specs(self):
            return SimpleNamespace(
                tick_size=0.01, lot_size=0.001, min_notional=10.0,
                base_asset="BTC", quote_asset="USDT",
            )

    h = _ConcreteHandler.__new__(_ConcreteHandler)
    h.symbol = "BTCUSDT"
    h.redis = redis_stub
    h._redis_atr_warning_logged = False
    h.logger = SimpleNamespace(warning=lambda *a, **k: None)
    h._timeframe_to_ms = lambda tf: 60_000
    return h


def test_load_tracker_atr_with_ts_filters_stale(monkeypatch):
    class _FakeRedis:
        def __init__(self, atr, last_close):
            self.atr = atr
            self.last_close = last_close

        def hmget(self, key, *fields):
            return (self.atr, self.last_close)

    h = _make_handler(_FakeRedis("1.23", "1700000000000"))

    monkeypatch.setenv("ATR_REDIS_STALENESS_MULT", "1")  # max_age = 60s

    # current_ts - lastCloseTime = 200s -> stale -> None
    v, ts = h._load_tracker_atr_from_redis_with_ts("1m", 1700000200000)
    assert v is None
    assert ts is None


def test_load_tracker_atr_with_ts_ok(monkeypatch):
    class _FakeRedis:
        def hmget(self, key, *fields):
            return ("2.50", "1700000000000")

    h = _make_handler(_FakeRedis())

    monkeypatch.setenv("ATR_REDIS_STALENESS_MULT", "3")  # max_age=180s

    v, ts = h._load_tracker_atr_from_redis_with_ts("1m", 1700000100000)  # +100s ok
    assert v == 2.50
    assert ts == 1700000000000
