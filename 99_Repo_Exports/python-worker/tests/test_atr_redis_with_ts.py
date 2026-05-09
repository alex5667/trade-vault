from types import SimpleNamespace


def test_load_tracker_atr_with_ts_filters_stale(monkeypatch):
    from orderflow.base_handler_legacy import BaseOrderFlowHandler

    class _FakeRedis:
        def __init__(self, atr, last_close):
            self.atr = atr
            self.last_close = last_close

        def hmget(self, key, *fields):
            return (self.atr, self.last_close)

    h = BaseOrderFlowHandler.__new__(BaseOrderFlowHandler)
    h.symbol = "BTCUSDT"
    h.redis = _FakeRedis("1.23", "1700000000000")
    h._redis_atr_warning_logged = False
    h.logger = SimpleNamespace(warning=lambda *a, **k: None)
    h._timeframe_to_ms = lambda tf: 60_000  # 1m

    monkeypatch.setenv("ATR_REDIS_STALENESS_MULT", "1")  # max_age = 60s

    # current_ts - lastCloseTime = 200s -> stale -> None
    v, ts = h._load_tracker_atr_from_redis_with_ts("1m", 1700000200000)
    assert v is None
    assert ts is None


def test_load_tracker_atr_with_ts_ok(monkeypatch):
    from orderflow.base_handler_legacy import BaseOrderFlowHandler

    class _FakeRedis:
        def hmget(self, key, *fields):
            return ("2.50", "1700000000000")

    h = BaseOrderFlowHandler.__new__(BaseOrderFlowHandler)
    h.symbol = "BTCUSDT"
    h.redis = _FakeRedis()
    h._redis_atr_warning_logged = False
    h.logger = SimpleNamespace(warning=lambda *a, **k: None)
    h._timeframe_to_ms = lambda tf: 60_000

    monkeypatch.setenv("ATR_REDIS_STALENESS_MULT", "3")  # max_age=180s

    v, ts = h._load_tracker_atr_from_redis_with_ts("1m", 1700000100000)  # +100s ok
    assert v == 2.50
    assert ts == 1700000000000
