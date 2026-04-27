import types


class _FakeRedis:
    def __init__(self):
        self.calls = []
    def xadd(self, stream, fields, maxlen=None):
        self.calls.append((stream, dict(fields), maxlen))
        return "1-0"


def _dummy_track():
    t = types.SimpleNamespace()
    t.trade_id = "T1"
    t.symbol = "BTCUSDT"
    t.direction = "LONG"
    t.regime = "na"
    t.start_ts_ms = 1700000000000
    t.bars_seen = 10
    t.tp1_price = 100.0
    t.entry_price = 100.0
    t.sl_price = 99.0
    t.risk_dist = 1.0
    t.atr_entry = 2.0
    t.max_favorable = 101.0
    t.min_favorable = 98.5
    return t


def test_lazy_builder_not_called_when_sampled_out(monkeypatch):
    from services import post_sl_analyzer as m
    monkeypatch.setenv("POSTSL_FINISH_META_SAMPLE_TIME_CAP", "0.0")

    a = m.PostSlAnalyzer.__new__(m.PostSlAnalyzer)
    a.redis = _FakeRedis()
    a._init_finish_meta_controls()

    track = _dummy_track()
    called = {"v": False}

    def builder():
        called["v"] = True
        return {"x": 1}

    a._finish_track(track, "time_cap", 1700000002000, finish_meta=builder)
    assert called["v"] is False

    _, fields, _ = a.redis.calls[0]
    assert "finish_meta" not in fields


def test_lazy_builder_called_when_want_meta(monkeypatch):
    from services import post_sl_analyzer as m
    monkeypatch.setenv("POSTSL_FINISH_META_SAMPLE_TP1", "1.0")

    a = m.PostSlAnalyzer.__new__(m.PostSlAnalyzer)
    a.redis = _FakeRedis()
    a._init_finish_meta_controls()

    track = _dummy_track()
    called = {"v": False}

    def builder():
        called["v"] = True
        return {"x": "LazyData"}

    a._finish_track(track, "tp1_hit", 1700000001000, finish_meta=builder)
    assert called["v"] is True

    _, fields, _ = a.redis.calls[0]
    assert "finish_meta" in fields
    assert "LazyData" in fields["finish_meta"]
