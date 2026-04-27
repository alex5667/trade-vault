import json
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
    t.regime = "range"
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


def test_sampling_p0_excludes_finish_meta(monkeypatch):
    from services import post_sl_analyzer as m
    monkeypatch.setattr(m.time, "time", lambda: 1700000000.0)
    monkeypatch.setenv("POSTSL_FINISH_META_SAMPLE_TIME_CAP", "0.0")
    monkeypatch.setenv("POSTSL_FINISH_META_SAMPLE_TAGS", "1")

    a = m.PostSlAnalyzer.__new__(m.PostSlAnalyzer)
    a.redis = _FakeRedis()
    a._init_finish_meta_controls()
    track = _dummy_track()

    a._finish_track(track, "time_cap", 1700000002000, finish_meta={"x": 1})
    _, fields, _ = a.redis.calls[0]
    assert "finish_meta" not in fields
    assert fields.get("finish_meta_sampled_out") == 1
    assert float(fields.get("finish_meta_sample_p")) == 0.0


def test_sampling_p1_includes_finish_meta(monkeypatch):
    from services import post_sl_analyzer as m
    monkeypatch.setattr(m.time, "time", lambda: 1700000000.0)
    monkeypatch.setenv("POSTSL_FINISH_META_SAMPLE_TP1", "1.0")

    a = m.PostSlAnalyzer.__new__(m.PostSlAnalyzer)
    a.redis = _FakeRedis()
    a._init_finish_meta_controls()
    track = _dummy_track()

    a._finish_track(track, "tp1_hit", 1700000001234, finish_meta={"tp1_eps_bps": 5.0})
    _, fields, _ = a.redis.calls[0]
    assert "finish_meta" in fields
    decoded = json.loads(fields["finish_meta"])
    assert decoded["tp1_eps_bps"] == 5.0


def test_sampling_deterministic(monkeypatch):
    from services import post_sl_analyzer as m
    monkeypatch.setenv("POSTSL_FINISH_META_SAMPLE_TIME_CAP", "0.5")

    a = m.PostSlAnalyzer.__new__(m.PostSlAnalyzer)
    a.redis = _FakeRedis()
    a._init_finish_meta_controls()
    track = _dummy_track()

    w1, p1, u1 = a._want_finish_meta(track, "time_cap")
    w2, p2, u2 = a._want_finish_meta(track, "time_cap")
    assert p1 == p2
    assert u1 == u2
    assert w1 == w2


def test_want_finish_meta_bool_matches_tuple(monkeypatch):
    from services import post_sl_analyzer as m
    a = m.PostSlAnalyzer.__new__(m.PostSlAnalyzer)
    a._init_finish_meta_controls()
    track = _dummy_track()
    # Check multiple reasons to be sure
    for reason in ["tp1_hit", "time_cap", "atr_cap", "default"]:
        want1, _, _ = a._want_finish_meta(track, reason)
        want2 = a._want_finish_meta_bool(track, reason)
        assert want1 == want2
