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


def test_finish_track_attaches_finish_meta_json(monkeypatch):
    from services import post_sl_analyzer as m

    # freeze time for determinism
    monkeypatch.setattr(m.time, "time", lambda: 1700000000.0)

    a = m.PostSlAnalyzer.__new__(m.PostSlAnalyzer)
    a.redis = _FakeRedis()
    a._init_finish_meta_controls()

    track = _dummy_track()
    meta = {"tp1_eps_bps": 5.0, "closest_key": "P", "dist_bps": 12.34}
    a._finish_track(track, "tp1_hit", 1700000001234, finish_meta=meta)

    assert len(a.redis.calls) == 1
    stream, fields, maxlen = a.redis.calls[0]
    assert stream == m.OUTPUT_STREAM

    assert "finish_meta" in fields
    decoded = json.loads(fields["finish_meta"])
    assert decoded["tp1_eps_bps"] == 5.0
    assert decoded["closest_key"] == "P"

    assert "finish_meta_trunc" in fields
    assert "finish_meta_len" in fields
    assert "end_ts_ms" in fields


def test_finish_meta_truncates(monkeypatch):
    from services import post_sl_analyzer as m
    monkeypatch.setattr(m.time, "time", lambda: 1700000000.0)

    # Force a small limit (min_v = 256) to trigger truncation even after sanitization
    # Also ensure sampling doesn't exclude the meta
    monkeypatch.setenv("POSTSL_FINISH_META_MAX_CHARS", "256")
    monkeypatch.setenv("POSTSL_FINISH_META_SAMPLE_TIME_CAP", "1.0")

    a = m.PostSlAnalyzer.__new__(m.PostSlAnalyzer)
    a.redis = _FakeRedis()
    a._init_finish_meta_controls()

    track = _dummy_track()
    # "a" * 1000 will be sanitized to 256 chars by inner layer.
    # The resulting JSON string will exceed 256 chars, triggering outer truncation.
    huge = {"x": "a" * 1000}
    a._finish_track(track, "time_cap", 1700000002000, finish_meta=huge)

    _, fields, _ = a.redis.calls[0]
    assert int(fields.get("finish_meta_trunc", 0)) == 1
    assert len(fields["finish_meta"]) <= 256


def test_finish_meta_sanitizes_nan_inf(monkeypatch):
    from services import post_sl_analyzer as m
    monkeypatch.setattr(m.time, "time", lambda: 1700000000.0)

    a = m.PostSlAnalyzer.__new__(m.PostSlAnalyzer)
    a.redis = _FakeRedis()
    a._init_finish_meta_controls()

    track = _dummy_track()
    meta = {"nan": float("nan"), "inf": float("inf"), "ninf": float("-inf")}
    a._finish_track(track, "atr_cap", 1700000003000, finish_meta=meta)

    _, fields, _ = a.redis.calls[0]
    decoded = json.loads(fields["finish_meta"])
    assert decoded["nan"] is None
    assert decoded["inf"] is None
    assert decoded["ninf"] is None
