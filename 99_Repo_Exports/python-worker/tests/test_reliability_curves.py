from services.reliability_curves import update_reliability_curve
from tests.fake_redis import FakeRedis


def _hget_int(redis, key: str, field: str) -> int:
    h = redis.hgetall(key) or {}
    for k, v in h.items():
        ks = k.decode("utf-8", errors="ignore") if isinstance(k, (bytes, bytearray)) else str(k)
        if ks == field:
            vs = v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else str(v)
            try:
                return int(float(vs))
            except Exception:
                return 0
    return 0


def test_reliability_tp1_default(monkeypatch):
    monkeypatch.setenv("RELIABILITY_TARGETS", "tp1")
    monkeypatch.setenv("RELIABILITY_BUCKET_STEP", "10")
    r = FakeRedis()
    closed = {
        "symbol": "BTCUSDT",
        "tf": "1m",
        "strategy": "breakout",
        "kind": "breakout",
        "entry_regime": "trend",
        "venue": "binance_futures",
        "confidence": 57.0,
        "tp1_hit": True,
        "close_reason": "TP2",
        "pnl_net": 10.0,
    }
    update_reliability_curve(r, closed=closed, pos=None)
    # Check v4 key (modern format with venue)
    key = "rel:v4:tp1:breakout:BTCUSDT:1m:binance_futures:breakout:trend:na"
    # 57.0 with step=10 -> bucket 60 (57/10=5.7->round=6->60)
    assert _hget_int(r, key, "n:60") == 1
    assert _hget_int(r, key, "h:60") == 1
