import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fake_redis import FakeRedis

from services.reliability_curves import update_reliability_curve


def _mk(pos_conf=55.0, tp1=True, tp2=False, pnl_net=0.0, close_reason="TP2"):
    pos = {
        "strategy": "k",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "signal_payload": {"confidence": pos_conf, "kind": "breakout"},
    }
    closed = {
        "strategy": "k",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "tp1_hit": bool(tp1),
        "tp2_hit": bool(tp2),
        "pnl_net": float(pnl_net),
        "close_reason": str(close_reason),
    }
    return pos, closed


def test_reliability_target_tp1_hit_updates():
    os.environ["RELIABILITY_CURVES_ENABLED"] = "1"
    os.environ["RELIABILITY_TARGET"] = "tp1_hit"
    os.environ["RELIABILITY_BUCKET_STEP"] = "5"
    r = FakeRedis()

    pos, closed = _mk(tp1=True)
    update_reliability_curve(r, pos=pos, closed=closed)
    # bucket for 55 with step=5 => 55
    key = "rel:tp1_hit:k:BTCUSDT:1m:na:breakout"
    data = r.hgetall(key)
    assert int(data.get("n_total_55") or 0) == 1
    assert int(data.get("n_hit_55") or 0) == 1


def test_reliability_target_win_updates():
    os.environ["RELIABILITY_CURVES_ENABLED"] = "1"
    os.environ["RELIABILITY_TARGET"] = "win"
    r = FakeRedis()

    pos, closed = _mk(tp1=True, pnl_net=-1.0)
    update_reliability_curve(r, pos=pos, closed=closed)
    key = "rel:win:k:BTCUSDT:1m:na:breakout"
    data = r.hgetall(key)
    assert int(data.get("n_total_55") or 0) == 1
    assert int(data.get("n_hit_55") or 0) == 0


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
