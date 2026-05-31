import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import fakeredis; FakeRedis = fakeredis.FakeRedis

from services.reliability_curves import update_reliability_curve


def _mk(pos_conf=55.0, tp1=True, tp2=False, pnl_net=0.0, close_reason="TP2"):
    pos = {
        "strategy": "k",
        "symbol": "BTCUSDT",
        "tf": "1m",
        # signal_payload.ctx.confidence is the canonical path for _extract_base_confidence
        "signal_payload": {"ctx": {"confidence": pos_conf}, "kind": "breakout"},
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


def _hget_int(r, key: str, field: str) -> int:
    data = r.hgetall(key)
    for k, v in (data or {}).items():
        ks = k.decode("utf-8", errors="ignore") if isinstance(k, (bytes, bytearray)) else str(k)
        if ks == field:
            vs = v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else str(v)
            try:
                return int(float(vs))
            except Exception:
                return 0
    return 0


def test_reliability_target_tp1_hit_updates():
    # Correct env var is RELIABILITY_TARGETS (plural); target "tp1_hit" canonicalises to "tp1"
    os.environ["RELIABILITY_TARGETS"] = "tp1"
    os.environ["RELIABILITY_BUCKET_STEP"] = "5"
    os.environ.pop("RELIABILITY_TARGET", None)
    r = FakeRedis()

    pos, closed = _mk(tp1=True)
    update_reliability_curve(r, pos=pos, closed=closed)

    # v3 key: rel:v3:{target}:{strategy}:{symbol}:{tf}:{kind}:{regime}:{ctx}
    # venue=na (not in test data), kind=breakout, regime=na, ctx=na
    key_v3 = "rel:v3:tp1:k:BTCUSDT:1m:breakout:na:na"
    # New field format: n:{bucket} for count, h:{bucket} for hits
    assert _hget_int(r, key_v3, "n:55") == 1, f"Expected n:55=1 in {key_v3}"
    assert _hget_int(r, key_v3, "h:55") == 1, f"Expected h:55=1 in {key_v3}"


def test_reliability_target_win_updates():
    os.environ["RELIABILITY_TARGETS"] = "win"
    os.environ["RELIABILITY_BUCKET_STEP"] = "5"
    os.environ.pop("RELIABILITY_TARGET", None)
    r = FakeRedis()

    # pnl_net < 0 → win=False → h:55 = 0
    pos, closed = _mk(tp1=True, pnl_net=-1.0)
    update_reliability_curve(r, pos=pos, closed=closed)

    key_v3 = "rel:v3:win:k:BTCUSDT:1m:breakout:na:na"
    assert _hget_int(r, key_v3, "n:55") == 1, f"Expected n:55=1 in {key_v3}"
    assert _hget_int(r, key_v3, "h:55") == 0, f"Expected h:55=0 (loss) in {key_v3}"
