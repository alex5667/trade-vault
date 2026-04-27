import json
import time
import fakeredis

from utils.atr_cache import ATRCache

def _mk_cache(fr):
    c = ATRCache()
    c.redis_client = fr
    return c

def test_selects_fresh_tf_match_over_stale():
    fr = fakeredis.FakeRedis(decode_responses=True)
    c = _mk_cache(fr)

    now = 10_000_000
    sym = "BTCUSDT"
    tf = "1m"

    # tracker hash (stale)
    fr.hset(f"ATR:{sym}:M1", mapping={"atr": "40.0", "ts": str(now - 400_000)})
    # atr:json (fresh)
    fr.set(f"atr:json:{sym}:{tf}", json.dumps({"atr": 41.0, "ts": now - 10_000}))
    # ta:last (fresh but wrong tf)
    fr.set(f"ta:last:atr:{sym}", json.dumps({"atr": 80.0, "tf": "H1", "ts": now - 5_000}))

    atr, meta = c.get_with_meta(sym, tf, now_ms=now)
    assert abs(float(atr) - 41.0) < 1e-9
    assert meta["src"] == "atr_json"
    assert meta["tf"] == "M1"
    assert meta["age_ms"] == 10_000

def test_prefers_tracker_hash_when_fresh_and_consistent():
    fr = fakeredis.FakeRedis(decode_responses=True)
    c = _mk_cache(fr)

    now = 20_000_000
    sym = "ETHUSDT"
    tf = "1m"
    fr.hset(f"ATR:{sym}:M1", mapping={"atr": "5.0", "ts": str(now - 2_000)})
    fr.set(f"atr:json:{sym}:{tf}", json.dumps({"atr": 5.1, "ts": now - 3_000}))

    atr, meta = c.get_with_meta(sym, tf, now_ms=now)
    assert atr is not None
    assert meta["tf"] == "M1"
    assert meta["src"] in ("tracker_hash", "atr_json")
