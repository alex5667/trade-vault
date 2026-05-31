import json

from utils.time_utils import get_ny_time_millis


def test_atr_cache_candidates_and_prefer_src(monkeypatch):
    import fakeredis

    from utils.atr_cache import ATRCache

    r = fakeredis.FakeRedis(decode_responses=True)
    c = ATRCache()
    c.redis_client = r


    now = get_ny_time_millis()
    sym = "BTCUSDT"
    tf = "1m"

    # ta:last (timestamped)
    r.set(f"ta:last:atr:{sym}", json.dumps({"atr": 40.0, "tf": "M1", "ts": now - 1_000}))
    # atr:json (timestamped, older)
    r.set(f"atr:json:{sym}:{tf}", json.dumps({"atr": 41.0, "ts": now - 50_000}))
    # atr string (no ts)
    r.set(f"atr:{sym}:{tf}", "39.0")

    cands = c.get_candidates(symbol=sym, timeframe=tf, now_ms=now)
    assert len(cands) >= 2

    atr, meta = c.get_with_meta(symbol=sym, timeframe=tf, now_ms=now)
    assert atr in (40.0, 41.0, 39.0)
    # Should prefer freshest timestamped: ta:last
    assert meta.get("src") == "ta_last"

    atr2, meta2 = c.get_with_meta(symbol=sym, timeframe=tf, now_ms=now, prefer_src="atr_json")
    assert atr2 == 41.0
    assert meta2.get("src") == "atr_json"
