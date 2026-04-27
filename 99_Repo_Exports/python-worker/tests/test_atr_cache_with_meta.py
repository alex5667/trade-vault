from utils.time_utils import get_ny_time_millis
import json
import time
import fakeredis
from utils.atr_cache import ATRCache

def test_atr_cache_get_with_meta_from_atr_string_and_ta_last():
    r = fakeredis.FakeRedis(decode_responses=True)
    sym = "BTCUSDT"
    tf = "1m"
    r.set(f"atr:{sym}:{tf}", "42.5")
    now_ms = get_ny_time_millis()
    r.set(f"ta:last:atr:{sym}", json.dumps({"atr": 42.5, "tf": "M1", "ts": now_ms - 1000}))
    c = ATRCache(ttl=15)
    c.redis_client = r
    v, meta = c.get_with_meta(symbol=sym, timeframe=tf)
    assert abs(v - 42.5) < 1e-9
    assert meta["src"] in ("atr_string", "ta_last", "atr_json", "atr_tracker")
    assert meta.get("key")
    # ts_ms might be 0 if parsing failed or not present, that's allowed by get_with_meta
    assert isinstance(meta.get("ts_ms", 0), int)

def test_atr_cache_get_with_meta_none():
    r = fakeredis.FakeRedis(decode_responses=True)
    c = ATRCache(ttl=15)
    c.redis_client = r
    v, meta = c.get_with_meta(symbol="ETHUSDT", timeframe="1m")
    assert v is None
    assert meta["src"] == "none"
