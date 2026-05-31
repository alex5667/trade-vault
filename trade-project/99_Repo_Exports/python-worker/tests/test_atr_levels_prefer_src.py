import json

import pytest

from utils.time_utils import get_ny_time_millis


def test_atrcache_prefer_src_meta_key_src():
    try:
        import fakeredis
    except ImportError:
        pytest.skip("fakeredis not available", allow_module_level=True)

    from unittest.mock import patch

    from utils.atr_cache import ATRCache

    r = fakeredis.FakeRedis(decode_responses=True)

    # Patch get_redis to return our fake redis, preventing real connection attempt
    with patch("utils.atr_cache.get_redis", return_value=r):
        c = ATRCache()
    c.redis_client = r

    now = get_ny_time_millis()
    sym = "ETHUSDT"
    tf = "1m"

    # ta:last (timestamped, usually preferred by age)
    # but we deliberately make it very fresh
    r.set(f"ta:last:atr:{sym}", json.dumps({"atr": 5.0, "tf": "M1", "ts": now - 500}))

    # atr:json (timestamped, older)
    # This one we will explicitly prefer below
    r.set(f"atr:json:{sym}:{tf}", json.dumps({"atr": 6.0, "ts": now - 50_000}))

    # 1. Default lookup -> gets freshest (ta:last = 5.0)
    atr, meta = c.get_with_meta(symbol=sym, timeframe=tf, now_ms=now)
    assert atr == 5.0
    assert meta.get("src") == "ta_last"

    # 2. Prefer specific src -> gets chosen one (atr_json = 6.0)
    atr2, meta2 = c.get_with_meta(symbol=sym, timeframe=tf, now_ms=now, prefer_src="atr_json")
    assert atr2 == 6.0
    assert meta2.get("src") == "atr_json"
