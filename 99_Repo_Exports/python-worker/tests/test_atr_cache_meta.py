from utils.time_utils import get_ny_time_millis
import json
import time

import fakeredis

from utils.atr_cache import ATRCache

# Monkeypatch fakeredis if hmget is missing (older versions)
if not hasattr(fakeredis.FakeRedis, 'hmget'):
    def _hmget(self, name, *keys):
        # Fallback using hgetall (safe for fakeredis)
        data = self.hgetall(name) or {}
        return [data.get(k) for k in keys]
    fakeredis.FakeRedis.hmget = _hmget


def test_atr_cache_meta_prefers_tracker_hash():
    r = fakeredis.FakeRedis(decode_responses=True)
    c = ATRCache(redis_client=r)

    sym = "BTCUSDT"
    # tracker hash wins if present
    r.hset(f"ATR:{sym}:M1", mapping={"atr": "42.0", "ts": str(get_ny_time_millis() - 1000)})
    r.set(f"atr:{sym}:1m", "41.0")

    v, meta = c.get_with_meta(sym, "1m", now_ms=get_ny_time_millis())
    assert float(v) == 42.0
    assert meta["src"] == "atr_tracker"
    assert meta["key"].startswith("ATR:")


def test_atr_cache_meta_reads_atr_json_and_age():
    r = fakeredis.FakeRedis(decode_responses=True)
    c = ATRCache(redis_client=r)

    sym = "ETHUSDT"
    now = get_ny_time_millis()
    payload = {"atr": 3.5, "ts": now - 5000}
    r.set(f"atr:json:{sym}:1m", json.dumps(payload))
    v, meta = c.get_with_meta(sym, "1m", now_ms=now)
    assert abs(float(v) - 3.5) < 1e-9
    assert meta["src"] == "atr_json"
    assert meta["age_ms"] >= 5000


def test_atr_cache_meta_reads_ta_last():
    r = fakeredis.FakeRedis(decode_responses=True)
    c = ATRCache(redis_client=r)

    sym = "BNBUSDT"
    now = get_ny_time_millis()
    r.set(f"ta:last:atr:{sym}", json.dumps({"atr": 0.55, "tf": "M1", "ts": now - 2000}))
    v, meta = c.get_with_meta(sym, "1m", now_ms=now)
    assert abs(float(v) - 0.55) < 1e-9
    assert meta["src"] == "ta_last"
    assert meta.get("tf") == "M1"  # tf field (was tf_last in legacy version)
