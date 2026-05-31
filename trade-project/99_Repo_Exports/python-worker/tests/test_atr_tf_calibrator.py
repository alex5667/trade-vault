
import pytest

from core.atr_tf_calibrator import ATRTfCalibrator
from utils.atr_cache import ATRCache
from utils.time_utils import get_ny_time_millis


class FakePipeline:
    def __init__(self, db, hashes):
        self.db = db
        self.hashes = hashes
        self.cmds = []

    def hgetall(self, key):
        self.cmds.append(("hgetall", key))
        return self

    def get(self, key):
        self.cmds.append(("get", key))
        return self

    def execute(self):
        results = []
        for cmd, key in self.cmds:
            if cmd == "hgetall":
                results.append(self.hashes.get(key, {}))
            else:
                results.append(self.db.get(key))
        return results

class MockRedis:
    def __init__(self):
        self.db = {}
        self.hashes = {}

    def set(self, key, val, ex=None):
        self.db[key] = str(val)

    def get(self, key):
        return self.db.get(key)

    def hset(self, key, mapping=None, **kwargs):
        mappings = mapping or {}
        mappings.update(kwargs)
        if key not in self.hashes:
            self.hashes[key] = {}
        for k, v in mappings.items():
            self.hashes[key][k] = str(v)

    def hmget(self, key, *args):
        # Implementation for hmget as used in get_with_meta
        h = self.hashes.get(key, {})
        return [h.get(k) for k in args]

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def pipeline(self):
        return FakePipeline(self.db, self.hashes)

@pytest.fixture
def fake_redis():
    return MockRedis()

@pytest.fixture
def rds(fake_redis):
    # conftest.py provides fake_redis (fakeredis client)
    return fake_redis


def _now_ms() -> int:
    return get_ny_time_millis()


def test_atr_cache_get_with_meta_prefers_tracker_hash(rds, monkeypatch):
    c = ATRCache(redis_client=rds)
    c.redis_client = rds
    now = _now_ms()
    # tracker hash uses normalized TF: M1
    rds.hset("ATR:BTCUSDT:M1", mapping={"atr": "42.0", "lastCloseTime": str(now - 10_000)})
    atr, meta = c.get_with_meta("BTCUSDT", "1m", now_ms=now)
    assert atr == 42.0
    assert meta["src"] == "tracker"
    assert meta["key"] == "ATR:BTCUSDT:M1"
    assert meta["age_ms"] >= 0


def test_atr_tf_calibrator_selects_fresher_tf(rds):
    c = ATRCache(redis_client=rds)
    c.redis_client = rds
    now = _now_ms()
    # M1 old, M5 fresh
    rds.hset("ATR:BTCUSDT:M1", mapping={"atr": "10.0", "lastCloseTime": str(now - 200_000)})
    rds.hset("ATR:BTCUSDT:M5", mapping={"atr": "50.0", "lastCloseTime": str(now - 30_000)})

    sel = ATRTfCalibrator(candidates=["1m", "5m"], max_jump_mult=10.0)
    choice = sel.choose(symbol="BTCUSDT", price=100_000.0, now_ms=now, atr_cache=c)
    assert choice is not None
    assert choice.tf in ("5m", "1m")
    assert choice.tf == "5m"


def test_atr_tf_calibrator_penalizes_big_jump(rds):
    c = ATRCache(redis_client=rds)
    c.redis_client = rds
    now = _now_ms()
    # both fresh
    rds.hset("ATR:BTCUSDT:M1", mapping={"atr": "10.0", "lastCloseTime": str(now - 10_000)})   # atr_bps=1.0
    rds.hset("ATR:BTCUSDT:M5", mapping={"atr": "100.0", "lastCloseTime": str(now - 300_000)})  # atr_bps=10.0, age=300s (score~0.5)

    sel = ATRTfCalibrator(candidates=["1m", "5m"], max_jump_mult=4.0)
    # set previous bps ~1.0 by first choosing 1m
    ch1 = sel.choose(symbol="BTCUSDT", price=100_000.0, now_ms=now, atr_cache=c)
    assert ch1 is not None
    assert ch1.tf == "1m"
    # if we now make 1m slightly older and 5m fresh, jump penalty should still keep 1m unless huge freshness diff
    # 1m is 60s old vs 5m 10s old
    # linear decay: age 60s/120s = 0.5 freshness. age 10s/600s = 0.98 freshness
    # fresh score: 0.5 vs 0.98. ratio ~2x.
    # jump pen: 0.25 if jump > 4.0.
    # 100/10 = 10x jump. -> 0.25 penalty.
    # 5m score = 0.98 * 0.25 = ~0.245
    # 1m score = 0.5 * 1.0 = 0.5 (and sticky 1.05 -> 0.525)
    # So 1m should win despite being older.
    rds.hset("ATR:BTCUSDT:M1", mapping={"atr": "10.0", "lastCloseTime": str(now - 60_000)})
    rds.hset("ATR:BTCUSDT:M5", mapping={"atr": "100.0", "lastCloseTime": str(now - 10_000)})
    ch2 = sel.choose(symbol="BTCUSDT", price=100_000.0, now_ms=now, atr_cache=c)
    assert ch2 is not None
    assert ch2.tf == "1m"


def test_atr_tf_calibrator_ignores_out_of_sanity_range(rds):
    c = ATRCache(redis_client=rds)
    c.redis_client = rds
    now = _now_ms()
    # atr too small -> atr_bps < 0.1 (price=100k => atr<1)
    # 0.1 / 100000 = 1e-6. * 10000 = 0.01 bps.
    rds.hset("ATR:BTCUSDT:M1", mapping={"atr": "0.1", "lastCloseTime": str(now - 10_000)})
    # valid
    rds.hset("ATR:BTCUSDT:M5", mapping={"atr": "20.0", "lastCloseTime": str(now - 10_000)})
    sel = ATRTfCalibrator(candidates=["1m", "5m"], min_atr_bps=0.10, max_atr_bps=500.0)
    ch = sel.choose(symbol="BTCUSDT", price=100_000.0, now_ms=now, atr_cache=c)
    assert ch is not None
    assert ch.tf == "5m"
