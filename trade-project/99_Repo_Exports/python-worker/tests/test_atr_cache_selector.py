import json

import pytest


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
        if key not in self.hashes:
            self.hashes[key] = {}
        if mapping:
            for k, v in mapping.items():
                self.hashes[key][k] = str(v)
        for k, v in kwargs.items():
            self.hashes[key][k] = str(v)

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def pipeline(self):
        return FakePipeline(self.db, self.hashes)

@pytest.fixture
def fake_r():
    return MockRedis()

def _mk_cache(fake_r):
    from utils.atr_cache import ATRCache
    return ATRCache(redis_client=fake_r)

def test_selects_tracker_when_fresh_and_consistent(fake_r):
    c = _mk_cache(fake_r)
    now = 1_700_000_000_000

    # tracker hash (fresh)
    fake_r.hset("ATR:BTCUSDT:M1", mapping={"atr": "42.0", "ts": str(now - 10_000)})
    # json (older)
    fake_r.set("atr:json:BTCUSDT:1m", json.dumps({"atr": 41.5, "ts": now - 120_000}))
    # ta:last (fresh but mismatched tf should not win if mismatch disabled)
    fake_r.set("ta:last:atr:BTCUSDT", json.dumps({"atr": 43.0, "tf": "M5", "ts": now - 5_000}))
    # string fallback
    fake_r.set("atr:BTCUSDT:1m", "41.9")

    atr, meta = c.get_with_meta("BTCUSDT", "1m", now_ms=now)
    assert meta.get("candidates_n", 0) > 0
    assert atr is not None
    assert abs(atr - 42.0) < 1e-6
    assert meta.get("src") == "tracker"
    assert meta.get("tf") == "M1"
    assert int(meta.get("tf_match")) == 1


def test_prefers_atr_json_if_tracker_missing(fake_r):
    c = _mk_cache(fake_r)
    now = 1_700_000_000_000

    fake_r.set("atr:json:ETHUSDT:1m", json.dumps({"atr": 3.8, "ts": now - 15_000}))
    fake_r.set("atr:ETHUSDT:1m", "3.7")
    fake_r.set("atr:val:ETHUSDT:1m", "3.6")

    atr, meta = c.get_with_meta("ETHUSDT", "1m", now_ms=now)
    assert atr is not None
    assert abs(atr - 3.8) < 1e-6
    assert meta.get("src") == "atr_json"


def test_rejects_tf_mismatch_by_default(fake_r):
    c = _mk_cache(fake_r)
    now = 1_700_000_000_000

    # only ta:last with mismatched tf
    fake_r.set("ta:last:atr:SOLUSDT", json.dumps({"atr": 0.55, "tf": "M5", "ts": now - 5_000}))

    atr, meta = c.get_with_meta("SOLUSDT", "1m", now_ms=now)
    assert atr is None
    assert meta.get("src") == "none"


def test_allows_tf_mismatch_if_enabled_env(fake_r, monkeypatch):
    monkeypatch.setenv("ATR_ALLOW_TF_MISMATCH", "1")
    c = _mk_cache(fake_r)
    now = 1_700_000_000_000

    fake_r.set("ta:last:atr:SOLUSDT", json.dumps({"atr": 0.55, "tf": "M5", "ts": now - 5_000}))

    atr, meta = c.get_with_meta("SOLUSDT", "1m", now_ms=now)
    assert atr is not None
    assert meta.get("src") == "ta_last"
    assert int(meta.get("tf_match")) == 0


def test_consistency_penalty_avoids_outlier(fake_r):
    c = _mk_cache(fake_r)
    now = 1_700_000_000_000

    # Two close candidates + one extreme outlier fresh
    # tracker: 0.010 (10s old)
    # json: 0.011 (12s old)
    # ta_last: 0.200 (2s old) <- OUTLIER
    fake_r.hset("ATR:XRPUSDT:M1", mapping={"atr": "0.010", "ts": str(now - 10_000)})
    fake_r.set("atr:json:XRPUSDT:1m", json.dumps({"atr": 0.011, "ts": now - 12_000}))
    fake_r.set("ta:last:atr:XRPUSDT", json.dumps({"atr": 0.200, "tf": "M1", "ts": now - 2_000}))

    atr, meta = c.get_with_meta("XRPUSDT", "1m", now_ms=now)
    assert atr is not None
    # Outlier (0.2) should be penalized enough for tracker (0.01) to win
    assert atr < 0.05
    assert meta.get("src") in ("tracker", "atr_json")
