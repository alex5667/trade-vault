# -*- coding: utf-8 -*-
import json
import fakeredis

from utils.atr_cache import ATRCache


def test_atr_cache_prefers_tracker_hash():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset("ATR:BTCUSDT:M1", mapping={"atr": "42.0", "lastCloseTime": "1700000000000"})
    # DEBUG: verify it is there
    chk = r.hgetall("ATR:BTCUSDT:M1")
    print(f"DEBUG TEST: {chk}")
    assert "atr" in chk
    
    c = ATRCache(redis_client=r)
    v, meta = c.get_with_meta("BTCUSDT", "1m", now_ms=1700000001000)
    print(f"DEBUG RESULT: v={v}, meta={meta}")
    assert v == 42.0
    assert meta["src"] == "atr_tracker"
    assert meta["age_ms"] == 1000


def test_atr_cache_reads_string_keys():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("atr:ETHUSDT:1m", "3.5")
    c = ATRCache(redis_client=r)
    v, meta = c.get_with_meta("ETHUSDT", "1m", now_ms=1000)
    assert v == 3.5
    assert meta["src"] == "atr_string"


def test_atr_cache_reads_json_key():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("atr:json:BTCUSDT:1m", json.dumps({"atr": 10.0, "ts": 900}))
    c = ATRCache(redis_client=r)
    v, meta = c.get_with_meta("BTCUSDT", "1m", now_ms=1000)
    assert v == 10.0
    assert meta["src"] == "atr_json"
    assert meta["age_ms"] == 100


def test_atr_cache_ta_last_tf_mismatch_flag():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("ta:last:atr:BTCUSDT", json.dumps({"atr": 11.0, "tf": "M5", "ts": 100}))
    c = ATRCache(redis_client=r)
    v, meta = c.get_with_meta("BTCUSDT", "1m", now_ms=200)
    assert v == 11.0
    assert meta["src"] == "ta_last"
    assert meta["tf_mismatch"] == 1
