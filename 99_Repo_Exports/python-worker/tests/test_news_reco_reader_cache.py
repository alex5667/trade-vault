import time

from services.news_reco_reader.cache import NewsRecoCache, now_ms


def test_update_from_map_json_filters_expired_and_invalid():
    c = NewsRecoCache(max_symbols=10)
    t0 = now_ms()
    raw = {
        "schema_ver": "v1",
        "ts_ms": t0 - 10,
        "reco": {
            "BTCUSDT": {"expires_ms": t0 + 1000, "suggest_profile": "tighten"},
            "ETHUSDT": {"expires_ms": t0 - 1, "suggest_profile": "soft"},   # expired
            "BAD": {"expires_ms": "nope"},  # invalid
            "X": "not an object",           # invalid
        },
    }
    import json
    updated, invalid, expired = c.update_from_map_json(json.dumps(raw), now=t0)
    assert updated == 1
    assert invalid == 2
    assert expired == 1

    assert c.get("BTCUSDT", now=t0) is not None
    assert c.get("ETHUSDT", now=t0) is None


def test_eviction_by_earliest_expiry():
    c = NewsRecoCache(max_symbols=2)
    c._max_symbols = 2  # override the clamp
    t0 = now_ms()
    import json
    raw = {
        "schema_ver": "v1",
        "ts_ms": t0,
        "reco": {
            "A": {"expires_ms": t0 + 100},
            "B": {"expires_ms": t0 + 200},
            "C": {"expires_ms": t0 + 300},
        },
    }
    c.update_from_map_json(json.dumps(raw), now=t0)
    # Only 2 should remain, with latest expiries (B,C)
    assert c.get("A", now=t0) is None
    assert c.get("B", now=t0) is not None
    assert c.get("C", now=t0) is not None
