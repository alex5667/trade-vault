"""ATR cache source-selection logic tests.

Uses fakeredis instead of MagicMock to avoid fragile coupling to internal
call patterns (hgetall vs hmget).
"""
from __future__ import annotations

import json

import fakeredis

from utils.atr_cache import ATRCache


def _mk(fr):
    c = ATRCache()
    c.redis_client = fr
    return c


NOW = 1_000_000_000


def test_freshness_wins():
    """Timestamped tracker (fresh, age=0) beats string source with unknown age."""
    fr = fakeredis.FakeRedis(decode_responses=True)
    c = _mk(fr)
    fr.hset("ATR:BTC:M1", mapping={"atr": "10.0", "ts": str(NOW)})
    fr.set("atr:BTC:1m", "20.0")  # no ts

    v, meta = c.get_with_meta("BTC", "1m", now_ms=NOW)
    assert v == 10.0, f"expected 10.0, got {v}"
    assert meta["src"] == "tracker"


def test_consistency_wins():
    """Lower-median outlier rejection: tracker(10, fresh) beats atr_json(100, fresh outlier).

    With two ts-matched candidates [10, 100]:
      lower-median = 10
      atr_json deviation = 0.9 → effective_age += 270 000 ms
      tracker deviation = 0   → effective_age = 0
    → tracker wins.
    """
    fr = fakeredis.FakeRedis(decode_responses=True)
    c = _mk(fr)
    fr.hset("ATR:BTC:M1", mapping={"atr": "10.0", "ts": str(NOW)})
    fr.set("atr:json:BTC:1m", json.dumps({"atr": 100.0, "ts": NOW}))

    v, meta = c.get_with_meta("BTC", "1m", now_ms=NOW)
    assert v == 10.0, f"expected tracker(10.0) to beat outlier atr_json(100.0), got {v}"
    assert meta["src"] == "tracker"


def test_three_source_median():
    """With [10, 10, 100]: median=10, outlier(100) receives high effective-age penalty."""
    fr = fakeredis.FakeRedis(decode_responses=True)
    c = _mk(fr)
    fr.hset("ATR:BTC:M1", mapping={"atr": "10.0", "ts": str(NOW)})
    # atr_val not tracked in candidates (no ts, becomes no_ts), add atr_json fresh
    fr.set("atr:json:BTC:1m", json.dumps({"atr": 100.0, "ts": NOW}))
    # Second fresh ts-matched source at 10.0 to shift median
    fr.set("atr:val:BTC:1m", "10.0")  # no ts — only ensures no_ts pool is present

    v, meta = c.get_with_meta("BTC", "1m", now_ms=NOW)
    # tracker(10) beats outlier atr_json(100)
    assert v == 10.0, f"expected 10.0 (tracker), got {v}"


def test_tf_match_bonus_skipped_for_mismatch():
    """ta:last with wrong tf is excluded when ATR_ALLOW_TF_MISMATCH=0 (default)."""
    fr = fakeredis.FakeRedis(decode_responses=True)
    c = _mk(fr)
    fr.set("ta:last:atr:BTC", json.dumps({"atr": 99.0, "tf": "H1", "ts": NOW}))
    # No other source

    v, meta = c.get_with_meta("BTC", "1m", now_ms=NOW)
    assert v is None, f"expected None (H1 mismatch filtered), got {v}"
