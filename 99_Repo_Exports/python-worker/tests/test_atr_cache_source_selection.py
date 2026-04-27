from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
import json
import time
import unittest
from unittest.mock import MagicMock

# Since fakeredis might not be installed, we use a MockRedis simple implementation
# that mimics enough of redis for our tests.

class MockRedis:
    def __init__(self, decode_responses=True):
        self.data = {}
        self.decode_responses = decode_responses

    def hset(self, key, mapping):
        if key not in self.data:
            self.data[key] = {}
        # Ensure it's a dict
        if not isinstance(self.data[key], dict):
             self.data[key] = {} # overwrite if it was string? simple mock behavior
        self.data[key].update({k: str(v) for k, v in mapping.items()})

    def hmget(self, key, *fields):
        if key not in self.data or not isinstance(self.data[key], dict):
            return [None] * len(fields)
        val = self.data[key]
        return [val.get(f) for f in fields]

    def set(self, key, value, ex=None):
        self.data[key] = str(value)

    def get(self, key):
        return self.data.get(key)
        
    def delete(self, *keys):
        count = 0
        for k in keys:
            if k in self.data:
                del self.data[k]
                count += 1
        return count
        
    def scan_iter(self, match="*", count=10):
        # Very simple glob match
        import fnmatch
        for k in self.data.keys():
            if fnmatch.fnmatch(k, match):
                yield k

def _mk_cache():
    from utils.atr_cache import ATRCache
    r = MockRedis(decode_responses=True)
    c = ATRCache(ttl=3600, redis_client=r)
    return c, r

class TestAtrCacheSourceSelection(unittest.TestCase):

    def test_prefers_fresh_ta_last_over_tracker_without_ts(self):
        """
        If tracker hash has ATR but missing/unknown timestamp, while ta:last has ts,
        we prefer ta:last due to freshness score.
        """
        c, r = _mk_cache()

        sym = "BTCUSDT"
        tf = "1m"      # requested
        tf_norm = "M1"

        # Tracker with ATR but no lastCloseTime (implies stale/unknown age)
        r.hset(f"ATR:{sym}:{tf_norm}", mapping={"atr": "42.0"})

        now_ms = get_ny_time_millis()
        # ta:last is fresh and matching tf
        r.set(
            f"ta:last:atr:{sym}",
            json.dumps({"atr": 41.5, "tf": tf_norm, "ts": now_ms - 5_000}, separators=(",", ":")),
        )

        atr, meta = c.get_with_meta(sym, tf, now_ms=now_ms)
        self.assertIsNotNone(atr)
        self.assertAlmostEqual(atr, 41.5, delta=1e-9)
        self.assertEqual(meta["src"], "ta_last")
        self.assertEqual(meta["tf_match"], 1)


    def test_prefers_tracker_when_ta_last_tf_mismatch(self):
        """
        ta:last may exist but with different tf; by default mismatch is not allowed.
        Then tracker wins (if present).
        """
        c, r = _mk_cache()

        sym = "ETHUSDT"
        tf = "1m"
        tf_norm = "M1"

        now_ms = get_ny_time_millis()
        # tracker has ts and atr
        r.hset(f"ATR:{sym}:{tf_norm}", mapping={"atr": "3.5", "lastCloseTime": str(now_ms - 20_000)})
        # ta:last is fresh but different tf
        r.set(
            f"ta:last:atr:{sym}",
            json.dumps({"atr": 9.9, "tf": "M5", "ts": now_ms - 1_000}, separators=(",", ":")),
        )

        atr, meta = c.get_with_meta(sym, tf, now_ms=now_ms)
        self.assertIsNotNone(atr)
        self.assertAlmostEqual(atr, 3.5, delta=1e-9)
        self.assertEqual(meta["src"], "tracker")


    def test_atr_json_beats_plain_string_due_to_ts(self):
        """
        atr:{sym}:{tf} has no ts; atr:json has ts => atr:json should win when both exist.
        """
        c, r = _mk_cache()

        sym = "SOLUSDT"
        tf = "1m"
        now_ms = get_ny_time_millis()

        r.set(f"atr:{sym}:{tf}", "10.0")
        r.set(f"atr:json:{sym}:{tf}", json.dumps({"atr": 9.5, "ts": now_ms - 3_000}, separators=(",", ":")))

        atr, meta = c.get_with_meta(sym, tf, now_ms=now_ms)
        self.assertIsNotNone(atr)
        self.assertAlmostEqual(atr, 9.5, delta=1e-9)
        self.assertEqual(meta["src"], "atr_json")


    def test_consistency_penalizes_outlier(self):
        """
        If one candidate is a large outlier vs median, it should be penalized.
        """
        c, r = _mk_cache()

        sym = "BNBUSDT"
        tf = "1m"
        tf_norm = "M1"
        now_ms = get_ny_time_millis()

        # Two consistent candidates around 0.50
        r.hset(f"ATR:{sym}:{tf_norm}", mapping={"atr": "0.51", "lastCloseTime": str(now_ms - 5_000)})
        r.set(f"atr:json:{sym}:{tf}", json.dumps({"atr": 0.49, "ts": now_ms - 4_000}, separators=(",", ":")))

        # Outlier ta:last (still matching tf but huge value)
        r.set(
            f"ta:last:atr:{sym}",
            json.dumps({"atr": 50.0, "tf": tf_norm, "ts": now_ms - 1_000}, separators=(",", ":")),
        )

        # We can't easily monkeypatch os.getenv inside the method without reloading the module
        # or unless the method strictly calls os.getenv every time.
        # The updated code DOES call os.getenv every time in get_with_meta.
        # We can use unittest.mock.patch.dict
        import os
        from unittest.mock import patch
        
        with patch.dict(os.environ, {"ATR_SRC_CONSIST_TOL": "0.10"}):
             atr, meta = c.get_with_meta(sym, tf, now_ms=now_ms)
             self.assertIsNotNone(atr)
             # should pick tracker or atr_json, not ta_last outlier
             self.assertIn(meta["src"], ("tracker", "atr_json"))
             self.assertLess(atr, 2.0)

if __name__ == '__main__':
    unittest.main()
