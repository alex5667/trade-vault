import json
import unittest
from unittest.mock import MagicMock

from utils.atr_cache import ATRCache


def _mk_cache(r):
    c = ATRCache(redis_client=r)
    return c


class TestAtrSanitySelector(unittest.TestCase):
    def setUp(self):
        self.r = MagicMock()
        self.c = _mk_cache(self.r)

    def test_selects_fresher_candidate_when_tf_matches(self):
        # desired TF = 1m -> M1
        sym = "BTCUSDT"
        tf = "1m"
        now_ms = 3_000_000

        # Mock Redis data
        # hmget(tracker_key, "atr", "lastCloseTime")
        # get(key)

        def side_effect_hmget(key, *args):
            if key == f"ATR:{sym}:M1":
                return ["40.0", str(now_ms - 200_000)]
            return [None, None]

        def side_effect_get(key):
            if key == f"ta:last:atr:{sym}":
                return json.dumps({"atr": 41.0, "tf": "M1", "ts": now_ms - 50_000})
            if key == f"atr:{sym}:1m":
                return None
            if key == f"atr:val:{sym}:1m":
                return None
            if key == f"atr:json:{sym}:1m":
                return None
            return None

        self.r.hmget.side_effect = side_effect_hmget
        self.r.get.side_effect = side_effect_get

        atr, meta = self.c.get_with_meta(sym, tf, now_ms=now_ms, expected_atr=40.5)
        self.assertIsNotNone(atr)
        self.assertEqual(atr, 41.0)
        self.assertIn(meta.get("source"), ("ta_last",))
        self.assertEqual(int(meta.get("age_ms") or 0), 50_000)

    def test_penalizes_tf_mismatch_even_if_fresher(self):
        sym = "BTCUSDT"
        tf = "1m"
        now_ms = 3_000_000

        # Tracker ok-ish (120s old)
        # ta:last fresher (10s old) but wrong TF (H1 vs M1)

        def side_effect_hmget(key, *args):
            if key == f"ATR:{sym}:M1":
                return ["40.0", str(now_ms - 120_000)]
            return [None, None]

        def side_effect_get(key):
            if key == f"ta:last:atr:{sym}":
                return json.dumps({"atr": 41.0, "tf": "H1", "ts": now_ms - 10_000})
            return None

        self.r.hmget.side_effect = side_effect_hmget
        self.r.get.side_effect = side_effect_get

        atr, meta = self.c.get_with_meta(sym, tf, now_ms=now_ms, expected_atr=40.0)
        self.assertIsNotNone(atr)
        # should prefer tracker due to tf mismatch penalty
        self.assertEqual(atr, 40.0)
        self.assertEqual(meta.get("source"), "tracker")

    def test_penalizes_huge_jump_vs_expected(self):
        sym = "ETHUSDT"
        tf = "1m"
        now_ms = 5_000_000

        # Tracker has absurd jump (999.0 vs exp 4.2)
        # atr:json has stable value (4.0), slightly older

        def side_effect_hmget(key, *args):
            if key == f"ATR:{sym}:M1":
                # Only if args are correct, but safe to ignore args for mock
                return ["999.0", str(now_ms - 20_000)]
            return [None, None]

        def side_effect_get(key):
            if key == f"atr:json:{sym}:1m":
                return json.dumps({"atr": 4.0, "tf": "1m", "ts": now_ms - 60_000, "close": 3000.0})
            return None

        self.r.hmget.side_effect = side_effect_hmget
        self.r.get.side_effect = side_effect_get

        atr, meta = self.c.get_with_meta(sym, tf, now_ms=now_ms, expected_atr=4.2)
        self.assertIsNotNone(atr)
        self.assertEqual(atr, 4.0)
        self.assertEqual(meta.get("source"), "atr_json")

    def test_falls_back_to_string_if_only_string_exists(self):
        sym = "SOLUSDT"
        tf = "1m"
        now_ms = 1_000_000

        def side_effect_get(key):
            if key == f"atr:{sym}:1m":
                return "0.35"
            return None

        self.r.hmget.return_value = [None, None]
        self.r.get.side_effect = side_effect_get

        atr, meta = self.c.get_with_meta(sym, tf, now_ms=now_ms, expected_atr=0.0)
        self.assertIsNotNone(atr)
        self.assertEqual(atr, 0.35)
        self.assertEqual(meta.get("source"), "atr_str")
