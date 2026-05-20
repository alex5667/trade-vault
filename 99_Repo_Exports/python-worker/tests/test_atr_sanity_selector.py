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
        # Default: no tracker data, no string data, no json data
        self.r.hgetall.return_value = {}
        self.r.get.return_value = None
        self.r.pttl.return_value = -1
        self.c = _mk_cache(self.r)

    def test_selects_fresher_candidate_when_tf_matches(self):
        # desired TF = 1m -> M1
        sym = "BTCUSDT"
        tf = "1m"
        now_ms = 3_000_000

        # tracker: atr=40.0, 200s old
        self.r.hgetall.side_effect = lambda key: (
            {"atr": "40.0", "lastCloseTime": str(now_ms - 200_000)}
            if key == f"ATR:{sym}:M1" else {}
        )
        # ta_last: atr=41.0, same TF, 50s old (fresher)
        self.r.get.side_effect = lambda key: (
            json.dumps({"atr": 41.0, "tf": "M1", "ts": now_ms - 50_000})
            if key == f"ta:last:atr:{sym}" else None
        )

        atr, meta = self.c.get_with_meta(sym, tf, now_ms=now_ms)
        self.assertIsNotNone(atr)
        self.assertEqual(atr, 41.0)
        self.assertEqual(meta.get("src"), "ta_last")
        self.assertEqual(int(meta.get("age_ms") or 0), 50_000)

    def test_penalizes_tf_mismatch_even_if_fresher(self):
        sym = "BTCUSDT"
        tf = "1m"
        now_ms = 3_000_000

        # tracker: atr=40.0, 120s old, matching TF
        self.r.hgetall.side_effect = lambda key: (
            {"atr": "40.0", "lastCloseTime": str(now_ms - 120_000)}
            if key == f"ATR:{sym}:M1" else {}
        )
        # ta_last: atr=41.0, H1 (mismatch), 10s old (fresher but wrong TF)
        self.r.get.side_effect = lambda key: (
            json.dumps({"atr": 41.0, "tf": "H1", "ts": now_ms - 10_000})
            if key == f"ta:last:atr:{sym}" else None
        )

        atr, meta = self.c.get_with_meta(sym, tf, now_ms=now_ms)
        self.assertIsNotNone(atr)
        # tracker should win because ta_last has TF mismatch (filtered out by default)
        self.assertEqual(atr, 40.0)
        self.assertEqual(meta.get("src"), "tracker")

    def test_penalizes_huge_jump_vs_expected(self):
        sym = "ETHUSDT"
        tf = "1m"
        now_ms = 5_000_000

        # tracker: outlier atr=999.0, 20s old
        self.r.hgetall.side_effect = lambda key: (
            {"atr": "999.0", "lastCloseTime": str(now_ms - 20_000)}
            if key == f"ATR:{sym}:M1" else {}
        )
        # atr_json: stable atr=4.0, 60s old
        self.r.get.side_effect = lambda key: (
            json.dumps({"atr": 4.0, "tf": "1m", "ts": now_ms - 60_000, "close": 3000.0})
            if key == f"atr:json:{sym}:1m" else None
        )

        atr, meta = self.c.get_with_meta(sym, tf, now_ms=now_ms)
        self.assertIsNotNone(atr)
        # consistency penalty (>30% deviation from median) makes tracker effective age huge
        self.assertEqual(atr, 4.0)
        self.assertEqual(meta.get("src"), "atr_json")

    def test_falls_back_to_string_if_only_string_exists(self):
        sym = "SOLUSDT"
        tf = "1m"
        now_ms = 1_000_000

        # Only atr:{sym}:{tf} plain string exists
        self.r.get.side_effect = lambda key: (
            "0.35" if key == f"atr:{sym}:1m" else None
        )

        atr, meta = self.c.get_with_meta(sym, tf, now_ms=now_ms)
        self.assertIsNotNone(atr)
        self.assertEqual(atr, 0.35)
        self.assertEqual(meta.get("src"), "atr_string")
