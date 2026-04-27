import unittest
from unittest.mock import MagicMock, patch
import json
import time

from utils.atr_cache import ATRCache

class TestATRCacheSourceSelect(unittest.TestCase):
    def setUp(self):
        with patch('utils.atr_cache.get_redis') as mock_get_redis:
            self.mock_redis = MagicMock()
            mock_get_redis.return_value = self.mock_redis
            self.cache = ATRCache()
            # Restore redis checks if needed, but for now we trust the mock
            self.cache.redis_client = self.mock_redis
        self.now_ms = 1_000_000_000

    def test_freshness_wins(self):
        # Setup: 
        # Source A (tracker): val=10, ts=now (fresh)
        # Source B (atr_str): val=20, ts=unknown (stale)
        
        # Mock hmget for tracker (A)
        self.mock_redis.hmget.return_value = ["10.0", str(self.now_ms)] 
        # Mock get for str (B)
        self.mock_redis.get.return_value = "20.0"
        
        # We need to control the sequence of 'get' calls strictly or mock side_effect
        # The code calls:
        # 1. hmget(tracker)
        # 2. get(atr:str)
        # 3. get(atr:val)
        # 4. get(atr:json)
        # 5. get(ta:last)
        
        def get_side_effect(key):
            if "atr:BTC:1m" == key: return "20.0"
            return None
        self.mock_redis.get.side_effect = get_side_effect

        v, meta = self.cache.get_with_meta("BTC", "1m", now_ms=self.now_ms)
        
        # Expect 10.0 because it is fresh (age=0), while 20.0 has unknown age (penalty)
        self.assertEqual(v, 10.0)
        self.assertEqual(meta["src"], "atr_tracker")

    def test_consistency_wins(self):
        # Source A (tracker): 10.0 (fresh)
        # Source B (json): 10.1 (fresh)
        # Source C (str): 10.0 (unknown age)
        # Source D (wrong): 100.0 (fresh but outlier)
        
        self.mock_redis.hmget.return_value = ["10.0", str(self.now_ms)] # tracker
        
        def get_side_effect(key):
            if "atr:json:BTC:1m" in key:
                return json.dumps({"atr": 100.0, "ts": self.now_ms}) # outlier
            if "atr:BTC:1m" == key:
                return "10.0" # consistent
            return None
        self.mock_redis.get.side_effect = get_side_effect

        # The median of [10, 10, 100] is 10. 
        # 100 is far from median (log distance high) -> low score.
        # 10 is close -> high score.
        
        v, meta = self.cache.get_with_meta("BTC", "1m", now_ms=self.now_ms)
        self.assertEqual(v, 10.0)
        # Should pick tracker (fresh + consistent) over json (fresh + outlier)

    def test_tf_match_bonus(self):
        # ta:last often has cross-tf data.
        # If requested tf="15m"
        # Source A (ta_last): val=10, tf="15m" (match)
        # Source B (tracker): val=10, tf="1m" (tracker matches requested tf by key definition, so this test implies logic check within tracker key)
        pass

if __name__ == '__main__':
    unittest.main()
