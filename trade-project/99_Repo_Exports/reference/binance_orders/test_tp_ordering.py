import sys
import os
import unittest
from unittest.mock import MagicMock

# MOCK dependencies before import
# Create a dummy package for redis so importing 'redis.asyncio' works
redis_mock = MagicMock()
redis_asyncio_mock = MagicMock()
redis_mock.asyncio = redis_asyncio_mock
sys.modules["redis"] = redis_mock
sys.modules["redis.asyncio"] = redis_asyncio_mock
sys.modules["redis.exceptions"] = MagicMock()

sys.modules["prometheus_client"] = MagicMock()
sys.modules["aioredis"] = MagicMock()
sys.modules["backoff"] = MagicMock()
sys.modules["async_timeout"] = MagicMock()

# Adjust path to include python-worker
sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

from services.crypto_orderflow_service import CryptoOrderflowService, SymbolRuntime

class TestTPOrdering(unittest.TestCase):
    def setUp(self):
        # CryptoOrderflowService(redis_dsn, ticks_dsn=None)
        self.service = CryptoOrderflowService("redis://mock", "redis://mock")
        # Mock _get_rocket_multiplier to return a high value
        self.service._get_rocket_multiplier = MagicMock(return_value=2.28)

    def test_rocket_v1_monotonic_scaling(self):
        # Setup Runtime with Config
        config = {
            "stop_mode": "ATR",
            "stop_atr_mult": 0.6,
            "tp_rr": "1.3,2.0,2.7",  # Standard RR
            "trail_profile": "rocket_v1",
            "atr_tf": "1m"
        }
        runtime = MagicMock(spec=SymbolRuntime)
        runtime.config = config
        runtime.symbol = "BTCUSDT"

        entry = 100.0
        indicators = {"atr": 1.0, "lot": 0.1}
        side = "LONG"

        # Call _calculate_levels
        sl, tps, lot, atr = self.service._calculate_levels(
            runtime, entry, side, indicators, trail_profile="rocket_v1"
        )
        
        print(f"\nLONG Entry: {entry}, ATR: {atr}")
        print(f"TPs: {tps}")

        # Assertions
        # 1. Check Monotonicity
        self.assertLess(tps[0], tps[1], "TP1 should be < TP2")
        self.assertLess(tps[1], tps[2], "TP2 should be < TP3")

        # 2. Check TP1 Distance matches Rocket Mult
        tp1_dist = tps[0] - entry
        self.assertAlmostEqual(tp1_dist, 2.28 * atr, places=2, msg="TP1 distance should match rocket mult")

        # 3. Check Dynamic Scaling of TP2/TP3
        # Static RR TP2 would be 0.6 * 2.0 = 1.2
        # Fixed logic should force TP2 >= 1.5 * TP1_dist = 1.5 * 2.28 = 3.42
        tp2_dist = tps[1] - entry
        expected_min_tp2 = tp1_dist * 1.5
        self.assertGreaterEqual(tp2_dist, expected_min_tp2 - 0.01, "TP2 should be scaled up relative to TP1")

        tp3_dist = tps[2] - entry
        expected_min_tp3 = tp1_dist * 2.0
        self.assertGreaterEqual(tp3_dist, expected_min_tp3 - 0.01, "TP3 should be scaled up relative to TP1")

    def test_rocket_v1_short_monotonic(self):
         # Setup Runtime with Config
        config = {
            "stop_mode": "ATR",
            "stop_atr_mult": 0.6,
            "tp_rr": "1.3,2.0,2.7",
            "trail_profile": "rocket_v1",
             "atr_tf": "1m"
        }
        runtime = MagicMock(spec=SymbolRuntime)
        runtime.config = config
        runtime.symbol = "BTCUSDT"

        entry = 100.0
        indicators = {"atr": 1.0, "lot": 0.1}
        side = "SHORT"

        # Call _calculate_levels
        sl, tps, lot, atr = self.service._calculate_levels(
            runtime, entry, side, indicators, trail_profile="rocket_v1"
        )
        
        print(f"\nSHORT Entry: {entry}, ATR: {atr}")
        print(f"TPs: {tps}")

        # Assertions for SHORT (TPs are lower than entry)
        # Distances from entry should be increasing
        dists = [entry - tp for tp in tps]
        
        self.assertLess(dists[0], dists[1], "TP1 dist should be < TP2 dist")
        self.assertLess(dists[1], dists[2], "TP2 dist should be < TP3 dist")
        
        # Verify actual price values are descending
        self.assertGreater(tps[0], tps[1], "TP1 > TP2 for SHORT")
        self.assertGreater(tps[1], tps[2], "TP2 > TP3 for SHORT")

if __name__ == '__main__':
    unittest.main()
