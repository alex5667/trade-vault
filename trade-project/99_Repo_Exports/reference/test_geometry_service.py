#!/usr/bin/env python3
"""
Unit tests for HTFLevelsService
"""

import unittest
from datetime import datetime
from geometry.htf_levels import HTFLevelsService
from geometry.structures import LevelType


class TestHTFLevelsService(unittest.TestCase):

    def setUp(self):
        self.service = HTFLevelsService()

    def test_initial_geometry(self):
        """Test initial geometry with no levels"""
        geometry = self.service.get_geometry("BTCUSDT", int(datetime.now().timestamp() * 1000), 50000.0)
        self.assertIsNotNone(geometry)
        self.assertEqual(geometry.symbol, "BTCUSDT")
        self.assertEqual(len(geometry.levels), 0)

    def test_daily_level_creation(self):
        """Test daily level creation on bar close"""
        # Create a mock bar that simulates daily close
        class MockBar:
            def __init__(self, ts_ms, high, low, close):
                self.ts_event_ms = ts_ms
                self.high = high
                self.low = low
                self.close = close
                self.symbol = "BTCUSDT"

        # Simulate daily close bar
        bar = MockBar(
            ts_ms=int(datetime.now().timestamp() * 1000),
            high=51000.0,
            low=49000.0,
            close=50000.0
        )

        # Force daily close detection by setting hour to 23
        original_is_daily_close = self.service._is_daily_close
        self.service._is_daily_close = lambda b: True

        try:
            self.service.on_bar(bar)

            geometry = self.service.get_geometry("BTCUSDT", bar.ts_event_ms, 50000.0)
            self.assertGreater(len(geometry.levels), 0)

            # Should have daily high and low levels
            level_types = [level.level_type for level in geometry.levels]
            self.assertIn(LevelType.DAILY_HIGH, level_types)
            self.assertIn(LevelType.DAILY_LOW, level_types)

        finally:
            self.service._is_daily_close = original_is_daily_close

    def test_geometry_distance_calculation(self):
        """Test distance calculation to nearest levels"""
        # Manually add some levels
        from geometry.structures import Level

        ts = int(datetime.now().timestamp() * 1000)
        levels = [
            Level("BTCUSDT", LevelType.DAILY_HIGH, 51000.0, ts, ts + 86400000, 0.8),
            Level("BTCUSDT", LevelType.DAILY_LOW, 49000.0, ts, ts + 86400000, 0.8),
        ]
        self.service._levels_by_symbol["BTCUSDT"].extend(levels)

        # Test geometry at price 50000 (between levels)
        geometry = self.service.get_geometry("BTCUSDT", ts, 50000.0)

        self.assertIsNotNone(geometry.nearest_level_above)
        self.assertIsNotNone(geometry.nearest_level_below)
        self.assertIsNotNone(geometry.distance_to_nearest_level_bp)

        # Distance to 51000 should be about 200 bps (2%)
        expected_distance_above = ((51000.0 - 50000.0) / 50000.0) * 10000
        self.assertAlmostEqual(geometry.nearest_resistance_bp, expected_distance_above, places=1)

    def test_session_data_tracking(self):
        """Test session data tracking"""
        class MockBar:
            def __init__(self, ts_ms, price):
                self.ts_event_ms = ts_ms
                self.high = self.low = self.close = self.open = price
                self.symbol = "BTCUSDT"

        # Create bars for different sessions with explicit timestamps
        bars = [
            MockBar(1640995200000, 50000.0),  # 2022-01-01 00:00:00 (Asia)
            MockBar(1641038400000, 50100.0),  # 2022-01-01 12:00:00 (Europe)
            MockBar(1641060000000, 50200.0),  # 2022-01-01 18:00:00 (US)
        ]

        for bar in bars:
            self.service.on_bar(bar)

        # Check session data exists
        self.assertIn("BTCUSDT_asia", self.service._session_data)
        self.assertIn("BTCUSDT_europe", self.service._session_data)
        self.assertIn("BTCUSDT_us", self.service._session_data)


if __name__ == '__main__':
    unittest.main()
