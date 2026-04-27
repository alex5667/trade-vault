#!/usr/bin/env python3
"""
Unit tests for MarketRegimeService
"""

import unittest
from datetime import datetime
from regime.market_regime_service import MarketRegimeService, RegimeType, RegimeSnapshot, BarSample


class TestMarketRegimeService(unittest.TestCase):

    def setUp(self):
        self.service = MarketRegimeService(atr_window=5)

    def test_initial_state(self):
        """Test initial state with no data"""
        regime = self.service.get_regime("BTCUSDT")
        self.assertIsNone(regime)

    def test_regime_classification_range(self):
        """Test range regime detection with low volatility"""
        # Create bars with low volatility (range-like)
        bars = []
        base_price = 50000.0
        for i in range(20):
            ts = int(datetime.now().timestamp() * 1000) + i * 60000  # 1 min intervals
            bars.append(BarSample(
                symbol="BTCUSDT",
                ts_event_ms=ts,
                open=base_price + i * 10,
                high=base_price + i * 10 + 50,
                low=base_price + i * 10 - 50,
                close=base_price + i * 10 + 10,
                volume=100.0
            ))

        # Feed bars to service
        for bar in bars:
            self.service.on_bar(bar)

        # Check regime
        regime = self.service.get_regime("BTCUSDT")
        self.assertIsNotNone(regime)
        self.assertIn(regime.regime, [RegimeType.RANGE, RegimeType.SQUEEZE])

    def test_regime_classification_trend(self):
        """Test trend regime detection with high directional movement"""
        bars = []
        base_price = 50000.0
        for i in range(20):
            ts = int(datetime.now().timestamp() * 1000) + i * 60000
            # Strong upward trend with high volatility (large ranges)
            close_price = base_price + i * 200 + 80
            # Make ATR high by having large high-low ranges
            high = close_price + 300 + i * 10  # Increasing volatility
            low = close_price - 200 - i * 5
            bars.append(BarSample(
                symbol="BTCUSDT",
                ts_event_ms=ts,
                open=base_price + i * 200,
                high=high,
                low=low,
                close=close_price,
                volume=100.0
            ))

        for bar in bars:
            self.service.on_bar(bar)

        regime = self.service.get_regime("BTCUSDT")
        self.assertIsNotNone(regime)
        # With high ATR, should be TREND_UP due to price movement analysis
        self.assertEqual(regime.regime, RegimeType.TREND_UP)
        self.assertTrue(regime.is_trending)

    def test_allow_emit_squeeze_blocks(self):
        """Test that squeeze regime blocks signal emission"""
        # Manually set squeeze regime
        squeeze_snapshot = RegimeSnapshot(
            symbol="BTCUSDT",
            ts_event_ms=int(datetime.now().timestamp() * 1000),
            regime=RegimeType.SQUEEZE,
            atr_value=10.0,
            atr_quantile=0.1,
            volatility_state="low",
            is_trending=False
        )
        self.service._state_by_symbol["BTCUSDT"] = squeeze_snapshot

        # Should not allow emit in squeeze
        allowed = self.service.allow_emit("BTCUSDT", int(datetime.now().timestamp() * 1000), None)
        self.assertFalse(allowed)

    def test_allow_emit_normal_regime(self):
        """Test that normal regimes allow signal emission"""
        # Manually set range regime
        range_snapshot = RegimeSnapshot(
            symbol="BTCUSDT",
            ts_event_ms=int(datetime.now().timestamp() * 1000),
            regime=RegimeType.RANGE,
            atr_value=50.0,
            atr_quantile=0.5,
            volatility_state="normal",
            is_trending=False
        )
        self.service._state_by_symbol["BTCUSDT"] = range_snapshot

        # Should allow emit in range
        allowed = self.service.allow_emit("BTCUSDT", int(datetime.now().timestamp() * 1000), None)
        self.assertTrue(allowed)


if __name__ == '__main__':
    unittest.main()
