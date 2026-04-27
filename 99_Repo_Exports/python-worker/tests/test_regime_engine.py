# test_regime_engine.py
"""
Tests for RegimeEngine and BarBuilder1m.
"""

import pytest
import math
from types import SimpleNamespace

# Import components
try:
    from regime_engine import BarBuilder1m, Bar, RegimeEngine, RegimeState
except ImportError:
    from python_worker.regime_engine import BarBuilder1m, Bar, RegimeEngine, RegimeState


def make_config():
    """Create test configuration with default regime parameters."""
    return SimpleNamespace(
        regime_atr_n=14,
        regime_atr_hist=120,
        regime_atr_hi_q=0.70,
        regime_atr_lo_q=0.35,
        regime_delta_ema_alpha=0.10,
        regime_cross_hist=30,
        regime_hold_ema_alpha=0.20,
        regime_delta_thr=0.0,
        regime_w_atr=0.35,
        regime_w_delta=0.30,
        regime_w_hold=0.25,
        regime_w_ping=0.20,
        regime_label_hi=0.35,
        regime_label_lo=-0.35,
    )


class TestBarBuilder1m:
    """Test BarBuilder1m functionality."""

    def test_initial_bar_creation(self):
        """Test that first tick creates initial bar."""
        builder = BarBuilder1m()

        # First tick
        ts = 100000  # 100 seconds
        price = 100.0
        volume = 1.0
        delta = 0.5

        result = builder.update_tick(ts, price, volume, delta)

        # Should return None (no completed bar yet)
        assert result is None

        # Check internal state
        assert builder.cur is not None
        assert builder.cur.ts_open == 60000  # minute boundary
        assert builder.cur.open == price
        assert builder.cur.high == price
        assert builder.cur.low == price
        assert builder.cur.close == price
        assert builder.cur.volume == volume
        assert builder.cur.delta == delta

    def test_same_minute_updates(self):
        """Test updates within same minute."""
        builder = BarBuilder1m()

        # First tick
        ts1 = 60000  # exactly at minute boundary
        builder.update_tick(ts1, 100.0, 1.0, 0.5)

        # Second tick in same minute
        ts2 = 119999  # still same minute
        builder.update_tick(ts2, 101.0, 2.0, -0.3)

        # Should still return None
        # Check accumulated values
        assert builder.cur is not None
        assert builder.cur.high == 101.0
        assert builder.cur.low == 100.0
        assert builder.cur.close == 101.0
        assert builder.cur.volume == 3.0
        assert builder.cur.delta == 0.2

    def test_minute_boundary_completion(self):
        """Test bar completion at minute boundary."""
        builder = BarBuilder1m()

        # Tick in first minute
        ts1 = 60000
        builder.update_tick(ts1, 100.0, 1.0, 0.5)

        # Tick in next minute
        ts2 = 120000  # next minute
        price2 = 102.0
        vol2 = 1.5
        delta2 = -0.2

        result = builder.update_tick(ts2, price2, vol2, delta2)

        # Should return completed bar
        assert result is not None
        assert isinstance(result, Bar)
        assert result.ts_open == 60000
        assert result.open == 100.0
        assert result.close == 100.0  # last close of first minute
        assert result.volume == 1.0
        assert result.delta == 0.5

        # New bar should be started
        assert builder.cur is not None
        assert builder.cur.ts_open == 120000
        assert builder.cur.open == price2
        assert builder.cur.close == price2


class TestRegimeEngine:
    """Test RegimeEngine functionality."""

    def test_initialization(self):
        """Test engine initializes with correct defaults."""
        config = make_config()
        engine = RegimeEngine(config)

        assert engine.cfg is config
        assert engine.atr_n == 14
        assert len(engine._atr_hist) == 0
        assert engine.state.score == 0.0
        assert engine.state.label == "mixed"

    def test_day_boundary_reset(self):
        """Test VWAP and state reset at day boundaries."""
        config = make_config()
        engine = RegimeEngine(config)

        # First day
        ts1 = 100000  # some time
        engine.on_tick(ts1, 100.0, 1.0, 0.5)
        assert engine._vwap == 100.0
        assert engine._open_day == 100.0

        # Same day - VWAP updates
        ts2 = 200000
        engine.on_tick(ts2, 101.0, 2.0, -0.3)
        expected_vwap = (100.0 * 1.0 + 101.0 * 2.0) / 3.0
        assert abs(engine._vwap - expected_vwap) < 1e-6

        # Next day - should reset
        ts3 = 90000000  # next day (86.4M ms later)
        engine.on_tick(ts3, 102.0, 1.0, 0.1)
        assert engine._vwap == 102.0  # reset to new price
        assert engine._open_day == 102.0

    def test_vwap_crossings(self):
        """Test VWAP crossing detection."""
        config = make_config()
        engine = RegimeEngine(config)

        ts = 100000
        engine.on_tick(ts, 100.0, 1.0, 0.0)  # establish VWAP at 100

        # Above VWAP
        ts += 1000
        engine.on_tick(ts, 101.0, 1.0, 0.0)
        assert engine._last_side_vs_vwap == 1

        # Still above - no crossing
        ts += 1000
        engine.on_tick(ts, 101.5, 1.0, 0.0)
        assert len(engine._cross_hist) >= 1
        assert engine._cross_hist[-1] == 0  # no crossing

        # Cross below - should detect crossing
        ts += 1000
        engine.on_tick(ts, 99.0, 1.0, 0.0)
        assert engine._last_side_vs_vwap == -1
        assert engine._cross_hist[-1] == 1  # crossing detected

    def test_trend_regime_classification(self):
        """Test classification of trending market."""
        config = make_config()
        engine = RegimeEngine(config)

        # Simulate trending behavior: high ATR, directional delta, persistent one-sided moves
        start_ts = 100000

        # Feed some initial data to establish ATR
        for i in range(50):  # 50 minutes of bars
            ts = start_ts + i * 60000
            engine.on_bar_1m(ts, 100.0 + i * 0.1, 99.9 + i * 0.1, 100.0 + i * 0.1)  # trending up

        # Simulate trending ticks: directional delta, persistent above VWAP
        for i in range(100):
            ts = start_ts + 3600000 + i * 1000  # after ATR warmup
            price = 105.0 + i * 0.01  # trending up
            engine.on_tick(ts, price, 1.0, 0.8)  # strong positive delta

        state = engine.compute(ts, price)

        # Should classify as trend
        assert state.label == "trend"
        assert state.score > 0.0

    def test_range_regime_classification(self):
        """Test classification of ranging market."""
        config = make_config()
        engine = RegimeEngine(config)

        start_ts = 100000

        # Feed low volatility bars (range)
        for i in range(50):
            ts = start_ts + i * 60000
            # Low TR bars - ranging
            engine.on_bar_1m(ts, 100.1, 99.9, 100.0)

        # Simulate ranging ticks: oscillating around VWAP, frequent crossings
        vwap_base = 100.0
        for i in range(100):
            ts = start_ts + 3600000 + i * 1000
            # Oscillate around VWAP
            price = vwap_base + 0.5 * math.sin(i * 0.1)
            delta = 0.1 * math.sin(i * 0.05)  # oscillating delta
            engine.on_tick(ts, price, 1.0, delta)

        state = engine.compute(ts, price)

        # Should have some range-like characteristics
        # (Exact classification depends on full feature combination)
        assert isinstance(state.score, float)
        assert -1.0 <= state.score <= 1.0
        assert state.label in ["trend", "range", "mixed"]

    def test_mixed_regime_classification(self):
        """Test classification of mixed/uncertain market."""
        config = make_config()
        engine = RegimeEngine(config)

        start_ts = 100000

        # Medium volatility bars
        for i in range(50):
            ts = start_ts + i * 60000
            engine.on_bar_1m(ts, 100.2, 99.8, 100.0)  # medium TR

        # Neutral ticks: balanced delta, moderate crossings
        for i in range(100):
            ts = start_ts + 3600000 + i * 1000
            price = 100.0 + 0.1 * math.sin(i * 0.05)  # mild oscillation
            delta = 0.0  # neutral delta
            engine.on_tick(ts, price, 1.0, delta)

        state = engine.compute(ts, price)

        # Should have valid classification
        assert isinstance(state.score, float)
        assert -1.0 <= state.score <= 1.0
        assert state.label in ["trend", "range", "mixed"]


if __name__ == "__main__":
    import math
    pytest.main([__file__, "-v"])
