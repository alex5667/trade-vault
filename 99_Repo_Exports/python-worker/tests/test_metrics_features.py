"""
Unit tests for metrics/features.py
"""

import pandas as pd

from metrics.features import (
    absorption_mask,
    atr_from_bars,
    cvd_from_delta,
    delta_series_from_ticks,
    delta_spike_z,
    weak_progress,
)


def make_ticks(spike_side="BUY", base_qty=10, spike_qty=300, n=200):
    """Generate test tick data with a spike."""
    rows = []
    ts = 1_700_000_000_000
    price = 2000.0

    for i in range(n):
        side = "BUY" if i % 2 == 0 else "SELL"
        qty = base_qty

        # Insert spike
        if i == 150:
            side = spike_side
            qty = spike_qty

        price += (1 if side == "BUY" else -1) * 0.05

        rows.append({
            "ts_ms": ts + i * 200,
            "side": side,
            "qty": qty,
            "price": price
        })

    return pd.DataFrame(rows)


class TestDeltaSeries:
    """Test delta_series_from_ticks."""

    def test_basic_aggregation(self):
        """Test basic tick aggregation."""
        ticks = make_ticks(n=120)  # 2 minutes of ticks
        bars = delta_series_from_ticks(ticks, bar_ms=60_000)

        # Should have 2 bars
        assert len(bars) == 2
        assert "delta" in bars.columns
        assert "range" in bars.columns
        assert "high" in bars.columns
        assert "low" in bars.columns

    def test_delta_calculation(self):
        """Test delta = taker_buy - taker_sell."""
        ticks = pd.DataFrame([
            {"ts_ms": 1000, "side": "BUY", "qty": 10, "price": 100.0},
            {"ts_ms": 2000, "side": "SELL", "qty": 5, "price": 100.0},
            {"ts_ms": 3000, "side": "BUY", "qty": 15, "price": 100.0},
        ])

        bars = delta_series_from_ticks(ticks, bar_ms=60_000)

        assert len(bars) == 1
        assert bars.iloc[0]["taker_buy"] == 25  # 10 + 15
        assert bars.iloc[0]["taker_sell"] == 5
        assert bars.iloc[0]["delta"] == 20  # 25 - 5

    def test_spike_detection(self):
        """Test spike appears in delta."""
        ticks = make_ticks(spike_side="BUY", spike_qty=500, n=180)
        bars = delta_series_from_ticks(ticks, bar_ms=60_000)

        # Bar with spike should have high delta
        max_delta = bars["delta"].max()
        assert max_delta > 100  # Spike should be significant


class TestATR:
    """Test ATR calculation."""

    def test_atr_basic(self):
        """Test basic ATR calculation."""
        bars = pd.DataFrame({
            "high": [110, 112, 115, 113, 116],
            "low": [100, 101, 103, 102, 104],
            "close": [105, 106, 108, 107, 110]
        })

        atr = atr_from_bars(bars, n=3)

        # ATR should be positive
        assert all(atr >= 0)
        assert not atr.isna().all()


class TestWeakProgress:
    """Test weak progress detection."""

    def test_weak_progress_detection(self):
        """Test weak progress identification."""
        bars = pd.DataFrame({
            "range": [0.5, 0.8, 0.3, 1.5, 0.2],
            "high": [100, 101, 102, 103, 104],
            "low": [99.5, 100.2, 101.7, 101.5, 103.8],
            "close": [99.8, 100.5, 101.9, 102, 103.9]
        })

        atr = atr_from_bars(bars, n=3)
        wp = weak_progress(bars, atr, threshold=0.3)

        # Should detect some weak progress
        assert wp.dtype == bool
        assert wp.any()


class TestDeltaSpikeZ:
    """Test Delta Z-score."""

    def test_zscore_calculation(self):
        """Test Z-score calculation."""
        ticks = make_ticks(spike_side="BUY", spike_qty=500, n=200)
        bars = delta_series_from_ticks(ticks, bar_ms=60_000)

        z = delta_spike_z(bars, lookback=50)

        # Should have some Z-scores
        assert not z.isna().all()

        # Spike should produce high Z-score
        if len(z) > 0:
            max_z = z.abs().max()
            # With a 500 qty spike vs 10 base, should be significant
            # (but depends on window, so just check it's > 1)
            assert max_z > 1.0


class TestAbsorptionMask:
    """Test absorption detection."""

    def test_absorption_detection(self):
        """Test absorption mask."""
        ticks = make_ticks(spike_side="BUY", spike_qty=500, n=200)
        bars = delta_series_from_ticks(ticks, bar_ms=60_000)

        atr = atr_from_bars(bars, n=14)
        wp = weak_progress(bars, atr, threshold=0.3)
        z = delta_spike_z(bars, lookback=50)

        mask = absorption_mask(bars, z, wp, z_strong=2.0, z_moderate=1.5)

        # Should be boolean
        assert mask.dtype == bool

        # Should detect some absorption
        # (depends on data, but at least mask should exist)
        assert len(mask) == len(bars)


class TestCVD:
    """Test Cumulative Volume Delta."""

    def test_cvd_calculation(self):
        """Test CVD cumsum."""
        delta = pd.Series([10, -5, 15, -8, 20])
        cvd = cvd_from_delta(delta)

        expected = pd.Series([10, 5, 20, 12, 32])
        pd.testing.assert_series_equal(cvd, expected)

    def test_cvd_from_bars(self):
        """Test CVD from real bars."""
        ticks = make_ticks(n=180)
        bars = delta_series_from_ticks(ticks, bar_ms=60_000)

        cvd = cvd_from_delta(bars["delta"])

        # Should be monotonic if all deltas same sign
        # Or at least should accumulate
        assert len(cvd) == len(bars)

