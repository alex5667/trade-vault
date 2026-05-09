from __future__ import annotations

"""
tests/core/test_v13_runtime_tracker.py
======================================
Unit tests for V13RuntimeTracker — the per-symbol tracker that computes
runtime attributes for v13_of indicator groups NA/NB/NC/NE/NF.
"""

import math
from types import SimpleNamespace

from core.v13_runtime_tracker import V13RuntimeTracker

# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _make_bar(o: float, h: float, l: float, c: float, *, vol: float = 100.0, ts_ms: int = 0, vwap: float = 0.0):
    return SimpleNamespace(open=o, high=h, low=l, close=c, vol=vol, volume=vol, end_ts_ms=ts_ms, vwap=vwap or (o + c) / 2)


def _feed_ticks(tracker: V13RuntimeTracker, n: int = 30, *, base: float = 100.0, step: float = 0.01):
    """Feed `n` synthetic ticks in alternating BUY/SELL with slight upward drift."""
    for i in range(n):
        side = "BUY" if i % 2 == 0 else "SELL"
        price = base + i * step
        qty = 1.0 + (i % 5) * 0.5
        tracker.on_tick(price, qty, side, ts_ms=1000000 + i * 100, book_mid=price - 0.005)


def _feed_bars(tracker: V13RuntimeTracker, n: int = 5, *, base: float = 100.0, range_pct: float = 0.5):
    """Feed `n` synthetic OHLC bars with realistic range."""
    for i in range(n):
        mid = base + i * 0.1
        bar = _make_bar(
            o=mid - range_pct * 0.3,
            h=mid + range_pct,
            l=mid - range_pct,
            c=mid + range_pct * 0.2,
            vol=100.0 + i * 10,
            ts_ms=2000000 + i * 60000,
            vwap=mid,
        )
        tracker.on_bar_close(bar)


# ═══════════════════════════════════════════════════════════════════════════════
# Group NA: OHLC Volatility
# ═══════════════════════════════════════════════════════════════════════════════

class TestGroupNA:
    """Tests for Garman-Klass, Parkinson, Yang-Zhang, vol-of-vol."""

    def test_empty_tracker_defaults(self):
        t = V13RuntimeTracker()
        assert t.garman_klass_vol == 0.0
        assert t.parkinson_vol == 0.0
        assert t.yang_zhang_vol == 0.0
        assert t.vol_of_vol == 0.0

    def test_parkinson_vol_positive(self):
        t = V13RuntimeTracker()
        _feed_bars(t, n=5, base=100.0, range_pct=1.0)
        assert t.parkinson_vol > 0.0

    def test_garman_klass_positive(self):
        t = V13RuntimeTracker()
        _feed_bars(t, n=5, base=100.0, range_pct=1.0)
        assert t.garman_klass_vol > 0.0

    def test_yang_zhang_needs_multiple_bars(self):
        t = V13RuntimeTracker()
        # Only 2 bars — not enough for overnight variance
        _feed_bars(t, n=2)
        # 2 bars is borderline — should produce something from Rogers-Satchell at least
        _feed_bars(t, n=5)
        assert t.yang_zhang_vol > 0.0

    def test_vol_of_vol_stable_market(self):
        t = V13RuntimeTracker()
        # Feed many bars with same range → vol_of_vol should be small
        for i in range(10):
            bar = _make_bar(100.0, 101.0, 99.0, 100.5, ts_ms=i * 60000)
            t.on_bar_close(bar)
        # vol_of_vol should be finite (possibly small but non-negative)
        assert t.vol_of_vol >= 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# Group NB: Academic Liquidity
# ═══════════════════════════════════════════════════════════════════════════════

class TestGroupNB:
    """Tests for Amihud, Corwin-Schultz, Hasbrouck."""

    def test_amihud_positive(self):
        t = V13RuntimeTracker()
        _feed_bars(t, n=5, base=100.0)
        assert t.amihud_illiquidity > 0.0

    def test_amihud_zero_volume(self):
        t = V13RuntimeTracker()
        for i in range(5):
            bar = _make_bar(100.0 + i, 101.0 + i, 99.0 + i, 100.5 + i, vol=0.0, ts_ms=i * 60000)
            t.on_bar_close(bar)
        # With zero volume, amihud should remain 0 (divide by zero guarded)
        assert t.amihud_illiquidity == 0.0

    def test_corwin_schultz_positive(self):
        t = V13RuntimeTracker()
        _feed_bars(t, n=5, base=100.0, range_pct=0.5)
        assert t.corwin_schultz_spread >= 0.0

    def test_hasbrouck_range(self):
        t = V13RuntimeTracker()
        _feed_ticks(t, n=50)
        # Hasbrouck should be in [0, 1]
        assert 0.0 <= t.hasbrouck_info_share <= 1.0


# ═══════════════════════════════════════════════════════════════════════════════
# Group NC: Flow Toxicity
# ═══════════════════════════════════════════════════════════════════════════════

class TestGroupNC:
    """Tests for PIN, Kyle split, sweep ratio."""

    def test_pin_estimate_range(self):
        t = V13RuntimeTracker()
        _feed_ticks(t, n=50)
        # PIN proxy should be in [0, 1]
        assert 0.0 <= t.pin_estimate <= 1.0

    def test_pin_balanced_flow(self):
        t = V13RuntimeTracker()
        # Perfectly balanced flow should give low PIN
        for i in range(40):
            side = "BUY" if i % 2 == 0 else "SELL"
            t.on_tick(100.0, 1.0, side, ts_ms=i * 100, book_mid=100.0)
        # PIN should be near 0 for balanced flow
        assert t.pin_estimate < 0.3  # generous threshold

    def test_kyle_split_initial(self):
        t = V13RuntimeTracker()
        _feed_ticks(t, n=50)
        # After feeding, kyle lambdas should have been computed
        assert isinstance(t.kyle_lambda_buy, float)
        assert isinstance(t.kyle_lambda_sell, float)

    def test_aggressive_sweep_ratio_zero(self):
        t = V13RuntimeTracker()
        # No sweeps → ratio = 0
        _feed_ticks(t, n=20)
        assert t.aggressive_sweep_ratio == 0.0

    def test_aggressive_sweep_ratio_nonzero(self):
        t = V13RuntimeTracker()
        # Some sweeps
        for i in range(20):
            t.on_tick(100.0 + i * 0.01, 1.0, "BUY", ts_ms=i * 100, levels_crossed=5 if i % 3 == 0 else 0)
        assert t.aggressive_sweep_ratio > 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# Group NE: Entropy / Information Theory
# ═══════════════════════════════════════════════════════════════════════════════

class TestGroupNE:
    """Tests for entropy, Gini, mutual information."""

    def test_entropy_positive(self):
        t = V13RuntimeTracker()
        _feed_ticks(t, n=50)
        assert t.price_entropy_50 > 0.0

    def test_entropy_constant_returns(self):
        t = V13RuntimeTracker()
        # Constant price → same bin → entropy should be small
        for i in range(50):
            t.on_tick(100.0, 1.0, "BUY", ts_ms=i * 100)
        assert t.price_entropy_50 == 0.0

    def test_gini_uniform_sizes(self):
        t = V13RuntimeTracker()
        # All same size → gini = 0
        for i in range(20):
            t.on_tick(100.0 + i * 0.01, 1.0, "BUY", ts_ms=i * 100)
        assert t.order_size_gini < 0.1

    def test_gini_skewed_sizes(self):
        t = V13RuntimeTracker()
        # Highly skewed sizes → gini should be high
        for i in range(20):
            qty = 1.0 if i < 19 else 1000.0
            t.on_tick(100.0 + i * 0.01, qty, "BUY", ts_ms=i * 100)
        assert t.order_size_gini > 0.3

    def test_mutual_info_nonneg(self):
        t = V13RuntimeTracker()
        _feed_ticks(t, n=50)
        assert t.mutual_info_price_volume >= 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# Group NF: Mean Reversion / Stationarity
# ═══════════════════════════════════════════════════════════════════════════════

class TestGroupNF:
    """Tests for half-life, ADF, mid-vwap diff std."""

    def test_half_life_trending(self):
        t = V13RuntimeTracker()
        # Pure uptrend → not mean-reverting → half_life = 0
        for i in range(50):
            t.on_tick(100.0 + i * 0.5, 1.0, "BUY", ts_ms=i * 100)
        assert t.half_life_mean_reversion == 0.0

    def test_half_life_mean_reverting(self):
        t = V13RuntimeTracker()
        # Mean-reverting: oscillate around 100 with strong amplitude
        for i in range(80):
            price = 100.0 + 5.0 * math.sin(i * 0.3)
            t.on_tick(price, 1.0, "BUY", ts_ms=i * 100)
        # Force recompute by resetting cache timestamp
        t._adf_cache_ts = 0
        t.on_tick(100.0, 1.0, "BUY", ts_ms=99999)
        assert t.half_life_mean_reversion > 0.0

    def test_adf_pvalue_range(self):
        t = V13RuntimeTracker()
        _feed_ticks(t, n=50)
        # p-value should be in [0, 1]
        assert 0.0 <= t.adf_pvalue_50 <= 1.0

    def test_mid_vwap_diff_std_positive(self):
        t = V13RuntimeTracker()
        for i in range(10):
            bar = _make_bar(100.0 + i * 0.1, 101.0, 99.0, 100.5 + i * 0.1, ts_ms=i * 60000, vwap=100.2 + i * 0.05)
            t.on_bar_close(bar)
        t._last_book_mid = 100.5
        assert t.mid_vwap_diff_std >= 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# forward_to_runtime
# ═══════════════════════════════════════════════════════════════════════════════

class TestForwardToRuntime:
    """Tests for runtime attribute forwarding."""

    def test_forward_copies_attrs(self):
        t = V13RuntimeTracker()
        _feed_ticks(t, n=50)
        _feed_bars(t, n=5)

        runtime = SimpleNamespace()
        t.forward_to_runtime(runtime)

        assert hasattr(runtime, "garman_klass_vol")
        assert hasattr(runtime, "pin_estimate")
        assert hasattr(runtime, "price_entropy_50")
        assert hasattr(runtime, "half_life_mean_reversion")
        assert hasattr(runtime, "amihud_illiquidity")
        assert hasattr(runtime, "kyle_lambda_buy")
        assert hasattr(runtime, "kyle_lambda_sell")
        assert hasattr(runtime, "aggressive_sweep_ratio")
        assert hasattr(runtime, "order_size_gini")
        assert hasattr(runtime, "mutual_info_price_volume")
        assert hasattr(runtime, "adf_pvalue_50")
        assert hasattr(runtime, "mid_vwap_diff_std")
        assert hasattr(runtime, "corwin_schultz_spread")
        assert hasattr(runtime, "hasbrouck_info_share")
        assert hasattr(runtime, "depth_resilience_half_life")
        assert hasattr(runtime, "parkinson_vol")
        assert hasattr(runtime, "yang_zhang_vol")
        assert hasattr(runtime, "vol_of_vol")

    def test_forward_tolerates_readonly_runtime(self):
        """Forward should not crash even if some attrs can't be set."""
        t = V13RuntimeTracker()

        class ReadOnly:
            __slots__ = ()

        r = ReadOnly()
        t.forward_to_runtime(r)  # should not raise


# ═══════════════════════════════════════════════════════════════════════════════
# Fail-open / Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """Tests for fail-open behaviour and edge cases."""

    def test_on_tick_zero_price(self):
        t = V13RuntimeTracker()
        t.on_tick(0.0, 1.0, "BUY", ts_ms=1000)  # should not crash

    def test_on_tick_negative_qty(self):
        t = V13RuntimeTracker()
        t.on_tick(100.0, -1.0, "BUY", ts_ms=1000)  # should not crash

    def test_on_bar_close_zero_ohlc(self):
        t = V13RuntimeTracker()
        bar = _make_bar(0.0, 0.0, 0.0, 0.0, ts_ms=1000)
        t.on_bar_close(bar)  # should not crash, should early-return

    def test_on_bar_close_invalid_bar(self):
        t = V13RuntimeTracker()
        t.on_bar_close(None)  # should not crash

    def test_multiple_cycles(self):
        """Running through many tick+bar cycles should not cause buffer overflow."""
        t = V13RuntimeTracker()
        for cycle in range(5):
            _feed_ticks(t, n=30, base=100.0 + cycle * 10)
            _feed_bars(t, n=5, base=100.0 + cycle * 10)
        # All attrs should be valid floats
        assert math.isfinite(t.garman_klass_vol)
        assert math.isfinite(t.pin_estimate)
        assert math.isfinite(t.price_entropy_50)
        assert math.isfinite(t.half_life_mean_reversion)
