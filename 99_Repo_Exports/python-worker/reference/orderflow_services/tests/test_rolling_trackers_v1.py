"""
Unit tests for A3 rolling trackers.

Tests cover:
  - RollingWindow: push, eviction, out-of-order rejection
  - WeightedRollingWindow: push, eviction
  - RollingVWAPTracker: basic, no-data, eviction
  - RollingMomentumTracker: price_momentum_bps, spread_momentum_bps_per_s
  - RollingVolatilityTracker: realized_vol_bps, no-data

All tests are stateless (no I/O, no Redis, no imports from services.*).
"""
import math
import os
import sys

# Add python-worker root to path so 'core.*' is importable
# Depth: tests -> orderflow -> services -> tick_flow_full -> python-worker (4 levels)
_PW_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
if _PW_ROOT not in sys.path:
    sys.path.insert(0, _PW_ROOT)


from core.rolling_momentum_tracker import RollingMomentumTracker
from core.rolling_volatility_tracker import RollingVolatilityTracker
from core.rolling_vwap_tracker import RollingVWAPTracker
from core.rolling_window import RollingWindow, WeightedRollingWindow

# ──────────────────────────────────────────────────────────
# RollingWindow
# ──────────────────────────────────────────────────────────

class TestRollingWindow:
    def test_push_and_len(self):
        w: RollingWindow[float] = RollingWindow(horizon_ms=10_000, maxlen=16)
        assert w.push(1000, 1.0)
        assert w.push(2000, 2.0)
        assert len(w) == 2

    def test_eviction(self):
        w: RollingWindow[float] = RollingWindow(horizon_ms=5_000, maxlen=64)
        w.push(1000, 10.0)
        w.push(2000, 20.0)
        # now push at 7000: horizon=5000 => cutoff=2000, item at 1000 dropped
        w.push(7000, 70.0)
        items = list(w)
        assert all(ts >= 2000 for ts, _ in items), f"stale item not evicted: {items}"

    def test_out_of_order_rejected(self):
        w: RollingWindow[float] = RollingWindow(horizon_ms=10_000, maxlen=64)
        w.push(5000, 50.0)
        ok = w.push(4000, 40.0)
        assert not ok
        assert w.bad_time_total == 1
        assert len(w) == 1

    def test_zero_ts_rejected(self):
        w: RollingWindow[float] = RollingWindow(horizon_ms=10_000, maxlen=64)
        ok = w.push(0, 1.0)
        assert not ok
        assert w.bad_time_total == 1

    def test_maxlen_bound(self):
        w: RollingWindow[float] = RollingWindow(horizon_ms=9_999_999, maxlen=4)
        for i in range(10):
            w.push(i * 1000 + 1000, float(i))
        assert len(w) <= 4


# ──────────────────────────────────────────────────────────
# WeightedRollingWindow
# ──────────────────────────────────────────────────────────

class TestWeightedRollingWindow:
    def test_weighted_push(self):
        w = WeightedRollingWindow(horizon_ms=60_000, maxlen=32)
        w.push(1000, 100.0, 5.0)
        w.push(2000, 200.0, 10.0)
        assert len(w) == 2

    def test_weighted_eviction(self):
        w = WeightedRollingWindow(horizon_ms=5_000, maxlen=64)
        w.push(1000, 100.0, 1.0)
        w.push(7000, 200.0, 2.0)
        items = list(w)
        assert all(ts >= 2000 for ts, _, _ in items)


# ──────────────────────────────────────────────────────────
# RollingVWAPTracker
# ──────────────────────────────────────────────────────────

class TestRollingVWAPTracker:
    def _make(self, horizon_ms=120_000):
        return RollingVWAPTracker(horizon_ms=horizon_ms, maxlen=64)

    def test_no_data_on_start(self):
        t = self._make()
        snap = t.last_snapshot
        assert snap["vwap_roll_no_data"] == 1.0
        assert snap["roll_vwap_px"] == 0.0

    def test_one_bar_no_data(self):
        """Single bar: cannot compute diff (need ref), so still no_data."""
        t = self._make()
        snap = t.update(ts_ms=1000, vwap=100.0, vol=10.0, ref_px=100.0)
        # 1 point: we have sum_pv/sum_v => valid
        assert snap["vwap_roll_no_data"] == 0.0
        assert math.isclose(snap["roll_vwap_px"], 100.0)
        # diff = (ref_px - vwap) / ref_px * 10000 = 0
        assert math.isclose(snap["vwap_roll_diff_bps"], 0.0)

    def test_vwap_diff_bps_basic(self):
        """ref_px higher than rolling VWAP => positive diff."""
        t = self._make()
        t.update(ts_ms=1000, vwap=99.0, vol=10.0, ref_px=100.0)
        snap = t.update(ts_ms=2000, vwap=99.0, vol=10.0, ref_px=100.0)
        # roll_vwap = 99.0, ref_px=100.0 => diff = (100-99)/100 * 10000 = 100 bps
        assert snap["vwap_roll_no_data"] == 0.0
        assert math.isclose(snap["vwap_roll_diff_bps"], 100.0, rel_tol=1e-4)

    def test_eviction(self):
        """After eviction window, old entries are removed."""
        t = RollingVWAPTracker(horizon_ms=2_000, maxlen=64)
        t.update(ts_ms=1000, vwap=50.0, vol=5.0, ref_px=55.0)
        # push at 4000: horizon=2000 => cutoff=2000, item at 1000 evicted
        snap = t.update(ts_ms=4000, vwap=55.0, vol=5.0, ref_px=55.0)
        # only second bar in window: vwap=55, ref_px=55 => diff=0
        assert snap["vwap_roll_no_data"] == 0.0

    def test_out_of_order_returns_last(self):
        """Out-of-order ts_ms: returns last good snapshot unchanged."""
        t = self._make()
        snap1 = t.update(ts_ms=5000, vwap=100.0, vol=10.0, ref_px=100.0)
        snap2 = t.update(ts_ms=3000, vwap=200.0, vol=10.0, ref_px=200.0)
        assert snap2 == snap1  # last snapshot unchanged

    def test_zero_vol_skipped(self):
        """Zero-volume bars: pushed as (ts, 0, 0) but sum_v=0 => no_data."""
        t = self._make()
        snap = t.update(ts_ms=1000, vwap=0.0, vol=0.0, ref_px=100.0)
        assert snap["vwap_roll_no_data"] == 1.0


# ──────────────────────────────────────────────────────────
# RollingMomentumTracker
# ──────────────────────────────────────────────────────────

class TestRollingMomentumTracker:
    def _make(self, horizon_ms=60_000):
        return RollingMomentumTracker(horizon_ms=horizon_ms, maxlen=64)

    def test_no_data_single_bar(self):
        t = self._make()
        snap = t.update(ts_ms=1000, px=100.0, spread_bps=5.0)
        assert snap["price_momentum_no_data"] == 1.0  # need >=2 points

    def test_price_momentum_up(self):
        t = self._make()
        t.update(ts_ms=1000, px=100.0, spread_bps=5.0)
        snap = t.update(ts_ms=2000, px=110.0, spread_bps=5.0)
        # mom = (110-100)/110 * 10000 = 909 bps
        assert snap["price_momentum_no_data"] == 0.0
        expected = (110.0 - 100.0) / 110.0 * 10_000.0
        assert math.isclose(snap["price_momentum_bps"], expected, rel_tol=1e-4)

    def test_spread_momentum(self):
        t = self._make()
        t.update(ts_ms=1000, px=100.0, spread_bps=5.0)
        snap = t.update(ts_ms=3000, px=100.0, spread_bps=7.0)
        # d(spread)/dt_sec = (7-5) / 2.0 = 1.0 bps/s
        assert snap["spread_momentum_no_data"] == 0.0
        assert math.isclose(snap["spread_momentum_bps_per_s"], 1.0, rel_tol=1e-5)

    def test_out_of_order_ignored(self):
        t = self._make()
        snap0 = t.update(ts_ms=5000, px=100.0, spread_bps=5.0)
        snap1 = t.update(ts_ms=3000, px=120.0, spread_bps=8.0)
        assert snap1 == snap0


# ──────────────────────────────────────────────────────────
# RollingVolatilityTracker
# ──────────────────────────────────────────────────────────

class TestRollingVolatilityTracker:
    def _make(self, horizon_ms=120_000):
        return RollingVolatilityTracker(horizon_ms=horizon_ms, maxlen=64)

    def test_no_data_fewer_than_3(self):
        t = self._make()
        t.update(ts_ms=1000, px=100.0)
        snap = t.update(ts_ms=2000, px=101.0)
        assert snap["realized_vol_no_data"] == 1.0  # need >=3

    def test_vol_computed_after_3(self):
        t = self._make()
        t.update(ts_ms=1000, px=100.0)
        t.update(ts_ms=2000, px=101.0)
        snap = t.update(ts_ms=3000, px=102.0)
        assert snap["realized_vol_no_data"] == 0.0
        assert snap["realized_vol_bps"] >= 0.0

    def test_constant_price_zero_vol(self):
        """Constant price => log-returns all 0 => vol=0 bps."""
        t = self._make()
        for i in range(5):
            t.update(ts_ms=(i + 1) * 1000, px=100.0)
        snap = t.last_snapshot
        assert snap["realized_vol_no_data"] == 0.0
        assert math.isclose(snap["realized_vol_bps"], 0.0, abs_tol=1e-8)

    def test_out_of_order_ignored(self):
        t = self._make()
        snap0 = t.update(ts_ms=5000, px=100.0)
        snap1 = t.update(ts_ms=3000, px=120.0)
        assert snap1 == snap0
