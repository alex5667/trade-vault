"""Tests for SLATRFloorCalibrator (W4)."""
import time
from core.sl_atr_floor_calibrator import SLATRFloorCalibrator


def _ts() -> int:
    return int(time.time() * 1000)


def _feed(cal: SLATRFloorCalibrator, *, symbol="BTCUSDT", venue="binance",
          sl_bps_vals: list[float], atr_bps_vals: list[float] | None = None) -> None:
    cal.recompute_gap_ms = 0
    ts = _ts()
    if atr_bps_vals is None:
        atr_bps_vals = [100.0] * len(sl_bps_vals)
    for i, (sl, atr) in enumerate(zip(sl_bps_vals, atr_bps_vals)):
        cal.observe(symbol=symbol, venue=venue, sl_bps=sl, atr_bps=atr,
                    ts_ms=ts + i * 1000)


class TestSLATRFloorCalibratorShadow:
    def test_default_when_enforce_off(self):
        cal = SLATRFloorCalibrator(enforce=False)
        assert cal.get_floor(symbol="BTC", venue="binance") == cal.default_floor

    def test_shadow_no_effect(self):
        cal = SLATRFloorCalibrator(enforce=False, auto_enforce=False, min_samples=3)
        _feed(cal, sl_bps_vals=[200.0] * 20)
        assert cal.get_floor(symbol="BTCUSDT", venue="binance") == cal.default_floor


class TestSLATRFloorCalibratorEnforce:
    def test_converges_on_consistent_ratio(self):
        cal = SLATRFloorCalibrator(enforce=True, min_samples=10)
        # SL=80bps, ATR=100bps → ratio=0.8 always
        _feed(cal, sl_bps_vals=[80.0] * 50, atr_bps_vals=[100.0] * 50)
        floor = cal.get_floor(symbol="BTCUSDT", venue="binance")
        # p25(0.8) * min(global_floor, 0.5) → should be ~0.8
        assert 0.5 <= floor <= 1.5

    def test_global_floor_respected(self):
        cal = SLATRFloorCalibrator(enforce=True, min_samples=5)
        # Tiny SL/ATR ratios → floor clamps to global_floor=0.5
        _feed(cal, sl_bps_vals=[1.0] * 50, atr_bps_vals=[100.0] * 50)
        floor = cal.get_floor(symbol="BTCUSDT", venue="binance")
        assert floor >= 0.5

    def test_max_floor_respected(self):
        cal = SLATRFloorCalibrator(enforce=True, min_samples=5)
        # Very high SL/ATR → floor should not exceed max_floor=1.5
        _feed(cal, sl_bps_vals=[200.0] * 50, atr_bps_vals=[100.0] * 50)
        floor = cal.get_floor(symbol="BTCUSDT", venue="binance")
        assert floor <= 1.5


class TestSLATRFloorCalibratorFallback:
    def test_venue_wildcard_fallback(self):
        cal = SLATRFloorCalibrator(enforce=True, min_samples=5)
        _feed(cal, symbol="ETHUSDT", venue="*", sl_bps_vals=[80.0] * 30, atr_bps_vals=[100.0] * 30)
        val = cal.get_floor(symbol="ETHUSDT", venue="bybit")
        assert val == cal.get_floor(symbol="ETHUSDT", venue="*")

    def test_symbol_wildcard_fallback(self):
        cal = SLATRFloorCalibrator(enforce=True, min_samples=5)
        _feed(cal, symbol="*", venue="*", sl_bps_vals=[75.0] * 30, atr_bps_vals=[100.0] * 30)
        val = cal.get_floor(symbol="PEPEUSDT", venue="binance")
        assert val > 0

    def test_unknown_returns_default(self):
        cal = SLATRFloorCalibrator(enforce=True, min_samples=100)
        val = cal.get_floor(symbol="UNKNOWN", venue="binance")
        assert val == cal.default_floor


class TestSLATRFloorCalibratorAutoEnforce:
    def test_auto_enforce_promotes_after_warmup(self):
        cal = SLATRFloorCalibrator(enforce=False, auto_enforce=True, min_samples=5)
        _feed(cal, sl_bps_vals=[80.0] * 20, atr_bps_vals=[100.0] * 20)
        floor = cal.get_floor(symbol="BTCUSDT", venue="binance")
        assert 0.5 <= floor <= 1.5  # calibrated value, not blocked

    def test_auto_enforce_roundtrip(self):
        snap = SLATRFloorCalibrator(auto_enforce=True).snapshot()
        assert snap["auto_enforce"] is True
        cal2 = SLATRFloorCalibrator(auto_enforce=False)
        cal2.load_state(snap)
        assert cal2.auto_enforce is True


class TestSLATRFloorCalibratorSnapshot:
    def test_roundtrip(self):
        cal = SLATRFloorCalibrator(enforce=True, min_samples=5)
        _feed(cal, sl_bps_vals=[90.0] * 20, atr_bps_vals=[100.0] * 20)
        snap = cal.snapshot()
        cal2 = SLATRFloorCalibrator(enforce=False)
        cal2.load_state(snap)
        assert cal2.enforce is True
        assert len(cal2._bins) > 0

    def test_invalid_atr_ignored(self):
        cal = SLATRFloorCalibrator(enforce=True)
        cal.observe(symbol="BTC", venue="binance", sl_bps=80.0, atr_bps=0.0, ts_ms=_ts())
        assert all(b.n_observed == 0 for b in cal._bins.values())

    def test_zero_sl_ignored(self):
        cal = SLATRFloorCalibrator(enforce=True)
        cal.observe(symbol="BTC", venue="binance", sl_bps=0.0, atr_bps=100.0, ts_ms=_ts())
        assert all(b.n_observed == 0 for b in cal._bins.values())
