"""Tests for TPSizeFractionCalibrator (W5)."""
import time
from core.tp_size_fraction_calibrator import TPSizeFractionCalibrator

_DEF = (0.334, 0.333, 0.333)  # mirrors _DEFAULT_FRACTIONS


def _ts() -> int:
    return int(time.time() * 1000)


def _feed(cal: TPSizeFractionCalibrator, *, regime="trending",
          tp1=0, tp2=0, tp3=0) -> None:
    cal.recompute_gap_ms = 0
    ts = _ts()
    for i in range(tp1):
        cal.observe_tp(regime=regime, tp_level=1, ts_ms=ts + i)
    for i in range(tp2):
        cal.observe_tp(regime=regime, tp_level=2, ts_ms=ts + tp1 + i)
    for i in range(tp3):
        cal.observe_tp(regime=regime, tp_level=3, ts_ms=ts + tp1 + tp2 + i)


class TestTPSizeFractionCalibratorShadow:
    def test_default_fractions_when_enforce_off(self):
        cal = TPSizeFractionCalibrator(enforce=False)
        f1, f2, f3 = cal.get_fractions(regime="trending")
        assert abs(f1 + f2 + f3 - 1.0) < 0.01
        assert abs(f1 - _DEF[0]) < 1e-6

    def test_shadow_no_effect(self):
        cal = TPSizeFractionCalibrator(enforce=False, auto_enforce=False, min_samples=5)
        _feed(cal, tp1=60, tp2=30, tp3=10)
        f1, _, _ = cal.get_fractions(regime="trending")
        assert abs(f1 - _DEF[0]) < 1e-6


class TestTPSizeFractionCalibratorEnforce:
    def test_fractions_sum_to_one(self):
        cal = TPSizeFractionCalibrator(enforce=True, min_samples=10)
        _feed(cal, tp1=50, tp2=30, tp3=20)
        f1, f2, f3 = cal.get_fractions(regime="trending")
        assert abs(f1 + f2 + f3 - 1.0) < 1e-6

    def test_majority_tp1_reflected(self):
        cal = TPSizeFractionCalibrator(enforce=True, min_samples=10)
        _feed(cal, tp1=90, tp2=5, tp3=5)
        f1, f2, f3 = cal.get_fractions(regime="trending")
        assert f1 > f2 and f1 > f3

    def test_floor_prevents_zero_fractions(self):
        cal = TPSizeFractionCalibrator(enforce=True, min_samples=5)
        # All tp3 — floor keeps tp1/tp2 non-zero even though raw count=0
        _feed(cal, tp3=100)
        f1, f2, f3 = cal.get_fractions(regime="trending")
        assert f1 > 0
        assert f2 > 0
        assert f3 > 0
        assert abs(f1 + f2 + f3 - 1.0) < 1e-6


class TestTPSizeFractionCalibratorFallback:
    def test_wildcard_regime_fallback(self):
        cal = TPSizeFractionCalibrator(enforce=True, min_samples=5)
        _feed(cal, regime="*", tp1=50, tp2=30, tp3=20)
        f1, _, _ = cal.get_fractions(regime="ranging")
        assert f1 > 0

    def test_unknown_regime_returns_default(self):
        cal = TPSizeFractionCalibrator(enforce=True, min_samples=100)
        f1, f2, f3 = cal.get_fractions(regime="unknown_xzxz")
        assert abs(f1 - _DEF[0]) < 1e-6


class TestTPSizeFractionCalibratorAutoEnforce:
    def test_auto_enforce_promotes_after_warmup(self):
        cal = TPSizeFractionCalibrator(enforce=False, auto_enforce=True, min_samples=5)
        _feed(cal, tp1=60, tp2=30, tp3=10)
        f1, f2, f3 = cal.get_fractions(regime="trending")
        assert abs(f1 + f2 + f3 - 1.0) < 1e-6
        # After warmup auto_enforce promotes: tp1-heavy → f1 > default
        assert f1 > _DEF[0]

    def test_auto_enforce_roundtrip(self):
        snap = TPSizeFractionCalibrator(auto_enforce=True).snapshot()
        assert snap["auto_enforce"] is True
        cal2 = TPSizeFractionCalibrator(auto_enforce=False)
        cal2.load_state(snap)
        assert cal2.auto_enforce is True


class TestTPSizeFractionCalibratorSnapshot:
    def test_roundtrip(self):
        cal = TPSizeFractionCalibrator(enforce=True, min_samples=5)
        _feed(cal, tp1=40, tp2=35, tp3=25)
        snap = cal.snapshot()
        cal2 = TPSizeFractionCalibrator(enforce=False)
        cal2.load_state(snap)
        assert cal2.enforce is True
        assert len(cal2._bins) > 0

    def test_schema_version(self):
        cal = TPSizeFractionCalibrator()
        assert cal.snapshot()["schema_version"] == 1

    def test_invalid_tp_level_ignored(self):
        cal = TPSizeFractionCalibrator(enforce=True)
        cal.observe_tp(regime="trending", tp_level=99, ts_ms=_ts())
        assert all(b.total == 0 for b in cal._bins.values())
