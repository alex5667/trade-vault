"""Tests for DailyDDPerTierCalibrator (W2)."""
import time
import pytest
from core.daily_dd_per_tier_calibrator import DailyDDPerTierCalibrator


def _ts() -> int:
    return int(time.time() * 1000)


def _feed(cal, *, tier="T1", regime="trending", losses: list[float],
          recompute_gap_ms: int = 0) -> None:
    """Feed daily losses; force recompute by patching gap."""
    if recompute_gap_ms == 0:
        cal.recompute_gap_ms = 0
    ts = _ts()
    for i, loss in enumerate(losses):
        cal.observe_day(tier=tier, regime=regime,
                        date_str=f"2026-05-{i+1:02d}",
                        pnl_pct=-loss, ts_ms=ts + i * 86_400_000)


class TestDailyDDPerTierCalibratorAutoEnforce:
    def test_auto_enforce_promotes_after_warmup(self):
        """After min_days windows, auto_enforce=True returns calibrated limit."""
        cal = DailyDDPerTierCalibrator(enforce=False, auto_enforce=True, min_days=3)
        _feed(cal, losses=[1.0] * 15)
        soft = cal.get_soft_limit(tier="T1", regime="trending")
        # 15 days >= min_days=3 → should return calibrated value, not default=2.0
        assert 0.5 <= soft <= 2.5


class TestDailyDDPerTierCalibratorShadow:
    def test_default_returned_when_not_enforced(self):
        cal = DailyDDPerTierCalibrator(enforce=False)
        assert cal.get_soft_limit(tier="T1", regime="trending") == cal.default_soft_pct
        assert cal.get_hard_limit(tier="T1", regime="trending") == cal.default_hard_pct

    def test_shadow_does_not_mutate_enforce_off(self):
        cal = DailyDDPerTierCalibrator(enforce=False, auto_enforce=False, min_days=3)
        _feed(cal, losses=[1.0, 2.0, 3.0, 4.0, 5.0] * 4)
        assert cal.get_soft_limit(tier="T1", regime="trending") == cal.default_soft_pct


class TestDailyDDPerTierCalibratorEnforce:
    def test_enforce_on_returns_calibrated(self):
        cal = DailyDDPerTierCalibrator(enforce=True, min_days=5)
        _feed(cal, losses=[1.0] * 15)
        soft = cal.get_soft_limit(tier="T1", regime="trending")
        hard = cal.get_hard_limit(tier="T1", regime="trending")
        assert 0.5 <= soft <= 2.5
        assert 0.5 <= hard <= 2.5
        assert hard >= soft

    def test_converges_from_large_losses(self):
        cal = DailyDDPerTierCalibrator(enforce=True, min_days=5, default_soft_pct=2.0)
        # Uniform losses of 0.8% → p75=0.8 * 0.85 ≈ 0.68, floored at 0.5
        _feed(cal, losses=[0.8] * 20)
        soft = cal.get_soft_limit(tier="T1", regime="trending")
        assert 0.5 <= soft <= 2.0


class TestDailyDDPerTierCalibratorFallback:
    def test_regime_wildcard_fallback(self):
        cal = DailyDDPerTierCalibrator(enforce=True, min_days=3)
        _feed(cal, tier="T1", regime="*", losses=[1.5] * 12)
        val = cal.get_soft_limit(tier="T1", regime="choppy")
        assert val == cal.get_soft_limit(tier="T1", regime="*")

    def test_global_wildcard_fallback(self):
        cal = DailyDDPerTierCalibrator(enforce=True, min_days=3)
        _feed(cal, tier="*", regime="*", losses=[1.0] * 12)
        val = cal.get_hard_limit(tier="MEME", regime="unknown")
        assert val > 0


class TestDailyDDPerTierCalibratorDedup:
    def test_same_date_deduplicated(self):
        cal = DailyDDPerTierCalibrator(enforce=False, min_days=1)
        ts = _ts()
        cal.observe_day(tier="T1", regime="*", date_str="2026-05-01", pnl_pct=-1.0, ts_ms=ts)
        cal.observe_day(tier="T1", regime="*", date_str="2026-05-01", pnl_pct=-1.5, ts_ms=ts + 100)
        b = cal._bins.get(("T1", "*"))
        assert b is not None and len(b.windows) == 1 and b.windows[-1].pnl_pct_abs == pytest.approx(1.5)


class TestDailyDDPerTierCalibratorSnapshot:
    def test_snapshot_roundtrip(self):
        cal = DailyDDPerTierCalibrator(enforce=True, min_days=3)
        _feed(cal, losses=[1.0] * 10)
        snap = cal.snapshot()
        cal2 = DailyDDPerTierCalibrator(enforce=False)
        cal2.load_state(snap)
        assert cal2.enforce is True
        assert len(cal2._bins) > 0

    def test_auto_enforce_roundtrip(self):
        snap = DailyDDPerTierCalibrator(auto_enforce=True).snapshot()
        assert snap["auto_enforce"] is True
        cal2 = DailyDDPerTierCalibrator(auto_enforce=False)
        cal2.load_state(snap)
        assert cal2.auto_enforce is True

    def test_snapshot_has_schema_version(self):
        cal = DailyDDPerTierCalibrator()
        assert cal.snapshot()["schema_version"] == 1

    def test_bounds_respected(self):
        cal = DailyDDPerTierCalibrator(enforce=True, min_days=3)
        # Force extremely high losses
        _feed(cal, losses=[100.0] * 15)
        soft = cal.get_soft_limit(tier="T1", regime="trending")
        hard = cal.get_hard_limit(tier="T1", regime="trending")
        assert soft <= 2.5  # _HARD_CAP
        assert hard <= 2.5
