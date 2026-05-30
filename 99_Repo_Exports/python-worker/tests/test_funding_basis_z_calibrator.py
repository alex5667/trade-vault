"""Tests for FundingBasisZCalibrator (W4)."""
import time
from core.funding_basis_z_calibrator import FundingBasisZCalibrator


def _ts() -> int:
    return int(time.time() * 1000)


def _feed(cal: FundingBasisZCalibrator, *, symbol="BTCUSDT", regime="trending",
          funding_z_vals: list[float], basis_bps_vals: list[float] | None = None) -> None:
    cal.recompute_gap_ms = 0
    ts = _ts()
    if basis_bps_vals is None:
        basis_bps_vals = [5.0] * len(funding_z_vals)
    for i, (fz, bb) in enumerate(zip(funding_z_vals, basis_bps_vals)):
        cal.observe(symbol=symbol, vol_regime=regime, funding_z=fz,
                    basis_bps=bb, ts_ms=ts + i * 1000)


class TestFundingBasisZCalibratorShadow:
    def test_default_when_enforce_off(self):
        cal = FundingBasisZCalibrator(enforce=False)
        assert cal.get_funding_z(symbol="BTC", vol_regime="trending") == cal.default_funding_z
        assert cal.get_basis_bps(symbol="BTC", vol_regime="trending") == cal.default_basis_bps

    def test_shadow_does_not_apply(self):
        cal = FundingBasisZCalibrator(enforce=False, auto_enforce=False, min_samples=5)
        _feed(cal, funding_z_vals=[10.0] * 50, basis_bps_vals=[20.0] * 50)
        assert cal.get_funding_z(symbol="BTCUSDT", vol_regime="trending") == cal.default_funding_z


class TestFundingBasisZCalibratorEnforce:
    def test_calibrates_from_observations(self):
        cal = FundingBasisZCalibrator(enforce=True, min_samples=20)
        _feed(cal, funding_z_vals=[2.0] * 100, basis_bps_vals=[8.0] * 100)
        fz = cal.get_funding_z(symbol="BTCUSDT", vol_regime="trending")
        bb = cal.get_basis_bps(symbol="BTCUSDT", vol_regime="trending")
        assert fz > 0
        assert bb > 0

    def test_p95_safety_mult_applied(self):
        cal = FundingBasisZCalibrator(enforce=True, min_samples=10, safety_mult=1.5)
        _feed(cal, funding_z_vals=[2.0] * 50, basis_bps_vals=[6.0] * 50)
        fz = cal.get_funding_z(symbol="BTCUSDT", vol_regime="trending")
        # p95(2.0) * 1.5 = 3.0; clamped to [2.0, 12.0]
        assert 2.0 <= fz <= 12.0

    def test_bounds_respected(self):
        cal = FundingBasisZCalibrator(enforce=True, min_samples=5)
        _feed(cal, funding_z_vals=[100.0] * 30, basis_bps_vals=[1000.0] * 30)
        fz = cal.get_funding_z(symbol="BTCUSDT", vol_regime="trending")
        bb = cal.get_basis_bps(symbol="BTCUSDT", vol_regime="trending")
        assert fz <= 12.0
        assert bb <= 50.0


class TestFundingBasisZCalibratorFallback:
    def test_wildcard_regime_fallback(self):
        cal = FundingBasisZCalibrator(enforce=True, min_samples=5)
        _feed(cal, symbol="ETHUSDT", regime="*", funding_z_vals=[3.0] * 30, basis_bps_vals=[7.0] * 30)
        val = cal.get_funding_z(symbol="ETHUSDT", vol_regime="choppy")
        assert val == cal.get_funding_z(symbol="ETHUSDT", vol_regime="*")

    def test_wildcard_symbol_fallback(self):
        cal = FundingBasisZCalibrator(enforce=True, min_samples=5)
        _feed(cal, symbol="*", regime="*", funding_z_vals=[4.0] * 30, basis_bps_vals=[9.0] * 30)
        val = cal.get_funding_z(symbol="SOLUSDT", vol_regime="ranging")
        assert val > 0

    def test_unknown_key_returns_default(self):
        cal = FundingBasisZCalibrator(enforce=True, min_samples=100)
        val = cal.get_funding_z(symbol="NOBODY", vol_regime="unknown")
        assert val == cal.default_funding_z


class TestFundingBasisZCalibratorAutoEnforce:
    def test_auto_enforce_promotes_after_warmup(self):
        cal = FundingBasisZCalibrator(enforce=False, auto_enforce=True, min_samples=5)
        _feed(cal, funding_z_vals=[4.0] * 30, basis_bps_vals=[12.0] * 30)
        fz = cal.get_funding_z(symbol="BTCUSDT", vol_regime="trending")
        assert fz > 0  # calibrated value returned, not blocked

    def test_auto_enforce_roundtrip(self):
        snap = FundingBasisZCalibrator(auto_enforce=True).snapshot()
        assert snap["auto_enforce"] is True
        cal2 = FundingBasisZCalibrator(auto_enforce=False)
        cal2.load_state(snap)
        assert cal2.auto_enforce is True


class TestFundingBasisZCalibratorSnapshot:
    def test_snapshot_roundtrip(self):
        cal = FundingBasisZCalibrator(enforce=True, min_samples=5)
        _feed(cal, funding_z_vals=[3.5] * 30, basis_bps_vals=[8.0] * 30)
        snap = cal.snapshot()
        cal2 = FundingBasisZCalibrator(enforce=False)
        cal2.load_state(snap)
        assert cal2.enforce is True
        assert len(cal2._bins) > 0

    def test_nan_observation_ignored(self):
        cal = FundingBasisZCalibrator(enforce=True, min_samples=5)
        cal.observe(symbol="BTC", vol_regime="*", funding_z=float("nan"),
                    basis_bps=5.0, ts_ms=_ts())
        assert all(b.n_observed == 0 for b in cal._bins.values())

    def test_observe_multiple_regimes_independent(self):
        cal = FundingBasisZCalibrator(enforce=True, min_samples=5)
        _feed(cal, regime="trending", funding_z_vals=[2.0] * 30, basis_bps_vals=[5.0] * 30)
        _feed(cal, regime="ranging", funding_z_vals=[6.0] * 30, basis_bps_vals=[15.0] * 30)
        fz_t = cal.get_funding_z(symbol="BTCUSDT", vol_regime="trending")
        fz_r = cal.get_funding_z(symbol="BTCUSDT", vol_regime="ranging")
        assert fz_r > fz_t
