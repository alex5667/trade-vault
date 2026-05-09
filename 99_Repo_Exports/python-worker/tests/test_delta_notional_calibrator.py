import math
from datetime import UTC, datetime

from core.delta_notional_calibrator import DeltaNotionalCalibrator
from services.orderflow.utils import hour_of_week_utc, session_utc


def test_dn_calib_bootstrap():
    """Test DN calibrator uses static fallbacks when low on samples."""
    calib = DeltaNotionalCalibrator(min_samples=100)
    rg = "trend"
    # No samples yet
    tiers = calib.tiers(regime=rg, default_t0=100, default_t1=200, default_t2=500)
    assert tiers.tier0_usd == 100
    assert tiers.tier1_usd == 200
    assert tiers.tier2_usd == 500
    assert tiers.src == "static"

def test_dn_calib_learning():
    """Test DN calibrator learns from samples."""
    calib = DeltaNotionalCalibrator(min_samples=10)
    rg = "range"
    # Feed 10 samples of 1000 USD
    for _ in range(10):
        calib.update(regime=rg, dn_usd=1000.0)

    tiers = calib.tiers(regime=rg, default_t0=10, default_t1=20, default_t2=50)
    assert tiers.src == "calib_p50/p80/p95"
    # Since all samples are 1000, quantiles should be ~1000
    assert math.isclose(tiers.tier0_usd, 1000.0, rel_tol=0.1)
    assert math.isclose(tiers.tier1_usd, 1000.0, rel_tol=0.1)
    assert math.isclose(tiers.tier2_usd, 1000.0, rel_tol=0.1)

def test_dn_calib_persistence():
    """Test dump/load state."""
    calib = DeltaNotionalCalibrator(min_samples=5)
    calib.update(regime="na", dn_usd=500.0)
    calib.update(regime="na", dn_usd=1000.0)

    state = calib.dump_regime_state(symbol="BTC", regime="na", updated_ts_ms=123)
    assert state["symbol"] == "BTC"
    assert state["regime"] == "na"
    assert "q50_log" in state

    calib2 = DeltaNotionalCalibrator()
    calib2.load_regime_state(state)
    assert "na" in calib2._n
    assert calib2._n["na"] >= 2

def test_dn_calib_hour_of_week():
    """Test hour-of-week liquidity scaling telemetry."""
    calib = DeltaNotionalCalibrator(min_samples=5, liq_alpha=0.1, liq_scale_clamp=(0.5, 2.0))

    # Monday 10:00 UTC (weekday 0, hour 10 -> how=10)
    ts_mon_10 = int(datetime(2024, 1, 1, 10, 0, tzinfo=UTC).timestamp() * 1000)
    # Tuesday 15:00 UTC (weekday 1, hour 15 -> how=39)
    ts_tue_15 = int(datetime(2024, 1, 2, 15, 0, tzinfo=UTC).timestamp() * 1000)

    # Update with different liquidity levels
    calib.update(regime="range", dn_usd=1000.0, ts_ms=ts_mon_10)  # Monday 10:00
    calib.update(regime="range", dn_usd=1000.0, ts_ms=ts_mon_10)  # Same bucket
    calib.update(regime="range", dn_usd=500.0, ts_ms=ts_tue_15)   # Tuesday 15:00

    # Check hour-of-week calculation
    assert calib._get_hour_of_week(ts_mon_10) == 10  # Monday 10:00
    assert calib._get_hour_of_week(ts_tue_15) == 39  # Tuesday 15:00 (1*24 + 15)

    # Check liquidity EMAs
    assert calib._global_liq["range"] > 0
    assert 10 in calib._bucket_liq["range"]
    assert 39 in calib._bucket_liq["range"]

    # Test tiers with scaling
    tiers_scaled = calib.tiers(regime="range", ts_ms=ts_mon_10, default_t0=100, default_t1=200, default_t2=500)
    tiers_unscaled = calib.tiers(regime="range", ts_ms=0, default_t0=100, default_t1=200, default_t2=500)

    # Scaled should be different from unscaled
    assert tiers_scaled.scale != 1.0
    assert tiers_unscaled.scale == 1.0

    # Check telemetry fields
    assert tiers_scaled.hour_of_week == 10
    assert tiers_scaled.g_liq_ema > 0
    assert tiers_scaled.b_liq_ema > 0

def test_dn_calib_scale_clamping():
    """Test that hour-of-week scale is properly clamped."""
    calib = DeltaNotionalCalibrator(min_samples=5, liq_alpha=1.0, liq_scale_clamp=(0.5, 1.5))

    ts = int(datetime(2024, 1, 1, 10, 0, tzinfo=UTC).timestamp() * 1000)

    # Create extreme scale by feeding very different values
    calib.update(regime="test", dn_usd=1000.0, ts_ms=ts)
    calib.update(regime="test", dn_usd=1000.0, ts_ms=ts)

    # Bucket gets same value -> scale = 1.0
    tiers = calib.tiers(regime="test", ts_ms=ts, default_t0=100, default_t1=200, default_t2=500)
    assert 0.5 <= tiers.scale <= 1.5

def test_dn_calib_persistence_with_how():
    """Test persistence includes hour-of-week liquidity state."""
    calib = DeltaNotionalCalibrator(min_samples=5, liq_alpha=0.5)
    ts = int(datetime(2024, 1, 1, 10, 0, tzinfo=UTC).timestamp() * 1000)

    calib.update(regime="test", dn_usd=1000.0, ts_ms=ts)
    calib.update(regime="test", dn_usd=500.0, ts_ms=ts)

    state = calib.dump_regime_state(symbol="BTC", regime="test", updated_ts_ms=ts)

    # Check new fields in state
    assert "liq_global" in state
    assert "liq_bucket" in state
    assert isinstance(state["liq_bucket"], dict)

    # Test load
    calib2 = DeltaNotionalCalibrator()
    calib2.load_regime_state(state)

    assert "test" in calib2._global_liq
    assert "test" in calib2._bucket_liq

def test_hour_of_week_utc():
    """Test hour_of_week_utc function."""
    # Monday 10:00 UTC (weekday 0, hour 10 -> how=10)
    ts_mon_10 = int(datetime(2024, 1, 1, 10, 0, tzinfo=UTC).timestamp() * 1000)
    assert hour_of_week_utc(ts_mon_10) == 10

    # Tuesday 15:00 UTC (weekday 1, hour 15 -> how=39)
    ts_tue_15 = int(datetime(2024, 1, 2, 15, 0, tzinfo=UTC).timestamp() * 1000)
    assert hour_of_week_utc(ts_tue_15) == 39

    # Sunday 23:00 UTC (weekday 6, hour 23 -> how=167)
    ts_sun_23 = int(datetime(2024, 1, 7, 23, 0, tzinfo=UTC).timestamp() * 1000)
    assert hour_of_week_utc(ts_sun_23) == 167

    # Invalid timestamp
    assert hour_of_week_utc(0) == -1
    assert hour_of_week_utc(-1) == -1

def test_session_utc():
    """Test session_utc function."""
    # Asia session: 00:00-06:59
    ts_asia = int(datetime(2024, 1, 1, 3, 0, tzinfo=UTC).timestamp() * 1000)
    assert session_utc(ts_asia) == "asia"

    # EU session: 07:00-12:59
    ts_eu = int(datetime(2024, 1, 1, 9, 0, tzinfo=UTC).timestamp() * 1000)
    assert session_utc(ts_eu) == "eu"

    # US session: 13:00-19:59
    ts_us = int(datetime(2024, 1, 1, 15, 0, tzinfo=UTC).timestamp() * 1000)
    assert session_utc(ts_us) == "us"

    # Late session: 20:00-23:59
    ts_late = int(datetime(2024, 1, 1, 21, 0, tzinfo=UTC).timestamp() * 1000)
    assert session_utc(ts_late) == "late"

    # Invalid timestamp
    assert session_utc(0) == "na"
    assert session_utc(-1) == "na"
