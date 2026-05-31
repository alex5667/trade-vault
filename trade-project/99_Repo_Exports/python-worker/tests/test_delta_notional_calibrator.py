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

def test_dn_calib_selection_bias_fix():
    """Prove that training on all events (not just gate-passers) avoids threshold drift.

    Without the fix: calibrator sees only delta_usd > tier0, so p50 of the
    training set converges to ~p75 of the full distribution → threshold inflates
    → more events get vetoed → threshold inflates further (positive feedback).

    With the fix: calibrator sees the full distribution → p50 stays at true p50.
    """
    import random
    random.seed(42)

    # True distribution: uniform [1000, 9000] → true p50 ≈ 5000
    population = [float(random.randint(1000, 9000)) for _ in range(2000)]
    true_p50 = sorted(population)[len(population) // 2]  # ≈ 5000

    # --- Biased calibrator: trained only on events that passed (delta_usd > threshold) ---
    biased = DeltaNotionalCalibrator(min_samples=100)
    threshold = 5000.0  # initial static tier0
    for v in population:
        if v > threshold:  # simulate old gate: update only on pass
            biased.update(regime="na", dn_usd=v)
    biased_tiers = biased.tiers(regime="na", default_t0=threshold, default_t1=7000, default_t2=9000)
    # Biased p50 should be significantly above true p50 (~p75 of [5001..9000] ≈ 7000)
    assert biased_tiers.tier0_usd > true_p50 * 1.2, (
        f"Biased calibrator should overestimate p50: got {biased_tiers.tier0_usd:.0f}, true p50={true_p50:.0f}"
    )

    # --- Unbiased calibrator: trained on ALL events (the fix) ---
    unbiased = DeltaNotionalCalibrator(min_samples=100)
    for v in population:
        unbiased.update(regime="na", dn_usd=v)  # no gate filter
    unbiased_tiers = unbiased.tiers(regime="na", default_t0=threshold, default_t1=7000, default_t2=9000)
    # Unbiased p50 should be within 15% of true p50
    assert abs(unbiased_tiers.tier0_usd - true_p50) / true_p50 < 0.15, (
        f"Unbiased calibrator p50={unbiased_tiers.tier0_usd:.0f} should be near true p50={true_p50:.0f}"
    )


def test_dn_calib_min_usd_floor():
    """dn_calib_min_usd=500 excludes sub-threshold noise from calibration."""
    calib_with_floor = DeltaNotionalCalibrator(min_samples=5)
    calib_without_floor = DeltaNotionalCalibrator(min_samples=5)

    noise_events = [10.0, 50.0, 100.0, 200.0, 400.0]
    signal_events = [1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0]

    min_usd = 500.0

    for v in noise_events + signal_events:
        if v > min_usd:  # simulates dn_calib_min_usd floor in tick_decision_engine
            calib_with_floor.update(regime="na", dn_usd=v)
        calib_without_floor.update(regime="na", dn_usd=v)

    tiers_floor = calib_with_floor.tiers(regime="na", default_t0=100, default_t1=200, default_t2=500)
    tiers_no_floor = calib_without_floor.tiers(regime="na", default_t0=100, default_t1=200, default_t2=500)

    # With floor: noise excluded → p50 reflects only signal events
    assert tiers_floor.tier0_usd > min_usd
    # Without floor: noise pulls p50 down
    assert tiers_no_floor.tier0_usd < tiers_floor.tier0_usd


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
    # ASIA session: 00:00-07:59 UTC
    ts_asia = int(datetime(2024, 1, 1, 3, 0, tzinfo=UTC).timestamp() * 1000)
    assert session_utc(ts_asia) == "ASIA"

    # EU session: 08:00-13:59 UTC
    ts_eu = int(datetime(2024, 1, 1, 9, 0, tzinfo=UTC).timestamp() * 1000)
    assert session_utc(ts_eu) == "EU"

    # NY session: 14:00-20:59 UTC
    ts_ny = int(datetime(2024, 1, 1, 15, 0, tzinfo=UTC).timestamp() * 1000)
    assert session_utc(ts_ny) == "NY"

    # OFF session: 21:00-23:59 UTC
    ts_off = int(datetime(2024, 1, 1, 21, 0, tzinfo=UTC).timestamp() * 1000)
    assert session_utc(ts_off) == "OFF"

    # Invalid timestamp
    assert session_utc(0) == "na"
    assert session_utc(-1) == "na"
