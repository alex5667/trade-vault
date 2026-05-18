"""Tests for VolZThrCalibrator (core/vol_z_thr_calibrator.py)."""
from __future__ import annotations

from core.vol_z_thr_calibrator import (
    DEFAULT_VOL_Z_HARD,
    DEFAULT_VOL_Z_SOFT,
    VOL_Z_CEIL,
    VOL_Z_FLOOR,
    VolZThresholds,
    VolZThrCalibrator,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _feed(cal: VolZThrCalibrator, regime: str, values: list[float]) -> None:
    for v in values:
        cal.observe(regime=regime, vol_z=v)


def _warm_regime(cal: VolZThrCalibrator, regime: str, n: int = 350) -> None:
    """Feed enough observations to pass the warmup guard."""
    import random
    rng = random.Random(42)
    for _ in range(n):
        cal.observe(regime=regime, vol_z=rng.gauss(1.2, 0.6))


# ── unit tests: cold / warmup ─────────────────────────────────────────────────

def test_cold_returns_static_defaults():
    cal = VolZThrCalibrator(min_samples=300, enforce=False)
    th = cal.thresholds(regime="btcusdt:us")
    assert th.soft == DEFAULT_VOL_Z_SOFT
    assert th.hard == DEFAULT_VOL_Z_HARD
    assert th.src == "static"
    assert th.n == 0


def test_enforce_still_defaults_while_cold():
    # enforce=True + n < min_samples: values remain at defaults (no valid quantile yet).
    cal = VolZThrCalibrator(min_samples=300, enforce=True)
    _feed(cal, "btcusdt:us", [1.0, 1.5, 2.0])  # n=3 < 300
    th = cal.thresholds(regime="btcusdt:us")
    assert th.soft == DEFAULT_VOL_Z_SOFT
    assert th.hard == DEFAULT_VOL_Z_HARD
    assert th.n == 3


def test_shadow_always_computes_after_warmup():
    cal = VolZThrCalibrator(min_samples=50, enforce=False)
    _warm_regime(cal, "btcusdt:us", n=60)
    # shadow is populated lazily on the first thresholds() call
    cal.thresholds(regime="btcusdt:us")
    shadow = cal.shadow_thresholds(regime="btcusdt:us")
    assert shadow is not None
    assert shadow.src == "calib_q80q90"


# ── unit tests: observations filtered ────────────────────────────────────────

def test_nan_ignored():
    cal = VolZThrCalibrator()
    cal.observe(regime="x:na", vol_z=float("nan"))
    assert cal.n("x:na") == 0


def test_inf_ignored():
    cal = VolZThrCalibrator()
    cal.observe(regime="x:na", vol_z=float("inf"))
    assert cal.n("x:na") == 0


def test_below_floor_ignored():
    cal = VolZThrCalibrator()
    cal.observe(regime="x:na", vol_z=VOL_Z_FLOOR - 0.01)
    assert cal.n("x:na") == 0


def test_above_ceil_ignored():
    cal = VolZThrCalibrator()
    cal.observe(regime="x:na", vol_z=VOL_Z_CEIL + 0.01)
    assert cal.n("x:na") == 0


def test_boundary_values_accepted():
    cal = VolZThrCalibrator()
    cal.observe(regime="x:na", vol_z=VOL_Z_FLOOR)
    cal.observe(regime="x:na", vol_z=VOL_Z_CEIL)
    assert cal.n("x:na") == 2


# ── unit tests: enforce mode, calibrated thresholds ───────────────────────────

def test_enforce_applies_calibrated_after_warmup():
    cal = VolZThrCalibrator(min_samples=50, enforce=True)
    # Feed a clearly skewed distribution: all values very low → q80/q90 should be < default 1.5
    for _ in range(60):
        cal.observe(regime="ethusdt:eu", vol_z=0.5)
    th = cal.thresholds(regime="ethusdt:eu")
    assert th.src == "calib_q80q90"
    assert th.soft < DEFAULT_VOL_Z_SOFT  # calibrated down
    assert th.hard >= th.soft            # monotonicity


def test_enforce_applies_calibrated_high_distribution():
    cal = VolZThrCalibrator(min_samples=50, enforce=True)
    # High-volume regime: values clustered around 3.0
    for _ in range(60):
        cal.observe(regime="solusdt:us", vol_z=3.0)
    th = cal.thresholds(regime="solusdt:us")
    assert th.src == "calib_q80q90"
    assert th.soft > DEFAULT_VOL_Z_SOFT  # calibrated up
    assert th.hard >= th.soft


# ── unit tests: monotonicity invariant ───────────────────────────────────────

def test_hard_always_gte_soft():
    cal = VolZThrCalibrator(min_samples=10, enforce=True)
    import random
    rng = random.Random(99)
    for _ in range(15):
        cal.observe(regime="r:na", vol_z=rng.uniform(0.3, 6.0))
    th = cal.thresholds(regime="r:na")
    assert th.hard >= th.soft


def test_static_thresholds_monotone():
    th = VolZThresholds(soft=2.0, hard=1.5, n=0, src="static")
    # __post_init__ should fix the monotonicity violation
    assert th.hard >= th.soft


# ── unit tests: hysteresis ───────────────────────────────────────────────────

def test_hysteresis_prevents_micro_update():
    """Committed threshold doesn't change when shift < update_band."""
    cal = VolZThrCalibrator(min_samples=10, enforce=True, update_band=0.5)
    # Warm with mid-range values
    for _ in range(15):
        cal.observe(regime="r:na", vol_z=1.5)
    th1 = cal.thresholds(regime="r:na")
    committed_soft = th1.soft

    # Add a tiny perturbation (less than update_band=0.5 shift in q80)
    cal.observe(regime="r:na", vol_z=1.51)
    th2 = cal.thresholds(regime="r:na")
    # Committed soft should not have jumped by > update_band
    assert abs(th2.soft - committed_soft) < 0.5 + 0.01  # leeway for P² approximation


# ── unit tests: multi-regime independence ────────────────────────────────────

def test_regimes_are_independent():
    cal = VolZThrCalibrator(min_samples=50, enforce=True)
    for _ in range(60):
        cal.observe(regime="btcusdt:us", vol_z=0.5)  # low vol regime
    for _ in range(60):
        cal.observe(regime="btcusdt:asia", vol_z=3.5)  # high vol regime

    th_us = cal.thresholds(regime="btcusdt:us")
    th_asia = cal.thresholds(regime="btcusdt:asia")
    assert th_us.soft < th_asia.soft


def test_regime_key_case_insensitive():
    cal = VolZThrCalibrator(min_samples=5, enforce=True)
    for _ in range(10):
        cal.observe(regime="BTCUSDT:US", vol_z=1.5)
    n = cal.n("btcusdt:us")
    assert n == 10


# ── unit tests: persistence ───────────────────────────────────────────────────

def test_dump_load_roundtrip():
    cal = VolZThrCalibrator(min_samples=50, enforce=True)
    for _ in range(60):
        cal.observe(regime="ethusdt:us", vol_z=1.8)
    th_before = cal.thresholds(regime="ethusdt:us")

    state = cal.dump_regime_state(symbol="ETHUSDT", regime="ethusdt:us", updated_ts_ms=1_000_000)

    cal2 = VolZThrCalibrator(min_samples=50, enforce=True)
    cal2.load_regime_state(state)
    th_after = cal2.thresholds(regime="ethusdt:us")

    assert th_after.n == th_before.n
    assert th_after.src == th_before.src
    # Thresholds may differ slightly because P² state is restored, not re-estimated
    assert abs(th_after.soft - th_before.soft) < 0.3


def test_dump_state_has_correct_kind():
    cal = VolZThrCalibrator()
    state = cal.dump_regime_state(symbol="X", regime="x:na", updated_ts_ms=0)
    assert state["kind"] == "vol_z_thr"
    assert state["v"] == 1


def test_load_wrong_kind_is_noop():
    cal = VolZThrCalibrator(min_samples=10, enforce=True)
    for _ in range(15):
        cal.observe(regime="r:na", vol_z=1.5)
    th_before = cal.thresholds(regime="r:na")

    cal.load_regime_state({"kind": "spread_staleness", "regime": "r:na", "n": 9999})
    th_after = cal.thresholds(regime="r:na")
    assert th_after.n == th_before.n  # unchanged


def test_load_corrupt_state_is_noop():
    cal = VolZThrCalibrator()
    cal.load_regime_state(None)          # not a dict
    cal.load_regime_state("garbage")     # wrong type
    cal.load_regime_state({"kind": "vol_z_thr", "regime": None, "n": "bad"})
    # Should not raise


# ── unit tests: shadow_thresholds ─────────────────────────────────────────────

def test_shadow_none_before_first_call():
    cal = VolZThrCalibrator()
    assert cal.shadow_thresholds(regime="new:na") is None


def test_shadow_available_after_thresholds_call():
    cal = VolZThrCalibrator(min_samples=10)
    _warm_regime(cal, "btcusdt:us", n=15)
    cal.thresholds(regime="btcusdt:us")
    shadow = cal.shadow_thresholds(regime="btcusdt:us")
    assert shadow is not None


# ── integration test: q80 < q90 on typical crypto distribution ────────────────

def test_q80_lt_q90_on_realistic_distribution():
    """q80 should always be ≤ q90 for any real distribution."""
    import random
    cal = VolZThrCalibrator(min_samples=200, enforce=True)
    rng = random.Random(7)
    for _ in range(250):
        # Crypto-like: lognormal-ish, heavier tail
        raw = abs(rng.gauss(1.0, 0.8))
        cal.observe(regime="btcusdt:us", vol_z=min(raw, VOL_Z_CEIL))
    th = cal.thresholds(regime="btcusdt:us")
    assert th.hard >= th.soft
    assert VOL_Z_FLOOR <= th.soft <= VOL_Z_CEIL
    assert VOL_Z_FLOOR <= th.hard <= VOL_Z_CEIL
