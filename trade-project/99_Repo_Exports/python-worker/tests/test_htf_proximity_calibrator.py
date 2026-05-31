"""Tests for HtfProximityCalibrator (core/htf_proximity_calibrator.py)."""
from __future__ import annotations

import random

from core.htf_proximity_calibrator import (
    DEFAULT_FAR_MULT,
    DEFAULT_NEAR_MULT,
    FAR_MULT_CEIL,
    FAR_MULT_FLOOR,
    NEAR_MULT_CEIL,
    NEAR_MULT_FLOOR,
    HtfProximityCalibrator,
    HtfProximityThresholds,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _warm(cal: HtfProximityCalibrator, symbol: str, n: int = 600,
          ratio_mean: float = 0.5, ratio_std: float = 0.3) -> None:
    """Feed n observations with ratio ~ Normal(mean, std), clipped to (0, 5]."""
    rng = random.Random(42)
    fed = 0
    while fed < n:
        ratio = rng.gauss(ratio_mean, ratio_std)
        atr = 200.0
        dist = ratio * atr
        if 0 < dist and 0 < ratio <= 5.0:
            cal.observe(symbol=symbol, dist_bps=dist, daily_atr_bps=atr)
            fed += 1


# ── cold / warmup ─────────────────────────────────────────────────────────────

def test_cold_returns_static_defaults():
    cal = HtfProximityCalibrator(min_samples=500)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.near_mult == DEFAULT_NEAR_MULT
    assert th.far_mult == DEFAULT_FAR_MULT
    assert th.src == "static"
    assert th.n == 0


def test_auto_enforce_false_never_calibrates():
    cal = HtfProximityCalibrator(min_samples=10, enforce=False, auto_enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "static"


def test_auto_enforce_activates_after_warmup():
    cal = HtfProximityCalibrator(min_samples=500, enforce=False, auto_enforce=True)
    _warm(cal, "BTCUSDT", n=600)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "calib_q20q80"
    assert th.n >= 500


def test_enforce_true_before_warmup():
    cal = HtfProximityCalibrator(min_samples=10000, enforce=True, auto_enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "calib_q20q80"


# ── observation filtering ─────────────────────────────────────────────────────

def test_negative_dist_ignored():
    cal = HtfProximityCalibrator()
    cal.observe(symbol="BTCUSDT", dist_bps=-1.0, daily_atr_bps=200.0)
    assert cal.n("BTCUSDT") == 0


def test_zero_atr_ignored():
    cal = HtfProximityCalibrator()
    cal.observe(symbol="BTCUSDT", dist_bps=10.0, daily_atr_bps=0.0)
    assert cal.n("BTCUSDT") == 0


def test_negative_atr_ignored():
    cal = HtfProximityCalibrator()
    cal.observe(symbol="BTCUSDT", dist_bps=10.0, daily_atr_bps=-100.0)
    assert cal.n("BTCUSDT") == 0


def test_ratio_above_5_ignored():
    cal = HtfProximityCalibrator()
    # ratio = 1000/100 = 10 > 5
    cal.observe(symbol="BTCUSDT", dist_bps=1000.0, daily_atr_bps=100.0)
    assert cal.n("BTCUSDT") == 0


def test_nan_values_ignored():
    cal = HtfProximityCalibrator()
    cal.observe(symbol="BTCUSDT", dist_bps=float("nan"), daily_atr_bps=200.0)
    cal.observe(symbol="BTCUSDT", dist_bps=10.0, daily_atr_bps=float("inf"))
    assert cal.n("BTCUSDT") == 0


def test_valid_observation_counted():
    cal = HtfProximityCalibrator()
    cal.observe(symbol="BTCUSDT", dist_bps=40.0, daily_atr_bps=200.0)  # ratio=0.2
    assert cal.n("BTCUSDT") == 1


# ── monotonicity invariant ────────────────────────────────────────────────────

def test_far_mult_always_greater_than_near_mult():
    cal = HtfProximityCalibrator(min_samples=10, enforce=True)
    _warm(cal, "BTCUSDT", n=50)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.far_mult > th.near_mult


def test_dataclass_monotonicity_fix():
    # __post_init__ should enforce far > near
    th = HtfProximityThresholds(near_mult=0.5, far_mult=0.4, n=0, src="test")
    assert th.far_mult > th.near_mult


# ── rails ─────────────────────────────────────────────────────────────────────

def test_near_mult_clamps_to_floor():
    cal = HtfProximityCalibrator(min_samples=5, enforce=True)
    # Inject very small ratios → q20 near 0
    for _ in range(20):
        cal.observe(symbol="X", dist_bps=1.0, daily_atr_bps=200.0)  # ratio=0.005
    th = cal.thresholds(symbol="X")
    assert th.near_mult >= NEAR_MULT_FLOOR


def test_far_mult_clamps_to_ceil():
    cal = HtfProximityCalibrator(min_samples=5, enforce=True)
    # Inject ratios near 5 → q80 near ceil
    for _ in range(20):
        cal.observe(symbol="X", dist_bps=990.0, daily_atr_bps=200.0)  # ratio=4.95
    th = cal.thresholds(symbol="X")
    assert th.far_mult <= FAR_MULT_CEIL


# ── per-symbol isolation ──────────────────────────────────────────────────────

def test_symbols_are_independent():
    cal = HtfProximityCalibrator(min_samples=10, enforce=True)
    # BTC: close to levels (low ratio)
    _warm(cal, "BTCUSDT", n=50, ratio_mean=0.1, ratio_std=0.05)
    # PEPE: far from levels (high ratio)
    _warm(cal, "PEPEUSDT", n=50, ratio_mean=1.5, ratio_std=0.3)

    th_btc = cal.thresholds(symbol="BTCUSDT")
    th_pepe = cal.thresholds(symbol="PEPEUSDT")
    # PEPE should have larger near/far mults
    assert th_pepe.near_mult > th_btc.near_mult or th_pepe.far_mult > th_btc.far_mult


def test_symbol_case_normalized():
    cal = HtfProximityCalibrator(min_samples=10, enforce=True)
    _warm(cal, "BTCUSDT", n=50)
    th_upper = cal.thresholds(symbol="BTCUSDT")
    th_lower = cal.thresholds(symbol="btcusdt")
    assert th_upper.near_mult == th_lower.near_mult
    assert th_upper.far_mult == th_lower.far_mult


# ── quantile accuracy ─────────────────────────────────────────────────────────

def test_near_mult_below_far_mult_for_skewed_distribution():
    """q20 < q80 for any non-degenerate distribution."""
    cal = HtfProximityCalibrator(min_samples=50, enforce=True)
    _warm(cal, "BTCUSDT", n=200, ratio_mean=0.4, ratio_std=0.2)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.near_mult < th.far_mult


# ── hysteresis ────────────────────────────────────────────────────────────────

def test_hysteresis_prevents_small_drift():
    cal = HtfProximityCalibrator(
        min_samples=10, enforce=True,
        update_band=0.10,
    )
    _warm(cal, "BTCUSDT", n=200, ratio_mean=0.4, ratio_std=0.2)
    th1 = cal.thresholds(symbol="BTCUSDT")
    near1 = th1.near_mult

    # Tiny nudge
    for _ in range(3):
        cal.observe(symbol="BTCUSDT", dist_bps=82.0, daily_atr_bps=200.0)
    th2 = cal.thresholds(symbol="BTCUSDT")
    # Committed value should be stable
    assert abs(th2.near_mult - near1) < 0.10


# ── shadow ────────────────────────────────────────────────────────────────────

def test_shadow_none_before_thresholds_call():
    cal = HtfProximityCalibrator()
    assert cal.shadow_thresholds(symbol="BTCUSDT") is None


def test_shadow_populated_after_thresholds_call():
    cal = HtfProximityCalibrator(min_samples=10, enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    cal.thresholds(symbol="BTCUSDT")
    shadow = cal.shadow_thresholds(symbol="BTCUSDT")
    assert shadow is not None
    assert shadow.src == "calib_q20q80"


def test_shadow_computed_even_when_static():
    cal = HtfProximityCalibrator(min_samples=10, enforce=False, auto_enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    cal.thresholds(symbol="BTCUSDT")
    shadow = cal.shadow_thresholds(symbol="BTCUSDT")
    assert shadow is not None


# ── persistence ───────────────────────────────────────────────────────────────

def test_dump_load_roundtrip():
    cal1 = HtfProximityCalibrator(min_samples=100, enforce=False, auto_enforce=True)
    _warm(cal1, "BTCUSDT", n=150)
    state = cal1.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=1_000_000)

    cal2 = HtfProximityCalibrator(min_samples=100, enforce=False, auto_enforce=True)
    cal2.load_symbol_state(state)

    assert cal2.n("btcusdt") == cal1.n("btcusdt")
    th1 = cal1.thresholds(symbol="BTCUSDT")
    th2 = cal2.thresholds(symbol="BTCUSDT")
    assert abs(th1.near_mult - th2.near_mult) < 0.05
    assert abs(th1.far_mult - th2.far_mult) < 0.05


def test_load_wrong_kind_ignored():
    cal = HtfProximityCalibrator()
    cal.load_symbol_state({"kind": "other", "symbol": "btcusdt", "n": 999})
    assert cal.n("btcusdt") == 0


def test_load_malformed_state_silent():
    cal = HtfProximityCalibrator()
    cal.load_symbol_state(None)
    cal.load_symbol_state("garbage")
    cal.load_symbol_state({"kind": "htf_proximity"})  # missing fields


def test_state_version_and_kind():
    cal = HtfProximityCalibrator()
    _warm(cal, "BTCUSDT", n=10)
    state = cal.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=1)
    assert state["v"] == 1
    assert state["kind"] == "htf_proximity"
    assert state["symbol"] == "btcusdt"
