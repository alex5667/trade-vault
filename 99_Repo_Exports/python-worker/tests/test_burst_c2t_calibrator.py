"""Tests for BurstC2TCalibrator (core/burst_c2t_calibrator.py)."""
from __future__ import annotations

import math
import random

from core.burst_c2t_calibrator import (
    BURST_FLIP_CEIL,
    BURST_FLIP_FLOOR,
    C2T_CEIL,
    C2T_FLOOR,
    DEFAULT_BURST_FLIP_MAX,
    DEFAULT_C2T_MAX,
    BurstC2TCalibrator,
    BurstC2TThresholds,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _warm(cal: BurstC2TCalibrator, regime: str, n: int = 350) -> None:
    rng = random.Random(42)
    for _ in range(n):
        cal.observe(
            regime=regime,
            burst_flip=rng.uniform(0.35, 0.90),
            c2t=rng.uniform(1.5, 12.0),
        )


# ── cold / warmup ─────────────────────────────────────────────────────────────

def test_cold_returns_static_defaults():
    cal = BurstC2TCalibrator(min_samples=300)
    th = cal.thresholds(regime="btcusdt:us")
    assert th.burst_flip_max == DEFAULT_BURST_FLIP_MAX
    assert th.c2t_max == DEFAULT_C2T_MAX
    assert th.src == "static"
    assert th.n == 0


def test_enforce_false_auto_enforce_false_never_calibrates():
    cal = BurstC2TCalibrator(min_samples=10, enforce=False, auto_enforce=False)
    _warm(cal, "ethusdt:eu", n=50)
    th = cal.thresholds(regime="ethusdt:eu")
    assert th.src == "static"


def test_auto_enforce_activates_after_warmup():
    cal = BurstC2TCalibrator(min_samples=300, enforce=False, auto_enforce=True)
    _warm(cal, "btcusdt:us", n=350)
    th = cal.thresholds(regime="btcusdt:us")
    assert th.src == "calib_q95"
    assert th.n >= 300


def test_enforce_true_activates_before_warmup():
    cal = BurstC2TCalibrator(min_samples=1000, enforce=True, auto_enforce=False)
    _warm(cal, "btcusdt:us", n=50)
    th = cal.thresholds(regime="btcusdt:us")
    assert th.src == "calib_q95"


# ── observation filtering ─────────────────────────────────────────────────────

def test_zero_burst_flip_ignored():
    cal = BurstC2TCalibrator(min_samples=5, enforce=True)
    for _ in range(10):
        cal.observe(regime="r", burst_flip=0.0, c2t=5.0)
    # c2t observed but burst_flip wasn't
    assert cal.n("r") > 0  # c2t counted


def test_zero_c2t_ignored():
    cal = BurstC2TCalibrator(min_samples=5, enforce=True)
    for _ in range(10):
        cal.observe(regime="r", burst_flip=0.7, c2t=0.0)
    assert cal.n("r") > 0  # burst_flip counted


def test_both_zero_not_counted():
    cal = BurstC2TCalibrator()
    cal.observe(regime="r", burst_flip=0.0, c2t=0.0)
    assert cal.n("r") == 0


def test_nan_values_ignored():
    cal = BurstC2TCalibrator()
    cal.observe(regime="r", burst_flip=float("nan"), c2t=5.0)
    cal.observe(regime="r", burst_flip=0.7, c2t=float("inf"))
    # only c2t=5.0 and burst_flip=0.7 valid
    assert cal.n("r") == 2


def test_out_of_rails_burst_flip_ignored():
    cal = BurstC2TCalibrator(min_samples=2, enforce=True)
    cal.observe(regime="r", burst_flip=0.1, c2t=5.0)  # burst_flip < FLOOR
    cal.observe(regime="r", burst_flip=1.5, c2t=5.0)  # burst_flip > CEIL
    th = cal.thresholds(regime="r")
    # only c2t contributed to n
    assert th.burst_flip_max == DEFAULT_BURST_FLIP_MAX  # no bf observations


def test_out_of_rails_c2t_ignored():
    cal = BurstC2TCalibrator(min_samples=2, enforce=True)
    cal.observe(regime="r", burst_flip=0.7, c2t=0.5)    # c2t < FLOOR
    cal.observe(regime="r", burst_flip=0.7, c2t=200.0)  # c2t > CEIL
    th = cal.thresholds(regime="r")
    assert th.c2t_max == DEFAULT_C2T_MAX  # no c2t observations


# ── threshold rails ───────────────────────────────────────────────────────────

def test_burst_flip_max_clamps_to_floor():
    cal = BurstC2TCalibrator(min_samples=5, enforce=True)
    # Inject extremely low burst_flip values → q95 near floor
    for _ in range(20):
        cal.observe(regime="r", burst_flip=BURST_FLIP_FLOOR + 0.001, c2t=5.0)
    th = cal.thresholds(regime="r")
    assert th.burst_flip_max >= BURST_FLIP_FLOOR


def test_c2t_max_clamps_to_ceil():
    cal = BurstC2TCalibrator(min_samples=5, enforce=True)
    for _ in range(20):
        cal.observe(regime="r", burst_flip=0.7, c2t=C2T_CEIL - 0.1)
    th = cal.thresholds(regime="r")
    assert th.c2t_max <= C2T_CEIL


# ── hysteresis ────────────────────────────────────────────────────────────────

def test_hysteresis_burst_flip_prevents_small_drift():
    cal = BurstC2TCalibrator(
        min_samples=5, enforce=True,
        update_band_burst=0.10, update_band_c2t=1.0,
    )
    for _ in range(20):
        cal.observe(regime="r", burst_flip=0.80, c2t=5.0)
    th1 = cal.thresholds(regime="r")
    committed1 = th1.burst_flip_max

    # Tiny nudge (< 0.10 band)
    for _ in range(5):
        cal.observe(regime="r", burst_flip=0.82, c2t=5.0)
    th2 = cal.thresholds(regime="r")
    assert th2.burst_flip_max == committed1


# ── multi-regime isolation ────────────────────────────────────────────────────

def test_regimes_are_independent():
    cal = BurstC2TCalibrator(min_samples=10, enforce=True)
    rng = random.Random(7)
    for _ in range(30):
        cal.observe(regime="btcusdt:us", burst_flip=rng.uniform(0.4, 0.7), c2t=rng.uniform(2, 6))
    for _ in range(30):
        cal.observe(regime="ethusdt:eu", burst_flip=rng.uniform(0.6, 0.95), c2t=rng.uniform(5, 15))

    th_btc = cal.thresholds(regime="btcusdt:us")
    th_eth = cal.thresholds(regime="ethusdt:eu")
    assert th_btc.burst_flip_max != th_eth.burst_flip_max or th_btc.c2t_max != th_eth.c2t_max


# ── shadow ────────────────────────────────────────────────────────────────────

def test_shadow_None_before_first_thresholds_call():
    cal = BurstC2TCalibrator()
    assert cal.shadow_thresholds(regime="btcusdt:us") is None


def test_shadow_populated_after_thresholds_call():
    cal = BurstC2TCalibrator(min_samples=5, enforce=False)
    _warm(cal, "btcusdt:us", n=20)
    cal.thresholds(regime="btcusdt:us")
    shadow = cal.shadow_thresholds(regime="btcusdt:us")
    assert shadow is not None
    assert shadow.src == "calib_q95"


def test_shadow_computed_even_in_static_mode():
    cal = BurstC2TCalibrator(min_samples=5, enforce=False, auto_enforce=False)
    _warm(cal, "r", n=20)
    cal.thresholds(regime="r")
    shadow = cal.shadow_thresholds(regime="r")
    assert shadow is not None


# ── persistence ───────────────────────────────────────────────────────────────

def test_dump_load_roundtrip():
    cal1 = BurstC2TCalibrator(min_samples=50, enforce=False, auto_enforce=True)
    _warm(cal1, "btcusdt:us", n=80)
    state = cal1.dump_regime_state(symbol="BTCUSDT", regime="btcusdt:us", updated_ts_ms=1_000_000)

    cal2 = BurstC2TCalibrator(min_samples=50, enforce=False, auto_enforce=True)
    cal2.load_regime_state(state)

    assert cal2.n("btcusdt:us") == cal1.n("btcusdt:us")
    th1 = cal1.thresholds(regime="btcusdt:us")
    th2 = cal2.thresholds(regime="btcusdt:us")
    assert abs(th1.burst_flip_max - th2.burst_flip_max) < 0.05
    assert abs(th1.c2t_max - th2.c2t_max) < 0.5


def test_load_wrong_kind_ignored():
    cal = BurstC2TCalibrator()
    cal.load_regime_state({"kind": "other", "regime": "r", "n": 999})
    assert cal.n("r") == 0


def test_load_malformed_state_silent():
    cal = BurstC2TCalibrator()
    cal.load_regime_state(None)
    cal.load_regime_state("garbage")
    cal.load_regime_state({"kind": "burst_c2t"})  # missing fields


def test_state_version_field():
    cal = BurstC2TCalibrator()
    _warm(cal, "r", n=10)
    state = cal.dump_regime_state(symbol="X", regime="r", updated_ts_ms=1)
    assert state["v"] == 1
    assert state["kind"] == "burst_c2t"


# ── n() ───────────────────────────────────────────────────────────────────────

def test_n_counts_observations():
    cal = BurstC2TCalibrator()
    for i in range(10):
        cal.observe(regime="r", burst_flip=0.7, c2t=5.0)
    assert cal.n("r") == 10


def test_n_unknown_regime_is_zero():
    cal = BurstC2TCalibrator()
    assert cal.n("unknown") == 0
