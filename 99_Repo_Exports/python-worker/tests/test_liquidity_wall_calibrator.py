"""Tests for LiquidityWallCalibrator (core/liquidity_wall_calibrator.py)."""
from __future__ import annotations

import random

from core.liquidity_wall_calibrator import (
    DEFAULT_MAX_DIST_BPS,
    DEFAULT_SIZE_Z_THR,
    DIST_BPS_CEIL,
    DIST_BPS_FLOOR,
    SIZE_Z_CEIL,
    SIZE_Z_FLOOR,
    LiquidityWallCalibrator,
    LiqWallThresholds,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _warm(cal: LiquidityWallCalibrator, symbol: str, n: int = 350,
          sz_mean: float = 2.0, dist_mean: float = 10.0) -> None:
    rng = random.Random(42)
    for _ in range(n):
        sz = max(SIZE_Z_FLOOR, rng.gauss(sz_mean, 0.5))
        dist = max(DIST_BPS_FLOOR + 0.1, rng.gauss(dist_mean, 3.0))
        cal.observe(symbol=symbol, size_z=sz, dist_bps=dist)


# ── cold / warmup ─────────────────────────────────────────────────────────────

def test_cold_returns_static_defaults():
    cal = LiquidityWallCalibrator(min_samples=300)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.size_z_thr == DEFAULT_SIZE_Z_THR
    assert th.max_dist_bps == DEFAULT_MAX_DIST_BPS
    assert th.src == "static"
    assert th.n == 0


def test_auto_enforce_false_never_calibrates():
    cal = LiquidityWallCalibrator(min_samples=10, enforce=False, auto_enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "static"


def test_auto_enforce_activates_after_warmup():
    cal = LiquidityWallCalibrator(min_samples=300, enforce=False, auto_enforce=True)
    _warm(cal, "BTCUSDT", n=350)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "calib_q75"
    assert th.n >= 300


def test_enforce_true_before_warmup():
    cal = LiquidityWallCalibrator(min_samples=10000, enforce=True, auto_enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "calib_q75"


# ── observation filtering ─────────────────────────────────────────────────────

def test_nan_size_z_ignored():
    cal = LiquidityWallCalibrator()
    cal.observe(symbol="X", size_z=float("nan"), dist_bps=10.0)
    # dist_bps valid → counted
    assert cal.n("X") == 1


def test_nan_dist_bps_ignored():
    cal = LiquidityWallCalibrator()
    cal.observe(symbol="X", size_z=2.0, dist_bps=float("inf"))
    # size_z valid → counted
    assert cal.n("X") == 1


def test_both_invalid_not_counted():
    cal = LiquidityWallCalibrator()
    cal.observe(symbol="X", size_z=float("nan"), dist_bps=float("nan"))
    assert cal.n("X") == 0


def test_size_z_below_floor_ignored():
    cal = LiquidityWallCalibrator()
    # size_z < SIZE_Z_FLOOR (0.5)
    cal.observe(symbol="X", size_z=0.1, dist_bps=10.0)
    # dist_bps valid, but size_z not tracked
    assert cal.n("X") == 1  # dist_bps counted


def test_dist_bps_above_ceil_ignored():
    cal = LiquidityWallCalibrator()
    cal.observe(symbol="X", size_z=2.0, dist_bps=DIST_BPS_CEIL + 1)
    # size_z valid, dist ignored
    assert cal.n("X") == 1  # size_z counted


def test_dist_bps_below_floor_ignored():
    cal = LiquidityWallCalibrator()
    cal.observe(symbol="X", size_z=2.0, dist_bps=0.5)  # < DIST_BPS_FLOOR
    assert cal.n("X") == 1  # size_z counted, dist not


# ── rails ─────────────────────────────────────────────────────────────────────

def test_size_z_thr_clamps_to_floor():
    cal = LiquidityWallCalibrator(min_samples=5, enforce=True)
    # Inject minimal valid size_z values → q75 near floor
    for _ in range(20):
        cal.observe(symbol="X", size_z=SIZE_Z_FLOOR + 0.01, dist_bps=10.0)
    th = cal.thresholds(symbol="X")
    assert th.size_z_thr >= SIZE_Z_FLOOR


def test_max_dist_bps_clamps_to_ceil():
    cal = LiquidityWallCalibrator(min_samples=5, enforce=True)
    # Inject near-ceil dist values → q75 near ceil
    for _ in range(20):
        cal.observe(symbol="X", size_z=2.0, dist_bps=DIST_BPS_CEIL - 0.5)
    th = cal.thresholds(symbol="X")
    assert th.max_dist_bps <= DIST_BPS_CEIL


# ── quantile accuracy ─────────────────────────────────────────────────────────

def test_high_sz_symbol_higher_threshold():
    """Liquid symbol with high z-scores → higher size_z_thr."""
    cal = LiquidityWallCalibrator(min_samples=10, enforce=True)
    # BTC: high z-scores (big walls)
    _warm(cal, "BTCUSDT", n=100, sz_mean=3.0, dist_mean=8.0)
    # PEPE: low z-scores (small walls)
    _warm(cal, "PEPEUSDT", n=100, sz_mean=0.8, dist_mean=8.0)

    th_btc = cal.thresholds(symbol="BTCUSDT")
    th_pepe = cal.thresholds(symbol="PEPEUSDT")
    # BTC walls are bigger → higher threshold
    assert th_btc.size_z_thr > th_pepe.size_z_thr


def test_far_walls_symbol_higher_dist():
    """Symbol with far walls → higher max_dist_bps."""
    cal = LiquidityWallCalibrator(min_samples=10, enforce=True)
    _warm(cal, "BTCUSDT", n=100, sz_mean=2.0, dist_mean=5.0)
    _warm(cal, "SOLUSDT", n=100, sz_mean=2.0, dist_mean=30.0)

    th_btc = cal.thresholds(symbol="BTCUSDT")
    th_sol = cal.thresholds(symbol="SOLUSDT")
    assert th_sol.max_dist_bps > th_btc.max_dist_bps


# ── hysteresis ────────────────────────────────────────────────────────────────

def test_hysteresis_size_z_prevents_small_drift():
    cal = LiquidityWallCalibrator(
        min_samples=10, enforce=True,
        update_band_size_z=0.20,
        update_band_dist_bps=2.0,
    )
    _warm(cal, "BTCUSDT", n=100, sz_mean=2.0, dist_mean=10.0)
    th1 = cal.thresholds(symbol="BTCUSDT")
    committed_sz = th1.size_z_thr

    # Tiny nudge
    for _ in range(3):
        cal.observe(symbol="BTCUSDT", size_z=2.05, dist_bps=10.0)
    th2 = cal.thresholds(symbol="BTCUSDT")
    assert th2.size_z_thr == committed_sz


# ── multi-symbol isolation ────────────────────────────────────────────────────

def test_symbols_are_independent():
    cal = LiquidityWallCalibrator(min_samples=10, enforce=True)
    _warm(cal, "BTCUSDT", n=50, sz_mean=2.5, dist_mean=6.0)
    _warm(cal, "DOGEUSDT", n=50, sz_mean=0.8, dist_mean=25.0)

    n_btc = cal.n("btcusdt")
    n_doge = cal.n("dogeusdt")
    assert n_btc == 50
    assert n_doge == 50

    th_btc = cal.thresholds(symbol="BTCUSDT")
    th_doge = cal.thresholds(symbol="DOGEUSDT")
    assert th_btc.size_z_thr != th_doge.size_z_thr or th_btc.max_dist_bps != th_doge.max_dist_bps


def test_symbol_case_normalized():
    cal = LiquidityWallCalibrator(min_samples=10, enforce=True)
    _warm(cal, "BTCUSDT", n=50)
    th_upper = cal.thresholds(symbol="BTCUSDT")
    th_lower = cal.thresholds(symbol="btcusdt")
    assert th_upper.size_z_thr == th_lower.size_z_thr
    assert th_upper.max_dist_bps == th_lower.max_dist_bps


# ── shadow ────────────────────────────────────────────────────────────────────

def test_shadow_none_before_thresholds_call():
    cal = LiquidityWallCalibrator()
    assert cal.shadow_thresholds(symbol="BTCUSDT") is None


def test_shadow_populated_after_thresholds_call():
    cal = LiquidityWallCalibrator(min_samples=10, enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    cal.thresholds(symbol="BTCUSDT")
    shadow = cal.shadow_thresholds(symbol="BTCUSDT")
    assert shadow is not None
    assert shadow.src == "calib_q75"


def test_shadow_computed_in_static_mode():
    cal = LiquidityWallCalibrator(min_samples=10, enforce=False, auto_enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    cal.thresholds(symbol="BTCUSDT")
    shadow = cal.shadow_thresholds(symbol="BTCUSDT")
    assert shadow is not None


# ── persistence ───────────────────────────────────────────────────────────────

def test_dump_load_roundtrip():
    cal1 = LiquidityWallCalibrator(min_samples=50, enforce=False, auto_enforce=True)
    _warm(cal1, "BTCUSDT", n=80)
    state = cal1.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=1_000_000)

    cal2 = LiquidityWallCalibrator(min_samples=50, enforce=False, auto_enforce=True)
    cal2.load_symbol_state(state)

    assert cal2.n("btcusdt") == cal1.n("btcusdt")
    th1 = cal1.thresholds(symbol="BTCUSDT")
    th2 = cal2.thresholds(symbol="BTCUSDT")
    assert abs(th1.size_z_thr - th2.size_z_thr) < 0.10
    assert abs(th1.max_dist_bps - th2.max_dist_bps) < 1.0


def test_load_wrong_kind_ignored():
    cal = LiquidityWallCalibrator()
    cal.load_symbol_state({"kind": "other", "symbol": "btcusdt", "n": 999})
    assert cal.n("btcusdt") == 0


def test_load_malformed_state_silent():
    cal = LiquidityWallCalibrator()
    cal.load_symbol_state(None)
    cal.load_symbol_state("garbage")
    cal.load_symbol_state({"kind": "liq_wall"})


def test_state_version_and_kind():
    cal = LiquidityWallCalibrator()
    _warm(cal, "BTCUSDT", n=10)
    state = cal.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=1)
    assert state["v"] == 1
    assert state["kind"] == "liq_wall"
    assert state["symbol"] == "btcusdt"


# ── n() ───────────────────────────────────────────────────────────────────────

def test_n_counts_valid_observations():
    cal = LiquidityWallCalibrator()
    for _ in range(5):
        cal.observe(symbol="X", size_z=2.0, dist_bps=10.0)
    assert cal.n("X") == 5


def test_n_unknown_symbol_is_zero():
    cal = LiquidityWallCalibrator()
    assert cal.n("UNKNOWN") == 0
