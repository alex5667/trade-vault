"""Tests for LiquiditySignalCalibrator (core/liquidity_signal_calibrator.py)."""
from __future__ import annotations
import random
from core.liquidity_signal_calibrator import (
    DEFAULT_CLUSTER_BPS, DEFAULT_NOTIONAL_THR,
    NOTIONAL_THR_FLOOR, NOTIONAL_THR_CEIL,
    CLUSTER_BPS_FLOOR, CLUSTER_BPS_CEIL,
    LiquiditySignalCalibrator, LiquiditySignalThresholds,
)


def _warm(cal: LiquiditySignalCalibrator, symbol: str, n: int = 600,
          depth_mean: float = 200_000, depth_std: float = 80_000,
          spread_mean: float = 4.0, spread_std: float = 1.5) -> None:
    rng = random.Random(42)
    fed = 0
    while fed < n:
        d = rng.gauss(depth_mean, depth_std)
        s = rng.gauss(spread_mean, spread_std)
        if d > 0 and s > 0:
            cal.observe(symbol=symbol, depth_usd=d, spread_bps=s)
            fed += 1


# ── cold / warmup ─────────────────────────────────────────────────────────────

def test_cold_returns_static_defaults():
    cal = LiquiditySignalCalibrator(min_samples=500)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.notional_thr == DEFAULT_NOTIONAL_THR
    assert th.cluster_bps == DEFAULT_CLUSTER_BPS
    assert th.src == "static"
    assert th.n == 0


def test_auto_enforce_false_never_calibrates():
    cal = LiquiditySignalCalibrator(min_samples=10, enforce=False, auto_enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "static"


def test_auto_enforce_activates_after_warmup():
    cal = LiquiditySignalCalibrator(min_samples=500, enforce=False, auto_enforce=True)
    _warm(cal, "BTCUSDT", n=600)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "calib_q50"
    assert th.n >= 500


def test_enforce_true_before_warmup():
    cal = LiquiditySignalCalibrator(min_samples=10000, enforce=True)
    _warm(cal, "BTCUSDT", n=50)
    th = cal.thresholds(symbol="BTCUSDT")
    assert th.src == "calib_q50"


# ── input validation ──────────────────────────────────────────────────────────

def test_zero_depth_ignored():
    cal = LiquiditySignalCalibrator()
    cal.observe(symbol="BTCUSDT", depth_usd=0.0, spread_bps=2.0)
    assert cal.n("BTCUSDT") == 0


def test_negative_spread_ignored():
    cal = LiquiditySignalCalibrator()
    cal.observe(symbol="BTCUSDT", depth_usd=100_000, spread_bps=-1.0)
    assert cal.n("BTCUSDT") == 0


def test_nan_ignored():
    cal = LiquiditySignalCalibrator()
    cal.observe(symbol="BTCUSDT", depth_usd=float("nan"), spread_bps=2.0)
    cal.observe(symbol="BTCUSDT", depth_usd=100_000, spread_bps=float("inf"))
    assert cal.n("BTCUSDT") == 0


def test_valid_observation_counted():
    cal = LiquiditySignalCalibrator()
    cal.observe(symbol="BTCUSDT", depth_usd=150_000, spread_bps=1.5)
    assert cal.n("BTCUSDT") == 1


# ── rails ─────────────────────────────────────────────────────────────────────

def test_notional_thr_clamps_to_floor():
    cal = LiquiditySignalCalibrator(min_samples=5, enforce=True)
    for _ in range(20):
        cal.observe(symbol="X", depth_usd=1.0, spread_bps=1.0)
    th = cal.thresholds(symbol="X")
    assert th.notional_thr >= NOTIONAL_THR_FLOOR


def test_notional_thr_clamps_to_ceil():
    cal = LiquiditySignalCalibrator(min_samples=5, enforce=True)
    for _ in range(20):
        cal.observe(symbol="X", depth_usd=100_000_000, spread_bps=1.0)
    th = cal.thresholds(symbol="X")
    assert th.notional_thr <= NOTIONAL_THR_CEIL


def test_cluster_bps_clamps_to_floor():
    cal = LiquiditySignalCalibrator(min_samples=5, enforce=True)
    for _ in range(20):
        cal.observe(symbol="X", depth_usd=100_000, spread_bps=0.001)
    th = cal.thresholds(symbol="X")
    assert th.cluster_bps >= CLUSTER_BPS_FLOOR


def test_cluster_bps_clamps_to_ceil():
    cal = LiquiditySignalCalibrator(min_samples=5, enforce=True)
    for _ in range(20):
        cal.observe(symbol="X", depth_usd=100_000, spread_bps=500.0)
    th = cal.thresholds(symbol="X")
    assert th.cluster_bps <= CLUSTER_BPS_CEIL


# ── per-symbol isolation ──────────────────────────────────────────────────────

def test_symbols_are_independent():
    cal = LiquiditySignalCalibrator(min_samples=10, enforce=True)
    # BTC: deep book
    _warm(cal, "BTCUSDT", n=50, depth_mean=1_000_000, depth_std=100_000, spread_mean=1.0, spread_std=0.2)
    # PEPE: thin book
    _warm(cal, "PEPEUSDT", n=50, depth_mean=20_000, depth_std=5_000, spread_mean=10.0, spread_std=2.0)
    th_btc = cal.thresholds(symbol="BTCUSDT")
    th_pepe = cal.thresholds(symbol="PEPEUSDT")
    assert th_btc.notional_thr > th_pepe.notional_thr
    assert th_btc.cluster_bps < th_pepe.cluster_bps


def test_symbol_case_normalized():
    cal = LiquiditySignalCalibrator(min_samples=10, enforce=True)
    _warm(cal, "BTCUSDT", n=50)
    th_up = cal.thresholds(symbol="BTCUSDT")
    th_lo = cal.thresholds(symbol="btcusdt")
    assert th_up.notional_thr == th_lo.notional_thr


# ── hysteresis ────────────────────────────────────────────────────────────────

def test_hysteresis_prevents_tiny_drift():
    cal = LiquiditySignalCalibrator(min_samples=10, enforce=True, rel_thresh=0.10)
    _warm(cal, "BTCUSDT", n=200, depth_mean=200_000, depth_std=5_000)
    th1 = cal.thresholds(symbol="BTCUSDT")
    prev = th1.notional_thr
    # Tiny nudge — should not commit
    for _ in range(3):
        cal.observe(symbol="BTCUSDT", depth_usd=prev * 1.02, spread_bps=2.0)
    th2 = cal.thresholds(symbol="BTCUSDT")
    assert abs(th2.notional_thr - prev) / max(prev, 1e-9) < 0.10


# ── shadow ────────────────────────────────────────────────────────────────────

def test_shadow_none_before_thresholds_call():
    cal = LiquiditySignalCalibrator()
    assert cal.shadow_thresholds(symbol="BTCUSDT") is None


def test_shadow_populated_after_thresholds_call():
    cal = LiquiditySignalCalibrator(min_samples=10, enforce=False)
    _warm(cal, "BTCUSDT", n=50)
    cal.thresholds(symbol="BTCUSDT")
    sh = cal.shadow_thresholds(symbol="BTCUSDT")
    assert sh is not None
    assert sh.src == "calib_q50"


# ── persistence ───────────────────────────────────────────────────────────────

def test_dump_load_roundtrip():
    cal1 = LiquiditySignalCalibrator(min_samples=50, enforce=True)
    _warm(cal1, "BTCUSDT", n=100)
    state = cal1.dump_symbol_state(symbol="BTCUSDT", updated_ts_ms=1_000_000)
    assert state["kind"] == "liquidity_signal"
    assert state["v"] == 1

    cal2 = LiquiditySignalCalibrator(min_samples=50, enforce=True)
    cal2.load_symbol_state(state)
    assert cal2.n("btcusdt") == cal1.n("btcusdt")
    th1 = cal1.thresholds(symbol="BTCUSDT")
    th2 = cal2.thresholds(symbol="BTCUSDT")
    assert abs(th1.notional_thr - th2.notional_thr) / max(th1.notional_thr, 1) < 0.05
    assert abs(th1.cluster_bps - th2.cluster_bps) / max(th1.cluster_bps, 1e-9) < 0.05


def test_load_wrong_kind_ignored():
    cal = LiquiditySignalCalibrator()
    cal.load_symbol_state({"kind": "other", "symbol": "btcusdt", "n": 999})
    assert cal.n("btcusdt") == 0


def test_load_malformed_state_silent():
    cal = LiquiditySignalCalibrator()
    cal.load_symbol_state(None)
    cal.load_symbol_state("garbage")
    cal.load_symbol_state({"kind": "liquidity_signal"})
