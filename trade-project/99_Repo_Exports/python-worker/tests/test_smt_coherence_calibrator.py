"""Tests for SmtCoherenceCalibrator (core/smt_coherence_calibrator.py)."""
from __future__ import annotations

import random

from core.smt_coherence_calibrator import (
    COH_CEIL,
    COH_FLOOR,
    DEFAULT_COH_MIN,
    SmtCoherenceCalibrator,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _warm(cal: SmtCoherenceCalibrator, regime: str, n: int = 220,
          seed: int = 42, mean: float = 0.72, std: float = 0.08) -> None:
    rng = random.Random(seed)
    for _ in range(n):
        val = rng.gauss(mean, std)
        val = max(COH_FLOOR, min(COH_CEIL, val))
        cal.observe(regime=regime, coh=val)


# ── cold / warmup ─────────────────────────────────────────────────────────────

def test_cold_returns_static_default():
    cal = SmtCoherenceCalibrator(min_samples=200)
    th = cal.thresholds(regime="btcusdt:smt")
    assert th.coh_min == DEFAULT_COH_MIN
    assert th.src == "static"
    assert th.n == 0


def test_enforce_still_defaults_while_cold():
    # enforce=True + n < min_samples: value stays at default (quantile not converged).
    cal = SmtCoherenceCalibrator(min_samples=200, enforce=True)
    for _ in range(10):
        cal.observe(regime="x", coh=0.75)
    th = cal.thresholds(regime="x")
    assert th.coh_min == DEFAULT_COH_MIN
    assert th.n == 10


def test_shadow_after_warmup():
    cal = SmtCoherenceCalibrator(min_samples=50)
    _warm(cal, "btcusdt:smt", n=60)
    cal.thresholds(regime="btcusdt:smt")
    shadow = cal.shadow_thresholds(regime="btcusdt:smt")
    assert shadow is not None
    assert shadow.src == "calib_q80"


# ── filters ──────────────────────────────────────────────────────────────────

def test_nan_ignored():
    cal = SmtCoherenceCalibrator()
    cal.observe(regime="x", coh=float("nan"))
    assert cal.n("x") == 0


def test_inf_ignored():
    cal = SmtCoherenceCalibrator()
    cal.observe(regime="x", coh=float("inf"))
    assert cal.n("x") == 0


def test_below_floor_ignored():
    cal = SmtCoherenceCalibrator()
    cal.observe(regime="x", coh=COH_FLOOR - 0.01)
    assert cal.n("x") == 0


def test_above_ceil_ignored():
    cal = SmtCoherenceCalibrator()
    cal.observe(regime="x", coh=COH_CEIL + 0.01)
    assert cal.n("x") == 0


def test_boundary_accepted():
    cal = SmtCoherenceCalibrator()
    cal.observe(regime="x", coh=COH_FLOOR)
    cal.observe(regime="x", coh=COH_CEIL)
    assert cal.n("x") == 2


# ── enforce / calibrated values ──────────────────────────────────────────────

def test_low_coh_distribution_lowers_coh_min():
    """On symbol with persistently low coherence, coh_min should drop."""
    cal = SmtCoherenceCalibrator(min_samples=50, enforce=True)
    for _ in range(60):
        cal.observe(regime="r", coh=0.40)
    th = cal.thresholds(regime="r")
    assert th.src == "calib_q80"
    assert th.coh_min < DEFAULT_COH_MIN


def test_high_coh_distribution_raises_coh_min():
    """On symbol with high base coherence, coh_min should rise."""
    cal = SmtCoherenceCalibrator(min_samples=50, enforce=True)
    for _ in range(60):
        cal.observe(regime="r", coh=0.90)
    th = cal.thresholds(regime="r")
    assert th.src == "calib_q80"
    assert th.coh_min > DEFAULT_COH_MIN


def test_calibrated_value_within_rails():
    cal = SmtCoherenceCalibrator(min_samples=50, enforce=True)
    _warm(cal, "r", n=60)
    th = cal.thresholds(regime="r")
    assert COH_FLOOR <= th.coh_min <= COH_CEIL


# ── hysteresis ───────────────────────────────────────────────────────────────

def test_hysteresis_prevents_micro_update():
    cal = SmtCoherenceCalibrator(min_samples=10, enforce=True, update_band=0.20)
    for _ in range(15):
        cal.observe(regime="r", coh=0.75)
    th1 = cal.thresholds(regime="r")
    committed = th1.coh_min

    cal.observe(regime="r", coh=0.751)
    th2 = cal.thresholds(regime="r")
    assert abs(th2.coh_min - committed) < 0.20 + 0.01


# ── regime independence ───────────────────────────────────────────────────────

def test_two_regimes_independent():
    cal = SmtCoherenceCalibrator(min_samples=50, enforce=True)
    for _ in range(60):
        cal.observe(regime="btcusdt:smt", coh=0.45)
    for _ in range(60):
        cal.observe(regime="solusdt:smt", coh=0.90)

    th_btc = cal.thresholds(regime="btcusdt:smt")
    th_sol = cal.thresholds(regime="solusdt:smt")
    assert th_sol.coh_min > th_btc.coh_min


def test_regime_key_normalised():
    cal = SmtCoherenceCalibrator(min_samples=5, enforce=True)
    for _ in range(10):
        cal.observe(regime="BTCUSDT:SMT", coh=0.7)
    assert cal.n("btcusdt:smt") == 10


# ── persistence ──────────────────────────────────────────────────────────────

def test_dump_load_roundtrip():
    cal = SmtCoherenceCalibrator(min_samples=50, enforce=True)
    _warm(cal, "ethusdt:smt", n=60)
    th_before = cal.thresholds(regime="ethusdt:smt")

    state = cal.dump_regime_state(symbol="ETHUSDT", regime="ethusdt:smt", updated_ts_ms=0)
    cal2 = SmtCoherenceCalibrator(min_samples=50, enforce=True)
    cal2.load_regime_state(state)
    th_after = cal2.thresholds(regime="ethusdt:smt")

    assert th_after.n == th_before.n
    assert th_after.src == th_before.src
    assert abs(th_after.coh_min - th_before.coh_min) < 0.05


def test_dump_kind_field():
    cal = SmtCoherenceCalibrator()
    state = cal.dump_regime_state(symbol="X", regime="x", updated_ts_ms=0)
    assert state["kind"] == "smt_coherence"
    assert state["v"] == 1


def test_load_wrong_kind_noop():
    cal = SmtCoherenceCalibrator(min_samples=10, enforce=True)
    for _ in range(15):
        cal.observe(regime="r", coh=0.7)
    before_n = cal.n("r")
    cal.load_regime_state({"kind": "funding_basis", "regime": "r", "n": 99999})
    assert cal.n("r") == before_n


def test_load_corrupt_state_noop():
    cal = SmtCoherenceCalibrator()
    cal.load_regime_state(None)
    cal.load_regime_state(42)
    cal.load_regime_state({"kind": "smt_coherence", "regime": None, "n": "bad"})


# ── shadow ────────────────────────────────────────────────────────────────────

def test_shadow_none_before_first_call():
    cal = SmtCoherenceCalibrator()
    assert cal.shadow_thresholds(regime="new") is None


def test_shadow_available_after_thresholds():
    cal = SmtCoherenceCalibrator(min_samples=10)
    _warm(cal, "r", n=15)
    cal.thresholds(regime="r")
    assert cal.shadow_thresholds(regime="r") is not None


# ── realistic distribution ────────────────────────────────────────────────────

def test_q80_typical_smt_distribution():
    rng = random.Random(7)
    cal = SmtCoherenceCalibrator(min_samples=100, enforce=True)
    for _ in range(120):
        val = rng.gauss(0.72, 0.10)
        val = max(COH_FLOOR, min(COH_CEIL, val))
        cal.observe(regime="btcusdt:smt", coh=val)
    th = cal.thresholds(regime="btcusdt:smt")
    assert COH_FLOOR <= th.coh_min <= COH_CEIL
    # q80 of N(0.72, 0.10) ≈ 0.72 + 0.84×0.10 ≈ 0.804
    assert 0.70 <= th.coh_min <= 0.90
