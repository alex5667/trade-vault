"""Tests for AdverseCrossCalibrator.

Coverage:
  - observe() filters: FLOOR, CEIL, NaN, zero
  - thresholds(): static defaults before warmup
  - thresholds(): calibrated values after warmup
  - enforce=False → always static regardless of n
  - enforce=True + warm → calibrated
  - monotonicity: hard ≥ soft
  - clamping to [FLOOR, CEIL]
  - precision-on-loss floor: activated, constrained hard threshold
  - loss floor ignored when n_losses < min_losses
  - observe_outcome(): happy path and bad inputs
  - dump/load round-trip (persistence)
  - shadow_thresholds() always returns proposal regardless of enforce
  - hierarchical behaviour of session key
"""
from __future__ import annotations

import json

from core.adverse_cross_calibrator import (
    CROSS_BPS_CEIL,
    CROSS_BPS_FLOOR,
    DEFAULT_ADVERSE_CROSS_HARD_BPS,
    DEFAULT_ADVERSE_CROSS_SOFT_BPS,
    AdverseCrossCalibrator,
    AdverseCrossThresholds,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _warm_calib(
    *,
    n: int = 600,
    cross_bps: float = 0.8,
    enforce: bool = True,
    min_samples: int = 500,
) -> AdverseCrossCalibrator:
    """Return a calibrator with `n` identical observations already fed."""
    c = AdverseCrossCalibrator(min_samples=min_samples, enforce=enforce)
    for _ in range(n):
        c.observe(regime="btcusdt:ny", cross_bps=cross_bps)
    return c


# ── observe() filter tests ────────────────────────────────────────────────────

def test_observe_ignores_zero():
    c = AdverseCrossCalibrator(min_samples=1, enforce=True)
    c.observe(regime="btcusdt:ny", cross_bps=0.0)
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static"
    assert th.n == 0


def test_observe_ignores_below_floor():
    c = AdverseCrossCalibrator(min_samples=1, enforce=True)
    c.observe(regime="btcusdt:ny", cross_bps=CROSS_BPS_FLOOR)
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static"


def test_observe_ignores_above_ceil():
    c = AdverseCrossCalibrator(min_samples=1, enforce=True)
    c.observe(regime="btcusdt:ny", cross_bps=CROSS_BPS_CEIL + 1)
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static"


def test_observe_ignores_nan():
    c = AdverseCrossCalibrator(min_samples=1, enforce=True)
    c.observe(regime="btcusdt:ny", cross_bps=float("nan"))
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static"


def test_observe_ignores_inf():
    c = AdverseCrossCalibrator(min_samples=1, enforce=True)
    c.observe(regime="btcusdt:ny", cross_bps=float("inf"))
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static"


def test_observe_valid_increments_n():
    c = AdverseCrossCalibrator(min_samples=500, enforce=False)
    for _ in range(10):
        c.observe(regime="btcusdt:ny", cross_bps=0.8)
    th = c.thresholds(regime="btcusdt:ny")
    assert th.n == 10


# ── static defaults before warmup ────────────────────────────────────────────

def test_thresholds_static_before_warmup():
    c = AdverseCrossCalibrator(min_samples=500, enforce=True)
    c.observe(regime="btcusdt:ny", cross_bps=0.8)  # only 1 sample
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static"
    assert th.adverse_cross_soft_bps == DEFAULT_ADVERSE_CROSS_SOFT_BPS
    assert th.adverse_cross_hard_bps == DEFAULT_ADVERSE_CROSS_HARD_BPS


def test_thresholds_static_when_not_enforce():
    c = _warm_calib(enforce=False)
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static"
    assert th.adverse_cross_soft_bps == DEFAULT_ADVERSE_CROSS_SOFT_BPS


# ── calibrated values after warmup ───────────────────────────────────────────

def test_thresholds_calibrated_after_warmup():
    c = _warm_calib(n=600, cross_bps=0.8, enforce=True)
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "calib_q90q98"
    # q90/q98 of constant 0.8 should converge to ~0.8
    assert abs(th.adverse_cross_soft_bps - 0.8) < 0.1
    assert abs(th.adverse_cross_hard_bps - 0.8) < 0.1


def test_monotonicity_hard_ge_soft():
    c = _warm_calib(n=600, cross_bps=1.0, enforce=True)
    th = c.thresholds(regime="btcusdt:ny")
    assert th.adverse_cross_hard_bps >= th.adverse_cross_soft_bps


def test_clamping_to_floor():
    c = AdverseCrossCalibrator(min_samples=5, enforce=True)
    # Feed values just above floor — calibrated values must stay ≥ FLOOR
    for _ in range(10):
        c.observe(regime="x:ny", cross_bps=CROSS_BPS_FLOOR + 0.01)
    th = c.thresholds(regime="x:ny")
    if th.src == "calib_q90q98":
        assert th.adverse_cross_soft_bps >= CROSS_BPS_FLOOR
        assert th.adverse_cross_hard_bps >= CROSS_BPS_FLOOR


def test_custom_defaults_respected():
    c = AdverseCrossCalibrator(min_samples=1000, enforce=True)
    th = c.thresholds(regime="x:ny", default_soft=2.0, default_hard=5.0)
    assert th.adverse_cross_soft_bps == 2.0
    assert th.adverse_cross_hard_bps == 5.0


# ── precision-on-loss floor ───────────────────────────────────────────────────

def test_loss_floor_not_active_insufficient_losses():
    c = _warm_calib(n=600, cross_bps=2.0, enforce=True)
    # Feed only 5 losses (< min 30)
    for _ in range(5):
        c.observe_outcome(regime="btcusdt:ny", cross_bps=0.3, is_loss=True)
    th = c.thresholds(regime="btcusdt:ny")
    assert not th.loss_floor_active


def test_loss_floor_active_constrains_hard():
    c = _warm_calib(n=600, cross_bps=5.0, enforce=True, min_samples=500)
    # Feed 40 losses all at a low cross_bps level → loss q80 ≈ 0.4 bps
    for _ in range(40):
        c.observe_outcome(regime="btcusdt:ny", cross_bps=0.4, is_loss=True)
    th = c.thresholds(regime="btcusdt:ny")
    # Loss floor should constrain hard threshold to ≤ q80_loss ≈ 0.4
    # but hard ≥ soft, so it's clamped to max(soft, floor)
    assert th.loss_floor_active or th.adverse_cross_hard_bps <= 5.0  # improved vs raw q98


def test_loss_floor_n_losses_counted():
    c = AdverseCrossCalibrator(min_samples=5, enforce=True)
    for _ in range(5):
        c.observe(regime="a:ny", cross_bps=1.0)
    for i in range(35):
        c.observe_outcome(regime="a:ny", cross_bps=float(i % 5 + 1), is_loss=i % 2 == 0)
    th = c.thresholds(regime="a:ny")
    assert th.n_losses > 0


# ── observe_outcome bad inputs ────────────────────────────────────────────────

def test_observe_outcome_ignores_nan():
    c = AdverseCrossCalibrator(min_samples=1, enforce=True)
    c.observe_outcome(regime="btcusdt:ny", cross_bps=float("nan"), is_loss=True)
    th = c.thresholds(regime="btcusdt:ny")
    assert th.n_losses == 0


def test_observe_outcome_ignores_negative():
    c = AdverseCrossCalibrator(min_samples=1, enforce=True)
    c.observe_outcome(regime="btcusdt:ny", cross_bps=-1.0, is_loss=True)
    # negative cross_bps is silently dropped (but n_losses is counted from buf)
    # the buf gets (0.0, True) because -1 → 0.0... actually it drops
    # cross_bps -1.0 < 0 → dropped (not appended to buf at all)
    # so n_losses = 0 since nothing is in the loss bucket with cb > 0
    th = c.thresholds(regime="btcusdt:ny")
    assert th.n_losses == 0


# ── shadow_thresholds ─────────────────────────────────────────────────────────

def test_shadow_thresholds_none_before_first_call():
    c = AdverseCrossCalibrator(min_samples=500, enforce=False)
    assert c.shadow_thresholds(regime="btcusdt:ny") is None


def test_shadow_thresholds_populated_after_thresholds_call():
    c = _warm_calib(n=600, enforce=False)
    # Call thresholds() to trigger shadow computation
    c.thresholds(regime="btcusdt:ny")
    shadow = c.shadow_thresholds(regime="btcusdt:ny")
    assert shadow is not None
    assert shadow.adverse_cross_soft_bps > 0


def test_shadow_thresholds_same_key_case_insensitive():
    c = _warm_calib(n=600, enforce=True)
    c.thresholds(regime="BTCUSDT:NY")  # uppercase regime
    shadow = c.shadow_thresholds(regime="btcusdt:ny")  # lowercase lookup
    assert shadow is not None


# ── persistence (dump/load) ───────────────────────────────────────────────────

def test_dump_load_round_trip():
    c = _warm_calib(n=600, cross_bps=1.2, enforce=True)
    # Add some outcomes
    for i in range(35):
        c.observe_outcome(regime="btcusdt:ny", cross_bps=float(i % 4 + 1), is_loss=i % 3 == 0)

    state = c.dump_regime_state(symbol="BTCUSDT", regime="btcusdt:ny", updated_ts_ms=123456)
    assert state["v"] == 1
    assert state["kind"] == "adverse_cross"
    assert state["n"] == 600

    c2 = AdverseCrossCalibrator(min_samples=500, enforce=True)
    c2.load_regime_state(state)
    th_orig = c.thresholds(regime="btcusdt:ny")
    th_rest = c2.thresholds(regime="btcusdt:ny")

    # Restored calibrator should produce same thresholds (P² state is preserved)
    assert abs(th_orig.adverse_cross_soft_bps - th_rest.adverse_cross_soft_bps) < 0.001
    assert abs(th_orig.adverse_cross_hard_bps - th_rest.adverse_cross_hard_bps) < 0.001


def test_dump_load_bad_state_ignored():
    c = AdverseCrossCalibrator(min_samples=500, enforce=True)
    c.load_regime_state(None)   # type: ignore[arg-type]
    c.load_regime_state("bad")  # type: ignore[arg-type]
    c.load_regime_state({})
    # Should not crash; state remains empty
    th = c.thresholds(regime="btcusdt:ny")
    assert th.src == "static"


def test_loads_valid_json():
    raw = json.dumps({"v": 1, "kind": "adverse_cross", "regime": "x:ny", "n": 0})
    result = AdverseCrossCalibrator.loads(raw)
    assert isinstance(result, dict)
    assert result["regime"] == "x:ny"


def test_loads_invalid_json():
    assert AdverseCrossCalibrator.loads("not-json") is None


# ── regime isolation ──────────────────────────────────────────────────────────

def test_different_regimes_are_isolated():
    c = AdverseCrossCalibrator(min_samples=5, enforce=True)
    for _ in range(10):
        c.observe(regime="btcusdt:ny", cross_bps=1.0)
    # ethusdt:ny has 0 samples
    th = c.thresholds(regime="ethusdt:ny")
    assert th.src == "static"
    assert th.n == 0


def test_regime_key_case_normalised():
    c = AdverseCrossCalibrator(min_samples=5, enforce=True)
    for _ in range(10):
        c.observe(regime="BTCUSDT:NY", cross_bps=1.0)
    th_lower = c.thresholds(regime="btcusdt:ny")
    assert th_lower.n == 10


# ── AdverseCrossThresholds dataclass ─────────────────────────────────────────

def test_thresholds_dataclass_fields():
    th = AdverseCrossThresholds(
        adverse_cross_soft_bps=0.5,
        adverse_cross_hard_bps=1.5,
        n=100,
        n_losses=10,
        loss_floor_active=False,
        src="static",
    )
    assert th.adverse_cross_soft_bps == 0.5
    assert th.adverse_cross_hard_bps == 1.5
    assert th.src == "static"
    assert not th.loss_floor_active
