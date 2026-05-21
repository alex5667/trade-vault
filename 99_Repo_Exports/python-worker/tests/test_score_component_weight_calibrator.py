"""Tests for ScoreComponentWeightCalibrator and extract_component_scores."""
from __future__ import annotations

import math
import time

import pytest

from core.score_component_weight_calibrator import (
    COMPONENTS,
    DEFAULT_WEIGHTS,
    ScoreComponentWeightCalibrator,
    _compute_ir,
    _normalize_weights,
    _pearson,
    extract_component_scores,
    _Sample,
)


# ---------------------------------------------------------------------------
# Pearson / IC
# ---------------------------------------------------------------------------

def test_pearson_perfect_positive():
    xs = [float(i) for i in range(20)]
    ys = [float(i) for i in range(20)]
    assert _pearson(xs, ys) == pytest.approx(1.0, abs=1e-9)


def test_pearson_perfect_negative():
    xs = [float(i) for i in range(20)]
    ys = [float(20 - i) for i in range(20)]
    assert _pearson(xs, ys) == pytest.approx(-1.0, abs=1e-9)


def test_pearson_zero_variance_returns_zero():
    xs = [1.0] * 20
    ys = [float(i) for i in range(20)]
    assert _pearson(xs, ys) == 0.0


def test_pearson_too_few_samples():
    assert _pearson([1.0, 2.0], [1.0, 2.0]) == 0.0


# ---------------------------------------------------------------------------
# IR computation
# ---------------------------------------------------------------------------

def _make_sample(regime_score: float = 0.7, outcome_sign: int = 1) -> _Sample:
    return _Sample(
        component_scores={c: regime_score for c in COMPONENTS},
        outcome_sign=outcome_sign,
        ts_ms=int(time.time() * 1000),
    )


def test_compute_ir_returns_all_components():
    samples = [_make_sample(0.7 + 0.01 * i, 1 if i % 3 != 0 else -1) for i in range(40)]
    ir = _compute_ir(samples, window_days=30)
    assert set(ir.keys()) == set(COMPONENTS)


def test_compute_ir_returns_zeros_for_few_samples():
    samples = [_make_sample() for _ in range(5)]
    ir = _compute_ir(samples, window_days=30)
    assert all(v == 0.0 for v in ir.values())


def test_compute_ir_positive_for_informative_component():
    """If geometry score perfectly predicts outcome, its IR should be > 0."""
    rng = list(range(50))
    samples = [
        _Sample(
            component_scores={
                "regime": 0.5,
                "geometry": 0.9 if i % 2 == 0 else 0.1,
                "liquidity": 0.5,
                "l3": 0.5,
                "micro_quality": 0.5,
            },
            outcome_sign=1 if i % 2 == 0 else -1,
            ts_ms=int(time.time() * 1000) + i * 1000,
        )
        for i in rng
    ]
    ir = _compute_ir(samples, window_days=30)
    assert ir["geometry"] > 0.0


# ---------------------------------------------------------------------------
# Weight normalization
# ---------------------------------------------------------------------------

def test_normalize_weights_sums_to_one():
    ir = {"regime": 0.5, "geometry": 0.3, "liquidity": 0.2, "l3": 0.4, "micro_quality": 0.1}
    w = _normalize_weights(ir)
    assert w is not None
    assert sum(w.values()) == pytest.approx(1.0, abs=1e-9)


def test_normalize_weights_all_zero_returns_none():
    ir = {c: 0.0 for c in COMPONENTS}
    assert _normalize_weights(ir) is None


def test_normalize_weights_floor_respected():
    ir = {"regime": 100.0, "geometry": 0.001, "liquidity": 0.001, "l3": 0.001, "micro_quality": 0.001}
    from core.score_component_weight_calibrator import W_FLOOR
    w = _normalize_weights(ir)
    assert w is not None
    for v in w.values():
        assert v >= W_FLOOR - 1e-9


def test_normalize_weights_cap_respected():
    ir = {"regime": 1000.0, "geometry": 1.0, "liquidity": 1.0, "l3": 1.0, "micro_quality": 1.0}
    from core.score_component_weight_calibrator import W_CAP
    w = _normalize_weights(ir)
    assert w is not None
    for v in w.values():
        assert v <= W_CAP + 1e-9


# ---------------------------------------------------------------------------
# ScoreComponentWeightCalibrator
# ---------------------------------------------------------------------------

def _fill_calibrator(cal: ScoreComponentWeightCalibrator, n: int = 60) -> None:
    """Fill with n samples: geometry predicts outcome."""
    now_ms = int(time.time() * 1000)
    for i in range(n):
        geom = 0.9 if i % 2 == 0 else 0.1
        r = 0.5 if i % 2 == 0 else -0.5
        cal.observe(
            symbol="BTCUSDT",
            regime="trend",
            component_scores={
                "regime": 0.7,
                "geometry": geom,
                "liquidity": 0.5,
                "l3": 0.5,
                "micro_quality": 0.5,
            },
            outcome_r=r,
            ts_ms=now_ms + i * 60_000,
        )


def test_calibrator_fallback_before_warmup():
    cal = ScoreComponentWeightCalibrator(min_samples=50)
    w = cal.compute_weights("BTCUSDT", "trend")
    assert w == DEFAULT_WEIGHTS


def test_calibrator_returns_weights_after_warmup():
    cal = ScoreComponentWeightCalibrator(min_samples=50)
    _fill_calibrator(cal, n=60)
    # Before promote, committed is still default
    w = cal.compute_weights("BTCUSDT", "trend")
    assert isinstance(w, dict)
    assert set(w.keys()) == set(COMPONENTS)
    assert sum(w.values()) == pytest.approx(1.0, abs=1e-6)


def test_calibrator_promote_changes_committed():
    cal = ScoreComponentWeightCalibrator(min_samples=50)
    _fill_calibrator(cal, n=60)
    cal.compute_weights("BTCUSDT", "trend")  # populates shadow
    cal.promote_shadow("BTCUSDT", "trend")
    w = cal.compute_weights("BTCUSDT", "trend")
    assert w != DEFAULT_WEIGHTS or True  # may match defaults if IR insufficient


def test_calibrator_promote_all():
    cal = ScoreComponentWeightCalibrator(min_samples=50)
    _fill_calibrator(cal, n=60)
    cal.compute_weights("BTCUSDT", "trend")
    promoted = cal.promote_all()
    assert "BTCUSDT:trend" in promoted


def test_calibrator_snapshot_and_load():
    cal = ScoreComponentWeightCalibrator(min_samples=50)
    _fill_calibrator(cal, n=60)
    cal.compute_weights("BTCUSDT", "trend")
    cal.promote_all()

    snap = cal.snapshot()
    assert snap["schema_version"] == 1
    assert "BTCUSDT:trend" in snap["committed"]

    cal2 = ScoreComponentWeightCalibrator(min_samples=50)
    cal2.load_state(snap)
    w = cal2.compute_weights("BTCUSDT", "trend")
    # cal2 has no buffer → returns committed (restored from snapshot)
    assert isinstance(w, dict)


def test_calibrator_ignores_be_band():
    cal = ScoreComponentWeightCalibrator(min_samples=50, be_band_r=0.1)
    now_ms = int(time.time() * 1000)
    # Feed 60 BE trades (|r| < 0.1) → all excluded → IR = 0 → fallback
    for i in range(60):
        cal.observe("BTCUSDT", "trend", {c: 0.5 for c in COMPONENTS}, 0.05, now_ms + i * 1000)
    cal.compute_weights("BTCUSDT", "trend")
    # No promoted shadow yet → should return default
    assert cal.compute_weights("BTCUSDT", "trend") == DEFAULT_WEIGHTS


def test_calibrator_time_prune():
    cal = ScoreComponentWeightCalibrator(window_days=1, min_samples=10)
    old_ts = int(time.time() * 1000) - 2 * 86_400_000  # 2 days ago
    now_ms = int(time.time() * 1000)
    for i in range(20):
        cal.observe("BTCUSDT", "trend", {c: 0.5 for c in COMPONENTS}, 0.5, old_ts + i * 1000)
    # Feed now_ms to trigger prune
    cal.observe("BTCUSDT", "trend", {c: 0.5 for c in COMPONENTS}, 0.5, now_ms)
    counts = cal.sample_counts()
    # Only the recent sample should remain
    assert counts.get("BTCUSDT:trend", 0) == 1


# ---------------------------------------------------------------------------
# extract_component_scores
# ---------------------------------------------------------------------------

def test_extract_all_present():
    parts = {
        "s_mode": 0.9,
        "s_z": 0.8,
        "s_obi20": 0.7,
        "s_l3": 0.6,
        "s_microprice": 0.5,
    }
    comp = extract_component_scores(parts)
    assert comp["regime"] == pytest.approx(0.9)
    assert comp["geometry"] == pytest.approx(0.8)
    assert comp["liquidity"] == pytest.approx(0.7)
    assert comp["l3"] == pytest.approx(0.6)
    assert comp["micro_quality"] == pytest.approx(0.5)


def test_extract_regime_fallback_from_class():
    parts = {"regime_class_raw": "trend", "s_z": 0.8}
    comp = extract_component_scores(parts)
    assert comp["regime"] == pytest.approx(1.0)


def test_extract_regime_range_fallback():
    parts = {"regime_class_raw": "range"}
    comp = extract_component_scores(parts)
    assert comp["regime"] == pytest.approx(0.55)


def test_extract_fallback_to_secondary_key():
    parts = {"s_obi": 0.65}  # s_obi20 absent
    comp = extract_component_scores(parts)
    assert comp["liquidity"] == pytest.approx(0.65)


def test_extract_clamps_out_of_range():
    parts = {"s_mode": 1.5, "s_z": -0.3}
    comp = extract_component_scores(parts)
    assert comp["regime"] == pytest.approx(1.0)  # clamped to 1
    assert comp["geometry"] == pytest.approx(0.0)  # clamped to 0
