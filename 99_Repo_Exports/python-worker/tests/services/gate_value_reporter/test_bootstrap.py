"""Tests for services/gate_value_reporter/bootstrap.py."""

from __future__ import annotations

from services.gate_value_reporter.bootstrap import bootstrap_avg_r_lift


def test_bootstrap_empty_inputs_returns_zeros() -> None:
    ci = bootstrap_avg_r_lift([], [], n_boot=100, seed=1)
    assert ci.lo == 0.0
    assert ci.mid == 0.0
    assert ci.hi == 0.0


def test_bootstrap_deterministic_seed() -> None:
    passed = [1.0, 1.2, -0.5, 0.8, 1.5, -1.0]
    gated = [-1.0, -0.8, 0.2, -0.5, 0.0]
    a = bootstrap_avg_r_lift(passed, gated, n_boot=300, seed=42)
    b = bootstrap_avg_r_lift(passed, gated, n_boot=300, seed=42)
    assert a == b


def test_bootstrap_positive_lift_when_passed_clearly_better() -> None:
    passed = [1.0] * 50
    gated = [-1.0] * 50
    ci = bootstrap_avg_r_lift(passed, gated, n_boot=500, seed=42)
    assert ci.lo > 1.9
    assert ci.hi > 1.9
    assert abs(ci.mid - 2.0) < 1e-6


def test_bootstrap_negative_lift_when_gated_better() -> None:
    passed = [-0.5] * 40
    gated = [1.0] * 40
    ci = bootstrap_avg_r_lift(passed, gated, n_boot=500, seed=42)
    assert ci.hi < 0
    assert abs(ci.mid - (-1.5)) < 1e-6


def test_bootstrap_interval_ordering() -> None:
    passed = [0.5, -0.3, 1.2, -0.8, 0.9, 0.1, -0.2]
    gated = [-0.1, 0.2, -0.4, 0.1, -0.3, 0.0]
    ci = bootstrap_avg_r_lift(passed, gated, n_boot=500, seed=7)
    assert ci.lo <= ci.mid <= ci.hi
