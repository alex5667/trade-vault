"""Tests for services/gate_value_reporter/decision.py."""

from __future__ import annotations

from services.gate_value_reporter.contracts import (
    CohortStats,
    ConfidenceInterval,
    GateLiftStats,
)
from services.gate_value_reporter.decision import decide_gate_action


def _cohort(
    *,
    n: int,
    avg_r: float,
    win_rate: float = 0.4,
    pf: float = 1.0,
    sl_rate: float = 0.3,
) -> CohortStats:
    return CohortStats(
        n=n,
        win_rate=win_rate,
        avg_r=avg_r,
        median_r=avg_r,
        p25_r=avg_r,
        p75_r=avg_r,
        profit_factor=pf,
        tp_hit_rate=win_rate,
        sl_hit_rate=sl_rate,
        timeout_rate=max(0.0, 1.0 - win_rate - sl_rate),
        avg_ret_bps=0.0,
    )


def _lift(*, avg_r_lift: float, fn_rate: float = 0.2) -> GateLiftStats:
    return GateLiftStats(
        avg_r_lift=avg_r_lift,
        win_rate_lift=0.05,
        profit_factor_lift=0.0,
        sl_hit_rate_reduction=0.0,
        false_negative_rate=fn_rate,
    )


def test_decision_insufficient_data_when_passed_small() -> None:
    res = decide_gate_action(
        passed=_cohort(n=10, avg_r=0.1),
        gated_out=_cohort(n=1000, avg_r=-0.1),
        lift=_lift(avg_r_lift=0.2),
        avg_r_ci=ConfidenceInterval(0.1, 0.2, 0.3),
        min_n_passed=500,
        min_n_gated_out=500,
        min_avg_r_lift=0.05,
        max_false_negative_rate=0.25,
    )
    assert res.decision == "INSUFFICIENT_DATA"
    assert "passed_n_too_low" in res.reason_codes


def test_decision_insufficient_data_when_gated_small() -> None:
    res = decide_gate_action(
        passed=_cohort(n=1000, avg_r=0.1),
        gated_out=_cohort(n=20, avg_r=-0.1),
        lift=_lift(avg_r_lift=0.2),
        avg_r_ci=ConfidenceInterval(0.1, 0.2, 0.3),
        min_n_passed=500,
        min_n_gated_out=500,
        min_avg_r_lift=0.05,
        max_false_negative_rate=0.25,
    )
    assert res.decision == "INSUFFICIENT_DATA"
    assert "gated_out_n_too_low" in res.reason_codes


def test_decision_keep_gate_positive_lift_with_better_pf() -> None:
    res = decide_gate_action(
        passed=_cohort(n=1000, avg_r=0.1, pf=1.5),
        gated_out=_cohort(n=900, avg_r=-0.1, pf=0.7),
        lift=_lift(avg_r_lift=0.2),
        avg_r_ci=ConfidenceInterval(0.08, 0.2, 0.3),
        min_n_passed=500,
        min_n_gated_out=500,
        min_avg_r_lift=0.05,
        max_false_negative_rate=0.25,
    )
    assert res.decision == "KEEP_GATE"
    assert res.severity == "info"


def test_decision_relax_gate_when_ci_upper_negative() -> None:
    res = decide_gate_action(
        passed=_cohort(n=1000, avg_r=-0.05, pf=0.9),
        gated_out=_cohort(n=900, avg_r=0.1, pf=1.4),
        lift=_lift(avg_r_lift=-0.15),
        avg_r_ci=ConfidenceInterval(-0.3, -0.15, -0.02),
        min_n_passed=500,
        min_n_gated_out=500,
        min_avg_r_lift=0.05,
        max_false_negative_rate=0.25,
    )
    assert res.decision == "RELAX_GATE"
    assert "gated_out_avg_r_better" in res.reason_codes


def test_decision_relax_gate_high_false_negative_rate() -> None:
    res = decide_gate_action(
        passed=_cohort(n=1000, avg_r=0.1, pf=1.1),
        gated_out=_cohort(n=900, avg_r=0.0, win_rate=0.4, pf=1.0),
        lift=_lift(avg_r_lift=0.1, fn_rate=0.4),
        avg_r_ci=ConfidenceInterval(-0.05, 0.1, 0.25),
        min_n_passed=500,
        min_n_gated_out=500,
        min_avg_r_lift=0.05,
        max_false_negative_rate=0.25,
    )
    assert res.decision == "RELAX_GATE"
    assert "false_negative_rate_high" in res.reason_codes


def test_decision_disable_gate_when_passed_negative() -> None:
    res = decide_gate_action(
        passed=_cohort(n=1000, avg_r=-0.2, pf=0.6),
        gated_out=_cohort(n=900, avg_r=-0.3, pf=0.5),
        lift=_lift(avg_r_lift=0.1, fn_rate=0.1),
        avg_r_ci=ConfidenceInterval(-0.05, 0.1, 0.25),
        min_n_passed=500,
        min_n_gated_out=500,
        min_avg_r_lift=0.05,
        max_false_negative_rate=0.25,
    )
    assert res.decision == "DISABLE_GATE"
    assert res.severity == "critical"


def test_decision_inconclusive_when_ci_straddles_zero() -> None:
    res = decide_gate_action(
        passed=_cohort(n=1000, avg_r=0.05, pf=1.05),
        gated_out=_cohort(n=900, avg_r=0.02, pf=1.02),
        lift=_lift(avg_r_lift=0.03, fn_rate=0.2),
        avg_r_ci=ConfidenceInterval(-0.05, 0.03, 0.11),
        min_n_passed=500,
        min_n_gated_out=500,
        min_avg_r_lift=0.05,
        max_false_negative_rate=0.25,
    )
    assert res.decision == "INCONCLUSIVE"
