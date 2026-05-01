# -*- coding: utf-8 -*-
import time

from orderflow_services.conf_score_guardrails_autopromo_controller_v1 import (
    extract_health_metrics,
    evaluate_canary,
)


def test_extract_health_metrics_flat():
    h = {"ts_ms": 1000, "degrade": 0, "ece_cal": 0.12, "brier_cal": 0.08, "n": 500}
    m = extract_health_metrics(h)
    assert m["ts_ms"] == 1000
    assert m["degrade"] == 0
    assert abs(m["ece_cal"] - 0.12) < 1e-9
    assert abs(m["brier_cal"] - 0.08) < 1e-9
    assert m["n"] == 500


def test_extract_health_metrics_nested_global():
    h = {"GLOBAL": {"ts_ms": 1000, "ece_cal": 0.11, "brier_cal": 0.07, "n": 300}, "status": {"degrade": 0}}
    m = extract_health_metrics(h)
    assert m["ts_ms"] == 1000
    assert m["degrade"] == 0
    assert abs(m["ece_cal"] - 0.11) < 1e-9
    assert abs(m["brier_cal"] - 0.07) < 1e-9
    assert m["n"] == 300


def test_evaluate_canary_regression_fails():
    baseline = {"ts_ms": 1000, "degrade": 0, "ece_cal": 0.10, "brier_cal": 0.06, "n": 500}
    current = {"ts_ms": 2000, "degrade": 0, "ece_cal": 0.12, "brier_cal": 0.07, "n": 500}
    res = evaluate_canary(
        baseline=baseline,
        current=current,
        now=2500,
        max_health_age_sec=10,
        min_n=200,
        max_delta_ece=0.01,
        max_delta_brier=0.005,
        max_arm_delta_ece=0.01,
        max_arm_delta_brier=0.01,
        max_cohort_delta_ece_wmean=0.01,
        max_cohort_delta_brier_wmean=0.01,
        max_cohort_delta_ece_max=0.02,
        max_cohort_delta_brier_max=0.02,
        allow_missing=False,
    )
    assert res.ok is False
    assert "ece_regression" in res.reasons or "brier_regression" in res.reasons


def test_evaluate_canary_degrade_fails():
    baseline = {"ts_ms": 1000, "degrade": 0, "ece_cal": 0.10, "brier_cal": 0.06}
    current = {"ts_ms": 2000, "degrade": 1, "ece_cal": 0.10, "brier_cal": 0.06}
    res = evaluate_canary(
        baseline=baseline,
        current=current,
        now=2500,
        max_health_age_sec=10,
        min_n=0,
        max_delta_ece=0.01,
        max_delta_brier=0.01,
        max_arm_delta_ece=0.01,
        max_arm_delta_brier=0.01,
        max_cohort_delta_ece_wmean=0.01,
        max_cohort_delta_brier_wmean=0.01,
        max_cohort_delta_ece_max=0.02,
        max_cohort_delta_brier_max=0.02,
        allow_missing=True,
    )
    assert res.ok is False
    assert "degrade_active" in res.reasons


def test_evaluate_canary_stale_health_fails():
    baseline = {"ts_ms": 1000, "degrade": 0, "ece_cal": 0.10, "brier_cal": 0.06}
    current = {"ts_ms": 1000, "degrade": 0, "ece_cal": 0.10, "brier_cal": 0.06}
    res = evaluate_canary(
        baseline=baseline,
        current=current,
        now=1000 + 1000 * 60,  # +60s
        max_health_age_sec=10,  # 10s
        min_n=0,
        max_delta_ece=0.01,
        max_delta_brier=0.01,
        max_arm_delta_ece=0.01,
        max_arm_delta_brier=0.01,
        max_cohort_delta_ece_wmean=0.01,
        max_cohort_delta_brier_wmean=0.01,
        max_cohort_delta_ece_max=0.02,
        max_cohort_delta_brier_max=0.02,
        allow_missing=False,
    ),
    assert res.ok is False
    assert "stale_health" in res.reasons

def test_extract_health_metrics_paired_and_cohorts():
    h = {
        "ts_ms": 1000,
        "status": {"degrade": 0},
        "arms": {"delta": {"ece_cal": 0.004, "brier_cal": 0.003, "n": 400}},
        "cohorts": {
            "agg": {"delta_ece_cal_wmean": 0.002, "delta_brier_cal_wmean": 0.001, "n": 400},
            "worst": {"delta_ece_cal_max": 0.01, "delta_brier_cal_max": 0.009, "key": "BTC|eu|trend", "n": 120},
        },
        "GLOBAL": {"ece_cal": 0.11, "brier_cal": 0.07, "n": 400},
    }
    m = extract_health_metrics(h)
    assert m["arm_delta_ece_cal"] == 0.004
    assert m["arm_delta_brier_cal"] == 0.003
    assert m["arm_n"] == 400
    assert m["cohort_delta_ece_cal_wmean"] == 0.002
    assert m["cohort_delta_brier_cal_wmean"] == 0.001
    assert m["cohort_delta_ece_cal_max"] == 0.01
    assert m["cohort_delta_brier_cal_max"] == 0.009

def test_evaluate_canary_paired_arm_regression_fails():
    baseline_raw = {"ts_ms": 1000, "degrade": 0, "ece_cal": 0.10, "brier_cal": 0.06, "n": 500}
    current_raw = {
        "ts_ms": 2000,
        "degrade": 0,
        "ece_cal": 0.10,
        "brier_cal": 0.06,
        "n": 500,
        "arms": {"delta": {"ece_cal": 0.02, "brier_cal": 0.0, "n": 500}},
    }
    baseline = extract_health_metrics(baseline_raw)
    current = extract_health_metrics(current_raw)
    res = evaluate_canary(
        baseline=baseline,
        current=current,
        now=2500,
        max_health_age_sec=10,
        min_n=200,
        max_delta_ece=0.01,
        max_delta_brier=0.01,
        max_arm_delta_ece=0.005,
        max_arm_delta_brier=0.01,
        max_cohort_delta_ece_wmean=0.01,
        max_cohort_delta_brier_wmean=0.01,
        max_cohort_delta_ece_max=0.02,
        max_cohort_delta_brier_max=0.02,
        allow_missing=True,
    )
    assert res.ok is False
    assert "arm_ece_regression" in res.reasons
