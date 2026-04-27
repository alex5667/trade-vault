from __future__ import annotations

"""Tests for extended calibration metrics in confidence_cal_metrics.py."""

import sys
import os

# Ensure the python-worker path is on sys.path for imports
_pw = os.path.join(os.path.dirname(__file__), "..", "..", "..", "python-worker")
if os.path.isdir(_pw) and _pw not in sys.path:
    sys.path.insert(0, _pw)

from orderflow_services.confidence_cal_metrics import (
    emit_train_report,
    confidence_cal_train_mce_raw_gauge,
    confidence_cal_train_slope_raw_gauge,
    confidence_cal_train_intercept_cal_gauge,
    confidence_cal_train_sharpness_entropy_cal_gauge,
    confidence_cal_train_prob_mass_near_half_raw_gauge,
)


def _sample_value(metric, symbol: str):
    if metric is None:
        return None
    target = str(symbol)
    for mf in metric.collect():
        for sample in mf.samples:
            if sample.labels.get("symbol") == target:
                return float(sample.value)
    return None


def test_emit_train_report_surfaces_extended_metrics() -> None:
    """Extended metrics (MCE, slope, intercept, sharpness, prob_mass_near_half) must surface."""
    sym = "BTCUSDT_dq_test_extended"
    emit_train_report(
        sym,
        cal_type="isotonic",
        schema_version=2,
        raw_ece=0.11,
        cal_ece=0.07,
        raw_brier=0.20,
        cal_brier=0.16,
        raw_metrics={
            "mce": 0.31,
            "calibration_slope": 0.84,
            "prob_mass_near_half": 0.41,
        },
        cal_metrics={
            "calibration_intercept": -0.03,
            "sharpness_entropy": 0.72,
        },
    )

    assert _sample_value(confidence_cal_train_mce_raw_gauge, sym) == 0.31
    assert _sample_value(confidence_cal_train_slope_raw_gauge, sym) == 0.84
    assert _sample_value(confidence_cal_train_intercept_cal_gauge, sym) == -0.03
    assert _sample_value(confidence_cal_train_sharpness_entropy_cal_gauge, sym) == 0.72
    assert _sample_value(confidence_cal_train_prob_mass_near_half_raw_gauge, sym) == 0.41


def test_emit_train_report_no_crash_without_extended() -> None:
    """Bare call without optional dicts must still not raise."""
    emit_train_report(
        "ETHUSDT_bare_test",
        cal_type="temp_logit",
        schema_version=1,
        raw_ece=0.15,
        cal_ece=0.10,
        raw_brier=0.25,
        cal_brier=0.18,
    )
