"""Tests that degenerate calibration is surfaced as invalid, not as "perfect".

P0-1: calibration_regression must return calibration_valid=False for all
degenerate scenarios so that promoters can reject unmeasurable models.
"""
import math
import pytest
from ml_analysis.calibration_extended import calibration_regression, report
from ml_analysis.tools.conf_cal_promotion_manager_v1 import compute_metrics


# ── calibration_regression unit tests ────────────────────────────────────────

def test_too_few_rows_is_invalid():
    result = calibration_regression([1, 0], [0.9, 0.1])
    assert result["calibration_valid"] is False
    assert result["calibration_status"] == "degenerate_too_few_rows"
    assert result["calibration_slope"] == 1.0
    assert result["calibration_intercept"] == 0.0


def test_single_class_all_ones_is_invalid():
    result = calibration_regression([1, 1, 1, 1, 1], [0.9, 0.8, 0.7, 0.6, 0.85])
    assert result["calibration_valid"] is False
    assert result["calibration_status"] == "degenerate_single_class"


def test_single_class_all_zeros_is_invalid():
    result = calibration_regression([0, 0, 0, 0, 0], [0.1, 0.2, 0.15, 0.05, 0.12])
    assert result["calibration_valid"] is False
    assert result["calibration_status"] == "degenerate_single_class"


def test_valid_mixed_labels_returns_valid():
    y = [1, 0, 1, 0, 1, 0, 1, 0, 1, 0]
    p = [0.9, 0.1, 0.8, 0.2, 0.7, 0.3, 0.85, 0.15, 0.75, 0.25]
    result = calibration_regression(y, p)
    assert result["calibration_valid"] is True
    assert result["calibration_status"] == "ok"
    assert math.isfinite(result["calibration_slope"])
    assert math.isfinite(result["calibration_intercept"])


# ── report() propagates calibration_valid ────────────────────────────────────

def test_report_empty_is_invalid():
    result = report([], [])
    assert result["calibration_valid"] is False
    assert result["calibration_status"] == "degenerate_empty"


def test_report_single_class_is_invalid():
    y = [1] * 20
    p = [0.8] * 20
    result = report(y, p)
    assert result["calibration_valid"] is False


def test_report_valid_propagates_ok():
    y = [1, 0] * 15
    p = [0.85, 0.15] * 15
    result = report(y, p)
    assert result["calibration_valid"] is True
    assert result["calibration_status"] == "ok"


# ── compute_metrics propagates calibration_valid ──────────────────────────────

def test_compute_metrics_empty_is_invalid():
    m = compute_metrics([], [])
    assert m["calibration_valid"] is False
    assert m["calibration_status"] == "degenerate_empty"


def test_compute_metrics_single_class_is_invalid():
    y = [1] * 20
    p = [0.9] * 20
    m = compute_metrics(y, p)
    assert m["calibration_valid"] is False


def test_compute_metrics_valid_has_ok_status():
    y = [1, 0] * 25
    p = [0.9, 0.1] * 25
    m = compute_metrics(y, p)
    assert m["calibration_valid"] is True
    assert m["calibration_status"] == "ok"


# ── Promoter gate: degenerate calibration must block promotion ───────────────

class _FakeArgs:
    force = False


def _make_cand_metrics(*, calibration_valid: bool, calibration_status: str = "ok") -> dict:
    """Candidate metrics with all other thresholds passing cleanly."""
    return {
        "ece": 0.03,
        "mce": 0.06,
        "brier": 0.18,
        "precision_top5p": 0.60,
        "calibration_slope": 1.0,
        "calibration_intercept": 0.0,
        "calibration_valid": calibration_valid,
        "calibration_status": calibration_status,
        "sharpness_mean": 0.15,
        "prob_mass_near_half": 0.30,
        "n": 500,
    }


def _make_champ_metrics() -> dict:
    return {
        "ece": 0.05,
        "mce": 0.10,
        "brier": 0.22,
        "precision_top5p": 0.50,
        "calibration_slope": 1.0,
        "calibration_intercept": 0.0,
        "calibration_valid": True,
        "calibration_status": "ok",
        "sharpness_mean": 0.12,
        "prob_mass_near_half": 0.35,
        "n": 400,
    }


def _run_promotion_gate(cand_metrics: dict, champ_metrics: dict, monkeypatch) -> tuple[bool, str]:
    """Inline the validation logic from conf_cal_promotion_manager_v1.main()."""
    import math, os
    from ml_analysis.tools.conf_cal_promotion_manager_v1 import (
        DEFAULT_MIN_N_24H, DEFAULT_MAX_ECE, DEFAULT_MAX_BRIER,
        DEFAULT_MIN_PREC_TOP5P, DEFAULT_MIN_DELTA_ECE,
    )

    thresholds = {
        "min_n": float(DEFAULT_MIN_N_24H),
        "max_ece": float(DEFAULT_MAX_ECE),
        "max_mce": 0.12,
        "max_brier": float(DEFAULT_MAX_BRIER),
        "min_prec": float(DEFAULT_MIN_PREC_TOP5P),
        "min_cal_slope": 0.70,
        "max_abs_cal_intercept": 0.20,
        "min_sharpness_mean": 0.02,
        "max_prob_mass_near_half": 0.60,
        "min_delta_ece": float(DEFAULT_MIN_DELTA_ECE),
        "max_mce_regression": 0.002,
        "max_sharpness_drop": 0.05,
    }

    is_valid = True
    reasons = []

    if cand_metrics["n"] < thresholds["min_n"]:
        is_valid = False
        reasons.append("n_too_small")
    if cand_metrics["ece"] > thresholds["max_ece"]:
        is_valid = False
        reasons.append("ece_too_high")
    if cand_metrics["mce"] > thresholds["max_mce"]:
        is_valid = False
        reasons.append("mce_too_high")
    if cand_metrics["brier"] > thresholds["max_brier"]:
        is_valid = False
        reasons.append("brier_too_high")
    if cand_metrics["precision_top5p"] < thresholds["min_prec"]:
        is_valid = False
        reasons.append("prec_too_low")

    # P0-1 gate
    if not cand_metrics.get("calibration_valid", True):
        is_valid = False
        status = cand_metrics.get("calibration_status", "unknown")
        reasons.append(f"calibration_unmeasurable: {status} (PROMO_DENY_CALIBRATION_UNMEASURABLE)")

    if math.isfinite(float(cand_metrics.get("calibration_slope", float("nan")))) and cand_metrics["calibration_slope"] < thresholds["min_cal_slope"]:
        is_valid = False
        reasons.append("slope_too_low")
    if math.isfinite(float(cand_metrics.get("calibration_intercept", float("nan")))) and abs(cand_metrics["calibration_intercept"]) > thresholds["max_abs_cal_intercept"]:
        is_valid = False
        reasons.append("intercept_too_high")

    if not is_valid:
        return False, f"Candidate invalid: {', '.join(reasons)}"

    ece_imp = champ_metrics["ece"] - cand_metrics["ece"]
    if ece_imp > thresholds["min_delta_ece"]:
        return True, f"Improvement ECE {ece_imp:.4f}"
    if champ_metrics["n"] < 10:
        return True, "First valid champion"
    return False, f"No significant improvement (ECE delta {ece_imp:.4f})"


def test_degenerate_too_few_rows_blocks_promotion(monkeypatch):
    cand = _make_cand_metrics(
        calibration_valid=False,
        calibration_status="degenerate_too_few_rows",
    )
    allow, reason = _run_promotion_gate(cand, _make_champ_metrics(), monkeypatch)
    assert allow is False
    assert "PROMO_DENY_CALIBRATION_UNMEASURABLE" in reason
    assert "degenerate_too_few_rows" in reason


def test_degenerate_single_class_blocks_promotion(monkeypatch):
    cand = _make_cand_metrics(
        calibration_valid=False,
        calibration_status="degenerate_single_class",
    )
    allow, reason = _run_promotion_gate(cand, _make_champ_metrics(), monkeypatch)
    assert allow is False
    assert "PROMO_DENY_CALIBRATION_UNMEASURABLE" in reason


def test_degenerate_singular_matrix_blocks_promotion(monkeypatch):
    cand = _make_cand_metrics(
        calibration_valid=False,
        calibration_status="degenerate_singular_matrix",
    )
    allow, reason = _run_promotion_gate(cand, _make_champ_metrics(), monkeypatch)
    assert allow is False
    assert "PROMO_DENY_CALIBRATION_UNMEASURABLE" in reason


def test_valid_calibration_can_be_promoted(monkeypatch):
    """Sanity: a genuinely well-calibrated model with ECE improvement passes."""
    cand = _make_cand_metrics(calibration_valid=True, calibration_status="ok")
    allow, reason = _run_promotion_gate(cand, _make_champ_metrics(), monkeypatch)
    # ECE improvement: 0.05 - 0.03 = 0.02 > DEFAULT_MIN_DELTA_ECE (typically 0.005)
    assert allow is True


def test_degenerate_with_perfect_looking_slope_still_blocked(monkeypatch):
    """slope=1.0, intercept=0.0 with calibration_valid=False must still be blocked.

    This is the exact scenario described in P0-1: degenerate calibration looks
    like "perfect" calibration but must not pass the promotion gate.
    """
    cand = _make_cand_metrics(
        calibration_valid=False,
        calibration_status="degenerate_single_class",
    )
    # slope and intercept look perfect but calibration was not measurable
    assert cand["calibration_slope"] == 1.0
    assert cand["calibration_intercept"] == 0.0

    allow, reason = _run_promotion_gate(cand, _make_champ_metrics(), monkeypatch)
    assert allow is False, "Must block even when slope=1.0/intercept=0.0 if calibration_valid=False"
    assert "PROMO_DENY_CALIBRATION_UNMEASURABLE" in reason
