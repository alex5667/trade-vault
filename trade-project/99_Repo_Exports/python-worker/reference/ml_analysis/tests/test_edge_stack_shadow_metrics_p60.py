
import numpy as np

from ml_analysis.tools.edge_stack_shadow_metrics_p60 import (
    calculate_brier_score,
    calculate_ece,
    calculate_expectancy_top_k_pct,
    calculate_precision_top_k_pct,
    check_promotion_guard,
)


def test_brier_score():
    y_true = np.array([1, 0, 1, 0])
    y_prob = np.array([0.9, 0.1, 0.8, 0.2])
    # Errors: (0.1^2, 0.1^2, 0.2^2, 0.2^2) = (0.01, 0.01, 0.04, 0.04)
    # Mean = 0.1 / 4 = 0.025
    bs = calculate_brier_score(y_true, y_prob)
    assert np.isclose(bs, 0.025)

def test_ece_perfect():
    y_true = np.array([0, 1, 0, 1])
    y_prob = np.array([0.1, 0.9, 0.2, 0.8])
    # Bins: 0-0.1, ..., 0.8-0.9
    # Bin 0.9: 1 item (1.0), true=1. Conf=0.9. Diff=0.1
    # Bin 0.1: 1 item (0.0), true=0. Conf=0.1. Diff=0.1
    # ...
    # Simple check: should be small
    ece = calculate_ece(y_true, y_prob, n_bins=5)
    assert ece < 0.2

def test_precision_top_k():
    y_true = np.array([1, 0, 1, 0, 1, 1, 0, 0, 1, 0]) # 10 items
    y_prob = np.array([0.9, 0.1, 0.85, 0.2, 0.8, 0.75, 0.3, 0.4, 0.7, 0.5])
    # Top 50% (k=0.5, n=5)
    # Sorted probs: 0.9 (1), 0.85 (1), 0.8 (1), 0.75 (1), 0.7 (1)
    # All top 5 are 1. Precision = 1.0
    prec = calculate_precision_top_k_pct(y_true, y_prob, k_pct=0.5)
    assert prec == 1.0

    # Top 10% (n=1): 0.9 (1) -> 1.0
    prec = calculate_precision_top_k_pct(y_true, y_prob, k_pct=0.1)
    assert prec == 1.0

def test_expectancy_top_k():
    y_r = np.array([2.0, -1.0, 1.5, -0.5])
    y_prob = np.array([0.9, 0.2, 0.8, 0.3])
    # Top 50% (n=2): 0.9 (2.0), 0.8 (1.5)
    # Mean = 1.75
    exp = calculate_expectancy_top_k_pct(y_r, y_prob, k_pct=0.5)
    assert exp == 1.75

def test_promotion_guard_success():
    champ = {"brier": 0.10, "ece": 0.05, "precision_top5pct": 0.6}
    cand = {"brier": 0.09, "ece": 0.04, "precision_top5pct": 0.65}

    promote, reasons = check_promotion_guard(champ, cand)
    assert promote
    assert len(reasons) == 0

def test_promotion_guard_failure_brier():
    champ = {"brier": 0.10, "ece": 0.05, "precision_top5pct": 0.6}
    cand = {"brier": 0.15, "ece": 0.05, "precision_top5pct": 0.6}
    # 0.15 / 0.10 = 1.5 > 1.02

    promote, reasons = check_promotion_guard(champ, cand)
    assert not promote
    assert "brier_rel 1.5000 > 1.02" in reasons[0]

def test_promotion_guard_failure_precision():
    champ = {"brier": 0.10, "ece": 0.05, "precision_top5pct": 0.7}
    cand = {"brier": 0.10, "ece": 0.05, "precision_top5pct": 0.6}
    # Delta = -0.1 < 0.0

    promote, reasons = check_promotion_guard(champ, cand)
    assert not promote
    assert "prec_delta -0.1000 < 0.0" in reasons[0]
