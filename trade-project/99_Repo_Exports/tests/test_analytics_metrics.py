"""
Unit tests for analytics.metrics

Tests:
- calculate_roc_auc: empty, single-class, known distribution
- find_best_threshold
- calculate_precision_recall edge cases
- calculate_confusion_matrix
- bootstrap_ci from ab_compare

Run:
    pytest tests/test_analytics_metrics.py -v
"""
from __future__ import annotations
import sys
import os
import math
import random
import pytest
import numpy as np

# ---------------------------------------------------------------------------
# Path setup — the analytics package lives one level up (scanner_infra root)
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from analytics.metrics import (
    calculate_roc_auc,
    calculate_confusion_matrix,
    calculate_precision_recall,
    calculate_youden_index,
    find_best_threshold,
    roc_from_signals,
    ROCResult,
)


# ---------------------------------------------------------------------------
# calculate_roc_auc
# ---------------------------------------------------------------------------
class TestCalculateRocAuc:
    def test_empty_input(self):
        result = calculate_roc_auc([], [])
        assert result.auc == 0.0
        assert result.fpr == []
        assert result.tpr == []

    def test_all_positive_labels(self):
        """Single-class → AUC=0.5 sentinel"""
        result = calculate_roc_auc([0.9, 0.8, 0.7], [1, 1, 1])
        assert result.auc == pytest.approx(0.5, abs=1e-6)

    def test_all_negative_labels(self):
        result = calculate_roc_auc([0.1, 0.2, 0.3], [0, 0, 0])
        assert result.auc == pytest.approx(0.5, abs=1e-6)

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="длины|должны"):
            calculate_roc_auc([0.5, 0.6], [1])

    def test_perfect_classifier(self):
        """Perfect scores: AUC should be 1.0"""
        scores = [1.0, 0.9, 0.8, 0.1, 0.05]
        labels = [1,   1,   1,   0,   0  ]
        result = calculate_roc_auc(scores, labels)
        assert result.auc == pytest.approx(1.0, abs=0.01)

    def test_random_classifier_near_half(self):
        """Shuffled labels → AUC ≈ 0.5"""
        rng = random.Random(42)
        scores = [rng.random() for _ in range(500)]
        labels = [rng.randint(0, 1) for _ in range(500)]
        result = calculate_roc_auc(scores, labels)
        assert 0.35 < result.auc < 0.65

    def test_result_structure(self):
        scores = [0.9, 0.7, 0.4, 0.2]
        labels = [1,   0,   1,   0  ]
        result = calculate_roc_auc(scores, labels)
        assert isinstance(result, ROCResult)
        assert len(result.fpr) == len(result.tpr) == len(result.thresholds)
        assert 0.0 <= result.auc <= 1.0

    def test_large_n_performance(self):
        """5 000 samples should complete well under 200 ms"""
        import time
        rng = random.Random(7)
        n = 5000
        scores = [rng.random() for _ in range(n)]
        labels = [rng.randint(0, 1) for _ in range(n)]
        t0 = time.perf_counter()
        result = calculate_roc_auc(scores, labels)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        assert elapsed_ms < 200, f"ROC for N={n} took {elapsed_ms:.1f} ms (> 200 ms)"
        assert 0.0 <= result.auc <= 1.0

    def test_auc_to_dict(self):
        r = calculate_roc_auc([0.9, 0.1], [1, 0])
        d = r.to_dict()
        assert "fpr" in d and "tpr" in d and "thresholds" in d and "auc" in d


# ---------------------------------------------------------------------------
# find_best_threshold
# ---------------------------------------------------------------------------
class TestFindBestThreshold:
    def _make_roc(self) -> ROCResult:
        """Simple hand-crafted ROC with known best Youden J at threshold index 1."""
        return ROCResult(
            fpr=[0.0, 0.1, 0.5, 1.0],
            tpr=[0.0, 0.8, 0.9, 1.0],
            thresholds=[1.0, 0.7, 0.4, 0.0],
            auc=0.75,
        )

    def test_youden_selects_max_j(self):
        roc = self._make_roc()
        thr, metrics = find_best_threshold(roc, method="youden")
        # Youden J at each point: 0, 0.7, 0.4, 0
        assert thr == pytest.approx(0.7)
        assert metrics["youden_j"] == pytest.approx(0.7)

    def test_balanced_method(self):
        roc = self._make_roc()
        thr, metrics = find_best_threshold(roc, method="balanced")
        assert isinstance(thr, float)
        assert 0.0 <= thr <= 1.0

    def test_f1_method(self):
        roc = self._make_roc()
        thr, _ = find_best_threshold(roc, method="f1")
        assert isinstance(thr, float)


# ---------------------------------------------------------------------------
# calculate_precision_recall
# ---------------------------------------------------------------------------
class TestCalculatePrecisionRecall:
    def test_zero_division_tp_fp(self):
        # tp=0, fp=0 → precision=0
        prec, rec, f1 = calculate_precision_recall(0, 0, 5)
        assert prec == 0.0
        assert rec == 0.0
        assert f1 == 0.0

    def test_zero_division_tp_fn(self):
        prec, rec, f1 = calculate_precision_recall(0, 3, 0)
        assert rec == 0.0

    def test_perfect(self):
        prec, rec, f1 = calculate_precision_recall(10, 0, 0)
        assert prec == 1.0
        assert rec == 1.0
        assert f1 == pytest.approx(1.0)

    def test_standard_case(self):
        # tp=7, fp=3, fn=2: precision=7/10=0.7, recall=7/9≈0.778
        prec, rec, f1 = calculate_precision_recall(7, 3, 2)
        assert prec == pytest.approx(0.7)
        assert rec == pytest.approx(7 / 9)
        assert f1 == pytest.approx(2 * 0.7 * (7 / 9) / (0.7 + 7 / 9))


# ---------------------------------------------------------------------------
# calculate_confusion_matrix
# ---------------------------------------------------------------------------
class TestCalculateConfusionMatrix:
    def test_threshold_zero_all_positive(self):
        scores = [0.9, 0.5, 0.1]
        labels = [1, 0, 1]
        tp, fp, tn, fn = calculate_confusion_matrix(scores, labels, threshold=0.0)
        # All predicted positive
        assert tp == 2  # actual pos
        assert fp == 1  # actual neg predicted pos
        assert tn == 0
        assert fn == 0

    def test_threshold_one_all_negative(self):
        scores = [0.9, 0.5, 0.1]
        labels = [1, 0, 1]
        tp, fp, tn, fn = calculate_confusion_matrix(scores, labels, threshold=1.0)
        assert tp == 0
        assert fp == 0
        assert tn == 1
        assert fn == 2

    def test_mid_threshold(self):
        scores = [0.8, 0.6, 0.4, 0.2]
        labels = [1,   1,   0,   0  ]
        tp, fp, tn, fn = calculate_confusion_matrix(scores, labels, threshold=0.5)
        assert tp == 2
        assert fp == 0
        assert tn == 2
        assert fn == 0


# ---------------------------------------------------------------------------
# calculate_youden_index
# ---------------------------------------------------------------------------
def test_youden_index():
    assert calculate_youden_index(0.8, 0.2) == pytest.approx(0.6)
    assert calculate_youden_index(0.5, 0.5) == pytest.approx(0.0)
    assert calculate_youden_index(1.0, 0.0) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# bootstrap_ci from ab_compare (vectorised path)
# ---------------------------------------------------------------------------
class TestBootstrapCI:
    """
    Test the vectorised bootstrap logic directly — avoids importing ab_compare
    at module level (which pulls in repository → services.trade_closed_hydrator,
    unavailable outside the Docker container).
    """

    @staticmethod
    def _winrate(data) -> float:
        if not len(data):
            return 0.0
        return float((np.asarray(data) >= 0).mean())

    @staticmethod
    def _avg(data) -> float:
        return float(np.mean(data)) if len(data) else 0.0

    def _bootstrap_ci(self, values, stat_fn, n_boot=500):
        """Inline vectorised bootstrap matching the ab_compare implementation."""
        if not values:
            return (0.0, 0.0, 0.0)
        vals = np.asarray(values, dtype=np.float64)
        n = len(vals)
        idx = np.random.randint(0, n, size=(n_boot, n))
        samples = vals[idx]
        if stat_fn is self._winrate:
            boot_stats = (samples >= 0).mean(axis=1)
        elif stat_fn is self._avg:
            boot_stats = samples.mean(axis=1)
        else:
            boot_stats = np.array([stat_fn(samples[i].tolist()) for i in range(n_boot)])
        lo, hi = float(np.quantile(boot_stats, 0.025)), float(np.quantile(boot_stats, 0.975))
        return (stat_fn(values), lo, hi)

    def test_empty_returns_zeros(self):
        result = self._bootstrap_ci([], self._avg)
        assert result == (0.0, 0.0, 0.0)

    def test_returns_3_tuple(self):
        est, lo, hi = self._bootstrap_ci([1.0, -1.0, 2.0, -0.5], self._avg, n_boot=200)
        assert isinstance(est, float)
        assert lo <= est <= hi

    def test_ci_contains_true_mean(self):
        np.random.seed(0)
        data = list(np.random.normal(loc=5.0, scale=1.0, size=100))
        est, lo, hi = self._bootstrap_ci(data, self._avg, n_boot=1000)
        assert lo <= 5.0 <= hi, f"True mean 5.0 not in 95% CI [{lo:.2f}, {hi:.2f}]"

    def test_winrate_ci(self):
        # 80% wins → CI should be around 0.8
        data = [1.0] * 80 + [-1.0] * 20
        est, lo, hi = self._bootstrap_ci(data, self._winrate, n_boot=500)
        assert lo >= 0.65
        assert hi <= 0.95
