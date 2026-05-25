"""Tests for train_v15_lgbm: stats helpers, CV split, gate evaluation."""
from __future__ import annotations

import random

import pytest

from tools.train_v15_lgbm import (
    Sample, norm_sid,
    auc, brier_score, lift_top_decile, expected_calibration_error,
    select_features, purged_walk_forward_splits,
    evaluate_gates,
    add_regime_features, blend_predictions, KNOWN_REGIMES, KNOWN_SYMBOLS,
)


def _mk(ts_ms, hit, features=None):
    return Sample(sid=f"sid-{ts_ms}", ts_ms=ts_ms, symbol="BTC", regime="na",
                  features=features or {"x": 0.0}, r=0.5 if hit else -0.5, hit=hit)


class TestNormSid:
    def test_of_long(self):
        assert norm_sid("of:BTCUSDT:1700000000000:L") == "BTCUSDT:1700000000000"

    def test_iceberg(self):
        assert norm_sid("iceberg:ETHUSDT:1700000000000") == "ETHUSDT:1700000000000"

    def test_invalid(self):
        assert norm_sid(None) is None
        assert norm_sid("garbage") is None


class TestAuc:
    def test_perfect_separation(self):
        # All positives have higher score than all negatives → AUC = 1.0
        y = [0, 0, 0, 1, 1, 1]
        p = [0.1, 0.2, 0.3, 0.7, 0.8, 0.9]
        assert auc(y, p) == 1.0

    def test_inverted(self):
        y = [0, 0, 0, 1, 1, 1]
        p = [0.9, 0.8, 0.7, 0.3, 0.2, 0.1]
        assert auc(y, p) == 0.0

    def test_random(self):
        # Random ⇒ ~0.5
        random.seed(42)
        y = [random.randint(0, 1) for _ in range(1000)]
        p = [random.random() for _ in range(1000)]
        a = auc(y, p)
        assert 0.45 < a < 0.55

    def test_empty_or_single_class(self):
        assert auc([1, 1, 1], [0.1, 0.5, 0.9]) == 0.5
        assert auc([0, 0, 0], [0.1, 0.5, 0.9]) == 0.5


class TestBrierAndECE:
    def test_brier_perfect(self):
        assert brier_score([1, 0, 1, 0], [1.0, 0.0, 1.0, 0.0]) == 0.0

    def test_brier_worst(self):
        assert brier_score([1, 0], [0.0, 1.0]) == 1.0

    def test_ece_perfect_calibration(self):
        # All predictions match outcome rates exactly
        y = [1] * 50 + [0] * 50
        p = [1.0] * 50 + [0.0] * 50
        assert expected_calibration_error(y, p) == 0.0

    def test_ece_overconfident(self):
        # Predict 0.9 but only 50% actually hit → overconfident
        y = [1] * 50 + [0] * 50
        p = [0.9] * 100
        ece = expected_calibration_error(y, p)
        assert ece > 0.3


class TestLift:
    def test_lift_top_decile_perfect(self):
        # Top 10 highest-prob are all positives → lift = (10/10) / (10/100) = 10x
        y = [0] * 90 + [1] * 10
        p = [0.01 * i for i in range(100)]
        l = lift_top_decile(y, p)
        assert l == 10.0

    def test_lift_no_signal(self):
        # Random ⇒ lift ≈ 1.0
        random.seed(7)
        y = [random.randint(0, 1) for _ in range(1000)]
        p = [random.random() for _ in range(1000)]
        l = lift_top_decile(y, p)
        assert 0.6 < l < 1.4


class TestSelectFeatures:
    def test_drop_low_coverage(self):
        samples = []
        for i in range(100):
            feats = {"common": float(i)}  # variant!
            if i < 10:
                feats["rare"] = float(i)  # < min_coverage
            samples.append(_mk(1000 + i, i % 2, feats))
        kept = select_features(samples, min_coverage=0.5)
        assert "common" in kept
        assert "rare" not in kept

    def test_drop_constants(self):
        samples = [_mk(1000 + i, i % 2, {"constant": 1.0, "varying": float(i)})
                   for i in range(20)]
        kept = select_features(samples, min_coverage=0.5)
        assert "constant" not in kept
        assert "varying" in kept

    def test_empty(self):
        assert select_features([]) == []


class TestPurgedWalkForward:
    def test_n_folds(self):
        samples = [_mk(1000 + i, 0) for i in range(1000)]
        splits = purged_walk_forward_splits(samples, n_folds=5, embargo_pct=0.01)
        assert 4 <= len(splits) <= 5
        for sp in splits:
            # No overlap between train and test
            train_set = set(sp.train_idx)
            test_set = set(sp.test_idx)
            assert not (train_set & test_set)

    def test_embargo_gap(self):
        samples = [_mk(1000 + i, 0) for i in range(1000)]
        splits = purged_walk_forward_splits(samples, n_folds=5, embargo_pct=0.02)
        for sp in splits:
            max_train = max(sp.train_idx) if sp.train_idx else -1
            min_test = min(sp.test_idx) if sp.test_idx else 999999
            # at least 1 sample gap (embargo_n = max(1, ...))
            assert min_test - max_train >= 1


class TestGates:
    def _result(self, **overrides):
        base = {
            "n_total": 5000,
            "n_positive": 500,
            "feature_cols": [f"f_{i}" for i in range(50)],
            "fold_metrics": [{"train_auc": 0.7, "test_auc": 0.65, "gap": 0.05}],
            "oof_metrics_raw": {"auc": 0.65, "brier": 0.18, "ece": 0.05,
                                "lift_top_decile": 2.0},
            "oof_metrics_calibrated": {"brier": 0.15, "ece": 0.03},
        }
        base.update(overrides)
        return base

    def test_all_pass(self):
        gates, all_ok = evaluate_gates(self._result())
        failed = [g for g in gates if not g["ok"]]
        assert all_ok is True, f"unexpected failures: {failed}"

    def test_low_positives_fails(self):
        gates, all_ok = evaluate_gates(self._result(n_positive=50))
        assert any(g["name"] == "min_positives" and not g["ok"] for g in gates)
        assert all_ok is False

    def test_overfit_gap_fails(self):
        gates, all_ok = evaluate_gates(
            self._result(fold_metrics=[{"train_auc": 0.95, "test_auc": 0.55, "gap": 0.40}])
        )
        assert any(g["name"] == "train_test_gap_max" and not g["ok"] for g in gates)
        assert all_ok is False

    def test_auc_below_threshold_fails(self):
        gates, all_ok = evaluate_gates(
            self._result(oof_metrics_raw={"auc": 0.50, "brier": 0.18, "ece": 0.05, "lift_top_decile": 2.0})
        )
        assert any(g["name"] == "oof_auc_min" and not g["ok"] for g in gates)


# ─── Regime feature injection ─────────────────────────────────────────────────


class TestAddRegimeFeatures:
    def test_trending_bull_one_hot(self):
        s = _mk(1000, 0, {"x": 1.0})
        s.regime = "trending_bull"
        s.symbol = "BTCUSDT"
        add_regime_features([s])
        assert s.features["_regime_trending_bull"] == 1.0
        assert s.features["_regime_range"] == 0.0
        assert s.features["_regime_other"] == 0.0
        assert s.features["_symbol_BTCUSDT"] == 1.0

    def test_unknown_regime_routes_to_other(self):
        s = _mk(1000, 0, {"x": 1.0})
        s.regime = "weird_new_regime"
        s.symbol = "FOOUSDT"
        add_regime_features([s])
        assert all(s.features[f"_regime_{k}"] == 0.0 for k in KNOWN_REGIMES)
        assert s.features["_regime_other"] == 1.0
        assert s.features["_symbol_other"] == 1.0

    def test_na_regime_routes_to_other(self):
        s = _mk(1000, 0, {})
        s.regime = "na"
        add_regime_features([s])
        # na ∉ KNOWN_REGIMES → all known one-hots = 0, other = 1
        assert s.features["_regime_other"] == 1.0

    def test_case_insensitive(self):
        s = _mk(1000, 0, {})
        s.regime = "Trending_Bull"
        add_regime_features([s])
        assert s.features["_regime_trending_bull"] == 1.0

    def test_all_known_regimes_have_feature(self):
        for rg in KNOWN_REGIMES:
            s = _mk(1000, 0, {})
            s.regime = rg
            add_regime_features([s])
            assert s.features[f"_regime_{rg}"] == 1.0


# ─── Blend predictions ────────────────────────────────────────────────────────


class _StubRegimeModel:
    def __init__(self, prob: float):
        self._prob = prob
    def predict_proba(self, X):
        import numpy as np
        n = len(X)
        return np.array([[1 - self._prob, self._prob]] * n)


class _IdentityCalibrator:
    def predict(self, xs):
        return list(xs)


class TestBlendPredictions:
    def test_no_regime_model_falls_back_to_global(self):
        blended, comps = blend_predictions(0.42, "trending_bull", per_regime={}, X_row=[0.1, 0.2])
        assert blended == 0.42
        assert comps["weight_global"] == 1.0
        assert comps["regime_used"] == -1.0

    def test_weak_regime_model_low_weight(self):
        per_regime = {"trending_bull": {
            "model": _StubRegimeModel(0.80), "calibrator": _IdentityCalibrator(),
            "oof_auc": 0.52, "n": 50,
        }}
        blended, comps = blend_predictions(0.30, "trending_bull", per_regime, X_row=[0.1])
        # quality = (0.52 - 0.50) * 2 = 0.04; sample = 50/200 = 0.25
        # w_regime = 0.04 * 0.25 = 0.01
        assert comps["weight_regime"] < 0.05
        assert 0.29 < blended < 0.35  # mostly global (0.30) with tiny push toward 0.80

    def test_strong_regime_model_pulls_blend(self):
        # AUC=0.75 → quality = (0.75-0.5)*2 = 0.5 (conservative formula)
        # n=500 → sample_factor = min(1, 500/200) = 1.0
        # w_regime = 0.5 * 1.0 = 0.5
        per_regime = {"range": {
            "model": _StubRegimeModel(0.90), "calibrator": _IdentityCalibrator(),
            "oof_auc": 0.75, "n": 500,
        }}
        blended, comps = blend_predictions(0.20, "range", per_regime, X_row=[0.1])
        assert abs(comps["weight_regime"] - 0.5) < 1e-9
        assert abs(comps["weight_global"] - 0.5) < 1e-9
        # blend = 0.5*0.20 + 0.5*0.90 = 0.55
        assert abs(blended - 0.55) < 1e-9

    def test_max_auc_regime_dominates(self):
        # AUC=1.0 → quality = 1.0; n>=200 → sample=1.0; w_regime=1.0
        per_regime = {"squeeze": {
            "model": _StubRegimeModel(0.95), "calibrator": _IdentityCalibrator(),
            "oof_auc": 1.00, "n": 500,
        }}
        blended, comps = blend_predictions(0.20, "squeeze", per_regime, X_row=[0.1])
        assert comps["weight_regime"] == 1.0
        assert blended == 0.95

    def test_unknown_regime_falls_back(self):
        per_regime = {"range": {
            "model": _StubRegimeModel(0.99), "calibrator": _IdentityCalibrator(),
            "oof_auc": 0.99, "n": 9999,
        }}
        blended, comps = blend_predictions(0.40, "expansion", per_regime, X_row=[0.1])
        # regime "expansion" not in per_regime → global only
        assert blended == 0.40
        assert comps["weight_global"] == 1.0


# ─── Smoke check: KNOWN_SYMBOLS pinned ────────────────────────────────────────


def test_known_symbols_includes_top_volume_pairs():
    assert "BTCUSDT" in KNOWN_SYMBOLS
    assert "ETHUSDT" in KNOWN_SYMBOLS
    # Sanity: no duplicates
    assert len(set(KNOWN_SYMBOLS)) == len(KNOWN_SYMBOLS)
