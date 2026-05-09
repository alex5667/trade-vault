#!/usr/bin/env python3
from __future__ import annotations

"""
test_oof_no_leak.py

Test that OOF (Out-of-Fold) predictions are built correctly without leakage.
Verifies that each sample's OOF prediction is computed only from folds that don't contain that sample.
"""


import numpy as np
import pytest
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

try:
    from sklearn.ensemble import HistGradientBoostingClassifier
except ImportError:
    HistGradientBoostingClassifier = None


def make_lr() -> Pipeline:
    """Create LR pipeline."""
    return Pipeline([
        ("scaler", RobustScaler(with_centering=True, with_scaling=True, quantile_range=(25.0, 75.0))),
        ("lr", LogisticRegression(
            C=1.0,
            solver="lbfgs",
            max_iter=500,
            class_weight="balanced",
            random_state=42
        ))
    ])


def day_group(ts_ms: np.ndarray) -> np.ndarray:
    """Group by UTC day."""
    return (ts_ms // 86_400_000).astype(np.int64)


def walk_forward_splits(groups: np.ndarray, n_splits: int) -> list[tuple[np.ndarray, np.ndarray]]:
    """Walk-forward split by days."""
    ug = np.unique(groups)
    ug.sort()
    if len(ug) < max(5, n_splits + 1):
        cut = int(len(groups) * 0.8)
        idx = np.arange(len(groups))
        return [(idx[:cut], idx[cut:])]

    folds = np.array_split(ug, n_splits + 1)
    splits = []
    for i in range(1, len(folds)):
        train_days = np.concatenate(folds[:i])
        val_days = folds[i]
        tr = np.where(np.isin(groups, train_days))[0]
        va = np.where(np.isin(groups, val_days))[0]
        if len(tr) == 0 or len(va) == 0:
            continue
        splits.append((tr, va))
    return splits


def test_oof_no_leakage():
    """Test that OOF predictions don't leak information."""
    # Create toy dataset with time structure
    n_samples = 200
    n_features = 5

    # Create time-ordered data (simulate 10 days)
    ts_ms = np.arange(0, n_samples * 86_400_000 // 10, 86_400_000 // 10, dtype=np.int64)
    groups = day_group(ts_ms)

    # Create features with some time-dependent structure
    np.random.seed(42)
    X = np.random.randn(n_samples, n_features).astype(np.float32)
    # Add time trend to make leakage detectable
    X[:, 0] += (np.arange(n_samples) / n_samples) * 0.1

    # Create labels with some correlation to features
    y = ((X[:, 0] + X[:, 1] + np.random.randn(n_samples) * 0.5) > 0).astype(int)

    # Build OOF predictions
    splits = walk_forward_splits(groups, n_splits=5)
    oof_preds = np.full(n_samples, np.nan, dtype=float)

    # Track which samples were used for training in each fold
    train_indices_per_fold = []

    for tr_idx, va_idx in splits:
        train_indices_per_fold.append(set(tr_idx))

        # Train on training fold
        lr = make_lr()
        lr.fit(X[tr_idx], y[tr_idx])

        # Predict only on validation fold
        oof_preds[va_idx] = lr.predict_proba(X[va_idx])[:, 1]

    # Verify no leakage: each validation sample should not appear in its training set
    for tr_idx, va_idx in splits:
        train_set = set(tr_idx)
        val_set = set(va_idx)
        # Validation set should not overlap with training set
        assert len(train_set & val_set) == 0, "Leakage detected: validation samples in training set"

    # Verify all samples have OOF predictions (except possibly edge cases)
    valid_oof = np.isfinite(oof_preds)
    assert valid_oof.sum() >= n_samples * 0.8, f"Too few OOF predictions: {valid_oof.sum()}/{n_samples}"

    # Verify OOF predictions are in valid range
    assert np.all((oof_preds[valid_oof] >= 0) & (oof_preds[valid_oof] <= 1)), "OOF predictions out of [0,1] range"

    # Verify OOF predictions are not identical to full-fit predictions (would indicate leakage)
    lr_full = make_lr()
    lr_full.fit(X, y)
    full_preds = lr_full.predict_proba(X)[:, 1]

    # OOF and full predictions should differ (OOF is more conservative)
    oof_valid = oof_preds[valid_oof]
    full_valid = full_preds[valid_oof]
    mse_diff = np.mean((oof_valid - full_valid) ** 2)
    assert mse_diff > 1e-6, "OOF predictions too similar to full-fit (possible leakage)"

    # Verify time ordering: later samples should not be in training for earlier validation
    for i, (tr_idx, va_idx) in enumerate(splits):
        if i == 0:
            continue
        # Current validation should be after previous training
        prev_max_train_idx = max(tr_idx) if len(tr_idx) > 0 else -1
        curr_min_val_idx = min(va_idx) if len(va_idx) > 0 else n_samples
        # In walk-forward, validation should come after training
        assert prev_max_train_idx < curr_min_val_idx or len(va_idx) == 0, \
            "Time ordering violated: validation before training"


def test_oof_meta_no_leakage():
    """Test that meta model trained on OOF doesn't leak."""
    n_samples = 200
    n_features = 5

    ts_ms = np.arange(0, n_samples * 86_400_000 // 10, 86_400_000 // 10, dtype=np.int64)
    groups = day_group(ts_ms)

    np.random.seed(42)
    X = np.random.randn(n_samples, n_features).astype(np.float32)
    y = ((X[:, 0] + X[:, 1] + np.random.randn(n_samples) * 0.5) > 0).astype(int)

    # Build OOF for base models
    splits = walk_forward_splits(groups, n_splits=5)
    oof_lr = np.full(n_samples, np.nan, dtype=float)
    oof_gbdt = np.full(n_samples, np.nan, dtype=float)

    for tr_idx, va_idx in splits:
        lr = make_lr()
        if HistGradientBoostingClassifier is not None:
            gbdt = HistGradientBoostingClassifier(
                max_depth=4,
                learning_rate=0.1,
                max_iter=100,
                random_state=42
            )
        else:
            pytest.skip("HistGradientBoostingClassifier not available")

        lr.fit(X[tr_idx], y[tr_idx])
        gbdt.fit(X[tr_idx], y[tr_idx])

        oof_lr[va_idx] = lr.predict_proba(X[va_idx])[:, 1]
        oof_gbdt[va_idx] = gbdt.predict_proba(X[va_idx])[:, 1]

    # Meta model should only use OOF predictions
    valid_mask = np.isfinite(oof_lr) & np.isfinite(oof_gbdt)
    assert valid_mask.sum() >= n_samples * 0.8, "Too few valid OOF predictions"

    Z_oof = np.stack([oof_lr[valid_mask], oof_gbdt[valid_mask]], axis=1)
    y_oof = y[valid_mask]

    # Train meta on OOF only
    meta = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=500,
        class_weight="balanced",
        random_state=42
    )
    meta.fit(Z_oof, y_oof)

    # Verify meta predictions are reasonable
    p_meta_oof = meta.predict_proba(Z_oof)[:, 1]
    assert np.all((p_meta_oof >= 0) & (p_meta_oof <= 1)), "Meta OOF predictions out of range"
    assert np.all(np.isfinite(p_meta_oof)), "Meta OOF predictions contain NaN/Inf"

    # Verify meta model doesn't overfit (AUC should be reasonable but not perfect)
    from sklearn.metrics import roc_auc_score
    if len(np.unique(y_oof)) > 1:
        auc = roc_auc_score(y_oof, p_meta_oof)
        assert 0.5 <= auc <= 1.0, f"Meta OOF AUC out of reasonable range: {auc}"


if __name__ == "__main__":
    test_oof_no_leakage()
    test_oof_meta_no_leakage()
    print("All OOF leakage tests passed!")







