from __future__ import annotations

"""Tests for purged K-Fold CV for event-based labels."""


import numpy as np

from ml_core.purged_cv import PurgedFold, purged_kfold_time_series


def test_purged_kfold_basic():
    """Test basic purged k-fold functionality."""
    n = 100
    ts_ms = np.arange(1000, 1000 + n * 100, 100, dtype=np.int64)
    t1_ms = ts_ms + 5000  # 5s intervals

    folds = purged_kfold_time_series(
        ts_ms=ts_ms,
        t1_ms=t1_ms,
        n_splits=5,
        embargo_ms=0,
    )

    assert len(folds) == 5
    for fold in folds:
        assert isinstance(fold, PurgedFold)
        assert len(fold.train_idx) > 0
        assert len(fold.test_idx) > 0
        # No overlap
        assert len(np.intersect1d(fold.train_idx, fold.test_idx)) == 0


def test_purged_kfold_embargo():
    """Test embargo exclusion."""
    n = 50
    ts_ms = np.arange(1000, 1000 + n * 100, 100, dtype=np.int64)
    t1_ms = ts_ms + 5000

    folds = purged_kfold_time_series(
        ts_ms=ts_ms,
        t1_ms=t1_ms,
        n_splits=3,
        embargo_ms=1000,  # 1s embargo
    )

    assert len(folds) == 3
    for fold in folds:
        # Train should not overlap with test + embargo
        train_ts = ts_ms[fold.train_idx]
        train_t1 = t1_ms[fold.train_idx]
        test_ts = ts_ms[fold.test_idx]
        test_t1 = t1_ms[fold.test_idx]

        test_min = test_ts.min() - 1000
        test_max = test_ts.max() + 1000  # use observation end, not label end

        # No train interval should overlap with test+embargo
        for i, (ts, t1) in enumerate(zip(train_ts, train_t1)):
            assert not (t1 >= test_min and ts <= test_max), f"Train sample {i} overlaps test+embargo"


def test_purged_kfold_empty():
    """Test empty input."""
    folds = purged_kfold_time_series(
        ts_ms=np.array([], dtype=np.int64),
        t1_ms=np.array([], dtype=np.int64),
        n_splits=5,
    )
    assert len(folds) == 0


def test_purged_kfold_sorted():
    """Test that folds respect time ordering."""
    n = 100
    # Unsorted timestamps
    ts_ms = np.random.randint(1000, 10000, n)
    t1_ms = ts_ms + 5000

    folds = purged_kfold_time_series(
        ts_ms=ts_ms,
        t1_ms=t1_ms,
        n_splits=5,
        embargo_ms=0,
    )

    # Test sets should be time-ordered
    for fold in folds:
        test_ts = ts_ms[fold.test_idx]
        assert np.all(test_ts[:-1] <= test_ts[1:]), "Test set should be time-ordered"

