import sys
from pathlib import Path

import pytest

# Ensure repo root is on sys.path even under pytest --import-mode=importlib
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_purged_embargo_time_series_split_walkforward():
    try:
        from ml_analysis.tools.train_edge_stack_v1_oof import PurgedEmbargoTimeSeriesSplit
    except (ImportError, SystemExit) as e:
        pytest.skip(f"split class not importable (missing dependencies): {e}", allow_module_level=True)

    ts = [i * 1000 for i in range(100)]  # 0..99000 ms
    splitter = PurgedEmbargoTimeSeriesSplit(n_splits=5, purge_ms=2000, embargo_ms=1000, min_train=10)

    splits = list(splitter.split(ts))
    assert len(splits) == 5

    for train_idx, val_idx in splits:
        assert train_idx
        assert val_idx
        train_ts = [ts[i] for i in train_idx]
        val_ts = [ts[i] for i in val_idx]
        # Walk-forward: all train before validation (with purge)
        assert max(train_ts) <= min(val_ts) - 2000
        # disjoint
        assert set(train_idx).isdisjoint(set(val_idx))


def test_split_respects_min_train():
    try:
        from ml_analysis.tools.train_edge_stack_v1_oof import PurgedEmbargoTimeSeriesSplit
    except (ImportError, SystemExit) as e:
        pytest.skip(f"split class not importable (missing dependencies): {e}", allow_module_level=True)

    ts = [i * 1000 for i in range(30)]
    splitter = PurgedEmbargoTimeSeriesSplit(n_splits=5, purge_ms=0, embargo_ms=0, min_train=25)

    # Some folds should be dropped because min_train too large
    splits = list(splitter.split(ts))
    assert 1 <= len(splits) < 5
