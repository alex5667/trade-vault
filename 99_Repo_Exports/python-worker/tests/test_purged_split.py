import numpy as np

from core.purged_embargo_split import PurgedEmbargoTimeSeriesSplit


def test_purged_embargo_split_basic():
    ts = np.arange(0, 1_000_000, 1000, dtype=np.int64)
    s = PurgedEmbargoTimeSeriesSplit(n_splits=3, purge_ms=2000, embargo_ms=1000)
    folds = list(s.split(ts))
    assert len(folds) > 0
    for tr, te in folds:
        assert len(tr) > 0 and len(te) > 0
        assert set(tr).isdisjoint(set(te))

