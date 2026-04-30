from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple

import numpy as np


@dataclass(frozen=True)
class PurgedFold:
    train_idx: np.ndarray
    test_idx: np.ndarray


def purged_kfold_time_series(
    *
    ts_ms: np.ndarray
    t1_ms: np.ndarray
    n_splits: int = 5
    embargo_ms: int = 0
) -> List[PurgedFold]:
    """Purged K-Fold for event-based labels (Lopez de Prado).

    Each sample i has an information interval [ts_ms[i], t1_ms[i]].
    When evaluating fold with test interval [T0, T1], we remove (purge) from train
    any sample whose interval overlaps test interval, plus optional embargo.

    Overlap condition:
      (t1_ms >= T0 - embargo) AND (ts_ms <= T1 + embargo)
    """
    ts_ms = np.asarray(ts_ms, dtype=np.int64)
    t1_ms = np.asarray(t1_ms, dtype=np.int64)
    n = len(ts_ms)
    if n == 0:
        return []

    # order by ts
    order = np.argsort(ts_ms)
    ts = ts_ms[order]
    t1 = t1_ms[order]

    # contiguous folds by index (time-respecting)
    idx_folds = np.array_split(np.arange(n, dtype=np.int64), int(n_splits))
    out: List[PurgedFold] = []
    for test_idx0 in idx_folds:
        if len(test_idx0) == 0:
            continue
        t0 = int(ts[test_idx0[0]])
        t1_fold = int(ts[test_idx0[-1]])
        # embargo expands the exclusion zone
        t0e = t0 - int(embargo_ms)
        t1e = t1_fold + int(embargo_ms)

        # purge overlapping intervals
        overlap = (t1 >= t0e) & (ts <= t1e)
        train_idx0 = np.where(~overlap)[0]

        # map back to original indices
        train_idx = order[train_idx0]
        test_idx = order[test_idx0]
        out.append(PurgedFold(train_idx=train_idx, test_idx=test_idx))
    return out

