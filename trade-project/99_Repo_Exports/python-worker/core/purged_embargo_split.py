from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class PurgedEmbargoTimeSeriesSplit:
    """Time-series CV with purge and embargo.

    Split is sequential. Purge removes from train any samples within `purge_ms` of test interval.
    Embargo removes samples immediately after test interval for `embargo_ms`.

    ts_ms must be sorted ascending.
    """

    n_splits: int = 5
    purge_ms: int = 180_000
    embargo_ms: int = 60_000

    def split(self, ts_ms: np.ndarray) -> Iterator[tuple[np.ndarray, np.ndarray]]:
        n = len(ts_ms)
        if n < self.n_splits + 2:
            return
        test_size = max(1, n // (self.n_splits + 1))
        for k in range(1, self.n_splits + 1):
            test_start = k * test_size
            test_end = min(n, (k + 1) * test_size)
            if test_end <= test_start:
                continue

            test_idx = np.arange(test_start, test_end, dtype=int)
            t0 = int(ts_ms[test_start])
            t1 = int(ts_ms[test_end - 1])

            purge_lo = t0 - int(self.purge_ms)
            purge_hi = t1 + int(self.purge_ms)
            embargo_hi = t1 + int(self.embargo_ms)

            train_mask = np.ones(n, dtype=bool)
            train_mask[test_idx] = False
            train_mask &= ~((ts_ms >= purge_lo) & (ts_ms <= purge_hi))
            train_mask &= ~((ts_ms > t1) & (ts_ms <= embargo_hi))

            train_idx = np.where(train_mask)[0]
            if len(train_idx) == 0:
                continue
            yield train_idx, test_idx

