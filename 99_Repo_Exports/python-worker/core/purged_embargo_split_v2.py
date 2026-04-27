from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Tuple
import numpy as np


@dataclass(frozen=True)
class PurgedEmbargoTimeSeriesSplitV2:
    """Sequential time split with purge and embargo (ms-based).

    ts_ms must be sorted ascending.
    - Test windows are contiguous partitions.
    - Train excludes:
        * test interval itself
        * purge_ms around [test_start, test_end]
        * embargo_ms after test_end
    """

    n_splits: int = 5
    purge_ms: int = 180_000
    embargo_ms: int = 60_000

    def split(self, ts_ms: np.ndarray) -> Iterator[Tuple[np.ndarray, np.ndarray]]:
        n = len(ts_ms)
        if n < self.n_splits + 2:
            return
        test_size = max(1, n // (self.n_splits + 1))
        for k in range(1, self.n_splits + 1):
            te0 = k * test_size
            te1 = min(n, (k + 1) * test_size)
            if te1 <= te0:
                continue

            test_idx = np.arange(te0, te1, dtype=int)
            t0 = int(ts_ms[te0])
            t1 = int(ts_ms[te1 - 1])

            purge_lo = t0 - int(self.purge_ms)
            purge_hi = t1 + int(self.purge_ms)
            embargo_hi = t1 + int(self.embargo_ms)

            mask = np.ones(n, dtype=bool)
            mask[test_idx] = False
            mask &= ~((ts_ms >= purge_lo) & (ts_ms <= purge_hi))
            mask &= ~((ts_ms > t1) & (ts_ms <= embargo_hi))

            train_idx = np.where(mask)[0]
            if len(train_idx) == 0:
                continue
            yield train_idx, test_idx

