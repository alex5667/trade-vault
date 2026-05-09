from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class ThrResult:
    thr: float
    n_take: int
    take_rate: float
    edge_rate: float
    mean_util: float
    sum_util: float


def best_threshold_by_utility(
    *,
    p: np.ndarray,
    y_edge: np.ndarray,
    util_r: np.ndarray,
    thr_min: float,
    thr_max: float,
    thr_step: float,
    min_trades: int,
) -> ThrResult:
    best = ThrResult(thr=thr_max, n_take=0, take_rate=0.0, edge_rate=0.0, mean_util=0.0, sum_util=-1e18)

    n = len(p)
    if n == 0:
        return best

    grid = np.arange(thr_min, thr_max + 1e-12, thr_step, dtype=float)
    for thr in grid:
        take = p >= thr
        n_take = int(take.sum())
        if n_take < int(min_trades):
            continue
        sum_util = float(util_r[take].sum())
        mean_util = float(util_r[take].mean()) if n_take else 0.0
        edge_rate = float(y_edge[take].mean()) if n_take else 0.0
        take_rate = float(n_take / n)

        # maximize sum util as primary (more robust than mean), with mean util as tie-breaker
        if sum_util > best.sum_util + 1e-9 or (abs(sum_util - best.sum_util) <= 1e-9 and mean_util > best.mean_util):
            best = ThrResult(
                thr=float(thr),
                n_take=n_take,
                take_rate=take_rate,
                edge_rate=edge_rate,
                mean_util=mean_util,
                sum_util=sum_util,
            )

    return best

