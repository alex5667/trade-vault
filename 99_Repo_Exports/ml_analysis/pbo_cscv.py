from __future__ import annotations

"""PBO / CSCV helpers for strategy-selection over many variants.

Input format is intentionally simple: a mapping variant -> list of
per-period scores. Scores can be Sharpe, expectancy, precision@topX, mean R, or
any metric where higher is better.
"""

import itertools
import math
from dataclasses import dataclass
from typing import Dict, Mapping, Sequence

import numpy as np


def _to_matrix(scores_by_variant: Mapping) -> tuple:
    variants = sorted(str(k) for k in scores_by_variant.keys())
    rows = []
    for v in variants:
        vals = []
        for x in scores_by_variant[v]:
            try:
                vals.append(float(x))
            except Exception:
                vals.append(float("nan"))
        rows.append(vals)
    mat = np.asarray(rows, dtype=np.float64)
    if mat.ndim != 2:
        raise ValueError("scores_by_variant must form a 2D matrix")
    return variants, mat


def _mean_ignore_nan(a: np.ndarray, axis: int) -> np.ndarray:
    with np.errstate(all="ignore"):
        out = np.nanmean(a, axis=axis)
    return np.where(np.isfinite(out), out, -np.inf)


@dataclass(frozen=True)
class CscvResult:
    n_variants: int
    n_periods: int
    n_splits: int
    pbo: float
    lambda_logits_mean: float
    chosen_variant_unique: int


def compute_pbo_cscv(scores_by_variant: Mapping) -> CscvResult:
    """Combinatorially Symmetric Cross-Validation (CSCV) PBO estimator.

    Returns a CscvResult where pbo in [0, 1]: higher means more overfitting.
    """
    variants, mat = _to_matrix(scores_by_variant)
    n_variants, n_periods = mat.shape
    if n_variants < 2 or n_periods < 4 or n_periods % 2 != 0:
        raise ValueError("need >=2 variants and an even number of periods >=4")
    half = n_periods // 2
    splits = list(itertools.combinations(range(n_periods), half))
    lambda_logits = []
    chosen = []
    for ins_idx in splits:
        ins = np.array(sorted(ins_idx), dtype=int)
        oos = np.array(sorted(set(range(n_periods)) - set(ins_idx)), dtype=int)
        ins_mean = _mean_ignore_nan(mat[:, ins], axis=1)
        best_idx = int(np.argmax(ins_mean))
        chosen.append(best_idx)
        oos_mean = _mean_ignore_nan(mat[:, oos], axis=1)
        order = np.argsort(oos_mean)
        rank = int(np.where(order == best_idx)[0][0]) + 1  # 1..M, lower = worse
        lam = rank / float(max(1, n_variants + 1))
        lam = min(max(lam, 1e-6), 1.0 - 1e-6)
        lambda_logits.append(math.log(lam / (1.0 - lam)))
    lambda_arr = np.asarray(lambda_logits, dtype=np.float64)
    pbo = float(np.mean(lambda_arr <= 0.0))
    return CscvResult(
        n_variants=n_variants,
        n_periods=n_periods,
        n_splits=len(splits),
        pbo=pbo,
        lambda_logits_mean=float(np.mean(lambda_arr)) if lambda_arr.size else float("nan"),
        chosen_variant_unique=int(len(set(chosen))),
    )
