from __future__ import annotations

"""CSCV / PBO utilities for nightly strategy research stats.

Input shape is deliberately simple: a mapping of variant -> ordered list of
per-period scores (higher is better). The implementation keeps the selection
procedure deterministic and low-dependency so it can run in the timer worker.
"""

import itertools
import math
from collections.abc import Iterable, Mapping, Sequence


def _clean_score(v: object) -> float:
    try:
        f = float(v)
    except Exception:
        return 0.0
    if not math.isfinite(f):
        return 0.0
    return f


def _matrix(matrix: Mapping[str, Sequence[float]]) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    width = None
    for key, vals in matrix.items():
        arr = [_clean_score(v) for v in vals]
        if width is None:
            width = len(arr)
        if width != len(arr):
            raise ValueError('all variants must have identical period counts')
        out[str(key)] = arr
    if not out:
        raise ValueError('empty variant matrix')
    return out


def contiguous_folds(n_periods: int, n_folds: int = 8) -> list[list[int]]:
    if n_periods < 2:
        raise ValueError('need at least 2 periods')
    if n_folds < 2:
        raise ValueError('need at least 2 folds')
    n_folds = min(int(n_folds), int(n_periods))
    base = n_periods // n_folds
    rem = n_periods % n_folds
    folds: list[list[int]] = []
    cur = 0
    for i in range(n_folds):
        size = base + (1 if i < rem else 0)
        idx = list(range(cur, cur + size))
        if idx:
            folds.append(idx)
        cur += size
    if len(folds) % 2 == 1:
        folds = folds[:-1]
    if len(folds) < 2:
        raise ValueError('insufficient non-empty folds')
    return folds


def _score_variant(period_scores: Sequence[float], indices: Iterable[int]) -> float:
    xs = [float(period_scores[i]) for i in indices]
    if not xs:
        return 0.0
    return sum(xs) / float(len(xs))


def _percentile_rank_desc(values: Sequence[tuple[str, float]], picked: str) -> float:
    ordered = sorted(values, key=lambda kv: (kv[1], kv[0]), reverse=True)
    n = len(ordered)
    if n <= 1:
        return 1.0
    for rank, (name, _) in enumerate(ordered):
        if name == picked:
            # top -> 1.0, bottom -> 0.0
            return 1.0 - (rank / float(n - 1))
    return 0.0


def compute_pbo(matrix: Mapping[str, Sequence[float]], *, n_folds: int = 8) -> dict[str, float]:
    """Compute Probability of Backtest Overfitting via CSCV.

    Args:
        matrix: mapping of variant_id -> ordered list of per-period scores
        n_folds: number of contiguous folds for combinatorial splits

    Returns:
        dict with keys: pbo, cscv_splits, chosen_variant_unique
    """
    mat = _matrix(matrix)
    variants = sorted(mat)
    periods = len(next(iter(mat.values())))
    folds = contiguous_folds(periods, n_folds=n_folds)
    half = len(folds) // 2
    split_choices = list(itertools.combinations(range(len(folds)), half))
    lambdas: list[float] = []
    unique_train_picks = 0

    for train_fold_ids in split_choices:
        train_idx = [i for fid in train_fold_ids for i in folds[fid]]
        test_idx = [i for fid in range(len(folds)) if fid not in train_fold_ids for i in folds[fid]]
        train_scores = [(v, _score_variant(mat[v], train_idx)) for v in variants]
        train_order = sorted(train_scores, key=lambda kv: (kv[1], kv[0]), reverse=True)
        top_score = train_order[0][1]
        top_names = [name for name, score in train_order if score == top_score]
        if len(top_names) == 1:
            unique_train_picks += 1
        picked = train_order[0][0]
        test_scores = [(v, _score_variant(mat[v], test_idx)) for v in variants]
        pct = min(max(_percentile_rank_desc(test_scores, picked), 1e-6), 1.0 - 1e-6)
        lambdas.append(math.log(pct / (1.0 - pct)))

    negative = sum(1 for x in lambdas if x <= 0.0)
    pbo = negative / float(len(lambdas)) if lambdas else 0.0
    return {
        'pbo': float(pbo),
        'cscv_splits': float(len(lambdas)),
        'chosen_variant_unique': 1.0 if unique_train_picks == len(lambdas) and len(lambdas) > 0 else 0.0,
    }
