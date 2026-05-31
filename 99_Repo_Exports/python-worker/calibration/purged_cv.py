"""
calibration/purged_cv.py — Phase 1: purged walk-forward CV + statistical guards.

Provides:
  purged_walkforward(decision_ms, resolved_ms, n_blocks, embargo_ms)
    → Iterator of (train_idx, test_idx) pairs with purge + embargo applied.
    Purge: removes training samples whose outcome horizons [decision, resolved]
    overlap with the test window — eliminates label leakage.
    Embargo: additional buffer after test window end → prevents correlated samples.

  deflated_sharpe(sr, n_trials, skew, kurt, n_obs)
    → DSR: probability that the true Sharpe is > 0, corrected for selection bias
    across n_trials (López de Prado, "The Deflated Sharpe Ratio", 2016).

  pbo_estimate(fold_returns)
    → PBO: Probability of Backtest Overfitting via combinatorial cross-validation.
    Returns float in [0, 1]. Values above 0.5 indicate the selected strategy
    is likely overfitted.

  check_calibration_guards(sr, n_trials, skew, kurt, n_obs, fold_returns)
    → (passed: bool, details: dict)
    Applies CALIBRATION_MIN_DSR and CALIBRATION_MAX_PBO gates.

Usage:
    from calibration.purged_cv import purged_walkforward, check_calibration_guards

    for train_idx, test_idx in purged_walkforward(
        decision_ms, resolved_ms, n_blocks=8, embargo_ms=600_000
    ):
        model.fit(X[train_idx], y[train_idx])
        score = evaluate(model, X[test_idx], y[test_idx])
        ...
"""
from __future__ import annotations

import math
import os
from typing import Iterator

import numpy as np


# ─── ENV defaults ─────────────────────────────────────────────────────────────

_MIN_DSR     = float(os.getenv("CALIBRATION_MIN_DSR", "0.0"))
_MAX_PBO     = float(os.getenv("CALIBRATION_MAX_PBO", "0.5"))
_N_BLOCKS    = int(os.getenv("CALIBRATION_N_BLOCKS", "8"))
_EMBARGO_MS  = int(os.getenv("CALIBRATION_EMBARGO_MS", "600000"))
_MIN_SAMPLES = int(os.getenv("CALIBRATION_MIN_SAMPLES", "500"))


# ─── Purged walk-forward ──────────────────────────────────────────────────────

def purged_walkforward(
    decision_ms: np.ndarray,
    resolved_ms: np.ndarray,
    n_blocks: int = _N_BLOCKS,
    embargo_ms: int = _EMBARGO_MS,
) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    """
    Expanding purged walk-forward cross-validation.

    Splits indices into n_blocks chronological blocks. For each fold k:
      test  = block[k]
      train = all blocks before k, with samples purged that overlap test window.

    Purge condition: a training sample is removed if its outcome horizon
    [decision_ms[i], resolved_ms[i]] overlaps the test period
    [test_start, test_end + embargo_ms]. This eliminates label leakage
    caused by overlapping TTL windows between train and test.

    Args:
        decision_ms: np.ndarray of int, epoch_ms at decision time (sort order)
        resolved_ms: np.ndarray of int, epoch_ms at resolution (may be NaN for open)
        n_blocks:    number of CV folds (walk-forward blocks)
        embargo_ms:  buffer in ms AFTER test_end removed from train

    Yields:
        (train_idx, test_idx): index arrays into the original arrays
    """
    n = len(decision_ms)
    if n == 0 or n_blocks < 2:
        return

    # Sort by decision time to ensure chronological blocks
    order = np.argsort(decision_ms)

    # Split sorted order into n_blocks approximately equal blocks
    bounds = np.array_split(order, n_blocks)

    for k in range(1, n_blocks):
        test_idx = bounds[k]
        if len(test_idx) == 0:
            continue

        # Test window: [t_start, t_end]
        t_start = int(decision_ms[test_idx].min())
        t_end   = int(np.max([
            resolved_ms[i]
            for i in test_idx
            if not math.isnan(float(resolved_ms[i]))
        ], initial=t_start))

        # Pool all blocks before k
        train_pool = np.concatenate(bounds[:k]) if k > 0 else np.array([], dtype=int)
        if len(train_pool) == 0:
            continue

        # Purge: remove samples whose [decision_ms, resolved_ms] overlaps
        # [t_start, t_end + embargo_ms].
        # A sample overlaps if: decision_ms[i] <= t_end + embargo_ms
        #                   AND resolved_ms[i] >= t_start
        embargo_end = t_end + embargo_ms
        keep_mask = np.ones(len(train_pool), dtype=bool)
        for j, i in enumerate(train_pool):
            r_ms = resolved_ms[i]
            r_ms_safe = float(r_ms)
            if math.isnan(r_ms_safe):
                # Open trade: conservative — treat as resolved at t_end (worst case)
                r_ms_safe = float(t_end)
            d_ms = float(decision_ms[i])
            # Overlaps if NOT (fully before OR fully after)
            overlaps = (d_ms <= embargo_end) and (r_ms_safe >= t_start)
            if overlaps:
                keep_mask[j] = False

        train_idx = train_pool[keep_mask]
        if len(train_idx) == 0:
            continue

        yield train_idx, test_idx


# ─── Deflated Sharpe Ratio ────────────────────────────────────────────────────

def deflated_sharpe(
    sr: float,
    n_trials: int,
    skew: float,
    kurt: float,
    n_obs: int,
) -> float:
    """
    Deflated Sharpe Ratio (DSR) — López de Prado (2016).

    Probability that the observed SR reflects a true positive edge
    after correcting for selection bias across n_trials experiments.

    DSR > 0 ↔ strategy likely has positive edge even after trial correction.
    DSR < 0 ↔ strategy likely overfitted (selected best of random noise).

    Args:
        sr:       observed Sharpe ratio of the selected strategy
        n_trials: number of independent trials / parameter combinations tested
        skew:     skewness of returns distribution
        kurt:     excess kurtosis of returns distribution
        n_obs:    number of observations (return samples)

    Returns:
        float in [0, 1]: DSR probability. 0.5 = indeterminate.
    """
    from scipy.stats import norm  # type: ignore[import]

    if n_obs < 4 or n_trials < 1:
        return 0.0

    # n_trials=1 → ppf(0)=-inf; minimum 2 to keep formula well-defined
    n_trials_safe = max(n_trials, 2)

    # Standard error of SR under non-normal distribution (López de Prado 2016)
    # kurt here = EXCESS kurtosis (normal distribution → kurt=0)
    variance = (1.0 - skew * sr + (kurt / 4.0) * sr ** 2) / (n_obs - 1)
    if variance <= 0:
        # Degenerate: use plain sr/(1/sqrt(n_obs-1)) as fallback
        variance = 1.0 / max(n_obs - 1, 1)
    sr_std = math.sqrt(variance)
    if sr_std <= 0:
        return 0.0

    # Expected maximum SR under H0 (all strategies are noise)
    # Euler–Mascheroni constant γ ≈ 0.5772
    gamma = 0.5772156649
    e_max_sr = sr_std * (
        (1.0 - gamma) * norm.ppf(1.0 - 1.0 / n_trials_safe)
        + gamma * norm.ppf(1.0 - 1.0 / (n_trials_safe * math.e))
    )

    return float(norm.cdf((sr - e_max_sr) / sr_std))


# ─── PBO Estimate ─────────────────────────────────────────────────────────────

def pbo_estimate(fold_returns: list[list[float]]) -> float:
    """
    Probability of Backtest Overfitting (PBO) — López de Prado (2015).

    Simplified combinatorial cross-validation variant:
      1. For each pair of folds (in-sample vs out-of-sample):
         - Identify the best strategy in-sample
         - Check if the same strategy is also best out-of-sample
      2. PBO = fraction of combinations where in-sample winner ≠ OOS winner.

    Args:
        fold_returns: list of length n_folds, each element is a list of per-strategy
                      returns on that fold. Shape: [n_folds][n_strategies].

    Returns:
        float [0, 1]: 0 = no overfitting evidence, 1 = certain overfitting.
    """
    if len(fold_returns) < 2:
        return 0.0

    n_folds = len(fold_returns)
    n_strats = len(fold_returns[0]) if fold_returns else 0
    if n_strats < 2:
        return 0.0

    overfit_count = 0
    trial_count   = 0

    # Combinatorial: for each pair of folds (is, oos)
    for is_fold in range(n_folds):
        for oos_fold in range(n_folds):
            if is_fold == oos_fold:
                continue
            is_rets  = fold_returns[is_fold]
            oos_rets = fold_returns[oos_fold]
            if len(is_rets) != n_strats or len(oos_rets) != n_strats:
                continue

            # Best strategy by in-sample returns
            is_best  = int(np.argmax(is_rets))
            # Best strategy by out-of-sample returns
            oos_best = int(np.argmax(oos_rets))

            trial_count += 1
            if is_best != oos_best:
                overfit_count += 1

    if trial_count == 0:
        return 0.0

    return overfit_count / trial_count


# ─── Guard check ──────────────────────────────────────────────────────────────

def check_calibration_guards(
    sr: float,
    n_trials: int,
    skew: float,
    kurt: float,
    n_obs: int,
    fold_returns: list[list[float]] | None = None,
    *,
    min_dsr: float = _MIN_DSR,
    max_pbo: float = _MAX_PBO,
) -> tuple[bool, dict]:
    """
    Apply calibration quality guards: DSR and PBO.

    Returns:
        (passed, details): True if calibration passes all gates, plus detail dict.
    """
    dsr = deflated_sharpe(sr, n_trials, skew, kurt, n_obs)
    pbo = pbo_estimate(fold_returns) if fold_returns else 0.0

    dsr_ok = dsr >= min_dsr
    pbo_ok = pbo <= max_pbo
    passed = dsr_ok and pbo_ok

    return passed, {
        "passed":   passed,
        "dsr":      dsr,
        "dsr_ok":   dsr_ok,
        "pbo":      pbo,
        "pbo_ok":   pbo_ok,
        "min_dsr":  min_dsr,
        "max_pbo":  max_pbo,
        "n_obs":    n_obs,
        "n_trials": n_trials,
        "sr":       sr,
    }
