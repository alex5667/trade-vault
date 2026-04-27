"""
Walk-Forward Calibrator — expanding-window out-of-sample validation engine.

Eliminates in-sample overfitting by splitting trade/time series into
expanding train windows and non-overlapping test windows.

Architecture:
  - Trade-indexed: each fold is a contiguous block of trades (not days).
  - Expanding window: train always starts from trade 0.
  - Objective function and evaluation function are pluggable callbacks.
  - Robust parameter = median of qualifying OOS fold parameters.
  - Deploy gate: std(oos_sharpe) < stability_threshold.

Usage:
    wfc = WalkForwardCalibrator(min_train_trades=100, test_trades=30, step_trades=20)
    result = wfc.run(trades, param_candidates, objective_fn, evaluate_fn)
    if result.deploy:
        apply(result.robust_param)

Refs:
    - News Agent walk-forward pattern (train_days=60, val_days=7)
    - P74 policy calibration suggester (tighten-only)
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, Generic, List, Optional, Sequence, TypeVar

import numpy as np

from common.log import setup_logger

logger = setup_logger("WalkForwardCalibrator")

T = TypeVar("T")  # trade record type


# ---------------------------------------------------------------------------
# Result data classes
# ---------------------------------------------------------------------------


@dataclass
class OOSMetrics:
    """Out-of-sample evaluation metrics for a single fold + parameter."""
    sharpe: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    expectancy_r: float = 0.0
    n_trades: int = 0
    score: float = 0.0          # composite objective score


@dataclass
class OOSFoldResult:
    """Full result of one walk-forward fold."""
    fold_idx: int
    train_start_idx: int          # inclusive
    train_end_idx: int            # exclusive
    test_start_idx: int           # inclusive
    test_end_idx: int             # exclusive
    best_param: float             # optimal parameter from train-set optimization
    oos_sharpe: float = 0.0
    oos_win_rate: float = 0.0
    oos_profit_factor: float = 0.0
    oos_expectancy_r: float = 0.0
    oos_n_trades: int = 0
    oos_score: float = 0.0
    train_score: float = 0.0     # in-sample score for comparison

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WalkForwardResult:
    """Aggregated walk-forward validation result."""
    symbol: str
    robust_param: float           # median of stable folds' best params
    stability_score: float        # std of OOS Sharpe across folds (lower = better)
    deploy: bool                  # True if stability_score < threshold
    n_folds: int
    n_stable_folds: int           # folds with oos_profit_factor > min_oos_pf
    mean_oos_sharpe: float = 0.0
    mean_oos_expectancy: float = 0.0
    mean_train_score: float = 0.0
    overfit_ratio: float = 0.0    # mean(train_score) / mean(oos_score) — >1 = overfit
    folds: List[OOSFoldResult] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["folds"] = [f.to_dict() for f in self.folds]
        return d


# ---------------------------------------------------------------------------
# Fold generator
# ---------------------------------------------------------------------------


def make_expanding_folds(
    n_items: int,
    min_train: int,
    test_size: int,
    step: int,
) -> List[tuple[int, int, int, int]]:
    """
    Generate expanding-window fold indices.

    Returns list of (train_start, train_end, test_start, test_end).
    All indices are [start, end) — exclusive end.

    Example with n=200, min_train=100, test=30, step=20:
      fold 0: train [0,100), test [100,130)
      fold 1: train [0,120), test [120,150)
      fold 2: train [0,140), test [140,170)
      fold 3: train [0,160), test [160,190)
    """
    if n_items < min_train + test_size:
        return []

    folds: List[tuple[int, int, int, int]] = []
    train_end = min_train

    while train_end + test_size <= n_items:
        test_start = train_end
        test_end = min(test_start + test_size, n_items)
        folds.append((0, train_end, test_start, test_end))
        train_end += step

    return folds


def make_sliding_folds(
    n_items: int,
    train_size: int,
    test_size: int,
    step: int,
) -> List[tuple[int, int, int, int]]:
    """
    Generate sliding-window fold indices (fixed train size).

    Returns list of (train_start, train_end, test_start, test_end).
    """
    if n_items < train_size + test_size:
        return []

    folds: List[tuple[int, int, int, int]] = []
    train_start = 0

    while train_start + train_size + test_size <= n_items:
        train_end = train_start + train_size
        test_start = train_end
        test_end = min(test_start + test_size, n_items)
        folds.append((train_start, train_end, test_start, test_end))
        train_start += step

    return folds


# ---------------------------------------------------------------------------
# Main calibrator
# ---------------------------------------------------------------------------


# Type aliases for the pluggable callbacks
ObjectiveFn = Callable[[Sequence[Any], float], float]
"""(trades_subset, param) -> score.  Higher = better."""

EvaluateFn = Callable[[Sequence[Any], float], OOSMetrics]
"""(trades_subset, param) -> OOSMetrics."""


class WalkForwardCalibrator:
    """
    Expanding-window walk-forward calibrator.

    Generic over any parameter being optimized (offset_mult, threshold, etc).
    Caller provides:
      - objective_fn(trades, param) -> float  (train-set scoring)
      - evaluate_fn(trades, param) -> OOSMetrics  (test-set evaluation)
      - param_candidates: list of candidate values to search
    """

    def __init__(
        self,
        min_train_trades: int = 100,
        test_trades: int = 30,
        step_trades: int = 20,
        stability_threshold: float = 0.5,
        min_oos_pf: float = 1.0,
        min_folds_to_deploy: int = 2,
        window_mode: str = "expanding",  # "expanding" | "sliding"
        sliding_train_size: int = 150,   # only used if window_mode="sliding"
    ) -> None:
        self.min_train_trades = max(1, min_train_trades)
        self.test_trades = max(1, test_trades)
        self.step_trades = max(1, step_trades)
        self.stability_threshold = stability_threshold
        self.min_oos_pf = min_oos_pf
        self.min_folds_to_deploy = max(1, min_folds_to_deploy)
        self.window_mode = window_mode
        self.sliding_train_size = max(1, sliding_train_size)

    def run(
        self,
        trades: Sequence[Any],
        param_candidates: List[float],
        objective_fn: ObjectiveFn,
        evaluate_fn: EvaluateFn,
        symbol: str = "",
    ) -> WalkForwardResult:
        """
        Execute walk-forward validation.

        Args:
            trades: Sequence of trade records (sorted by time, oldest first).
            param_candidates: List of candidate parameter values.
            objective_fn: (trades_slice, param) -> score (train-set).
            evaluate_fn: (trades_slice, param) -> OOSMetrics (test-set).
            symbol: Symbol name for attribution.

        Returns:
            WalkForwardResult with robust_param, stability_score, deploy flag.
        """
        n = len(trades)

        if not param_candidates:
            logger.warning("[%s] WF: no param candidates provided", symbol)
            return self._empty_result(symbol)

        # Generate fold indices
        if self.window_mode == "sliding":
            folds = make_sliding_folds(
                n, self.sliding_train_size, self.test_trades, self.step_trades,
            )
        else:
            folds = make_expanding_folds(
                n, self.min_train_trades, self.test_trades, self.step_trades,
            )

        if not folds:
            logger.warning(
                "[%s] WF: insufficient trades (%d) for walk-forward "
                "(min_train=%d, test=%d)",
                symbol, n, self.min_train_trades, self.test_trades,
            )
            return self._empty_result(symbol)

        logger.info(
            "[%s] WF: %d trades, %d folds, %d candidates, mode=%s",
            symbol, n, len(folds), len(param_candidates), self.window_mode,
        )

        fold_results: List[OOSFoldResult] = []

        for fold_idx, (tr_start, tr_end, ts_start, ts_end) in enumerate(folds):
            train_slice = trades[tr_start:tr_end]
            test_slice = trades[ts_start:ts_end]

            if len(test_slice) == 0:
                continue

            # ----- In-sample: find best param on train -----
            best_param = param_candidates[0]
            best_train_score = -1e18

            for param in param_candidates:
                try:
                    score = objective_fn(train_slice, param)
                except Exception as e:
                    logger.debug(
                        "[%s] WF fold %d: objective_fn error for param=%.4f: %s",
                        symbol, fold_idx, param, e,
                    )
                    score = -1e18

                if score > best_train_score:
                    best_train_score = score
                    best_param = param

            # ----- Out-of-sample: evaluate best param on test -----
            try:
                oos = evaluate_fn(test_slice, best_param)
            except Exception as e:
                logger.warning(
                    "[%s] WF fold %d: evaluate_fn error: %s", symbol, fold_idx, e,
                )
                oos = OOSMetrics()

            fold_result = OOSFoldResult(
                fold_idx=fold_idx,
                train_start_idx=tr_start,
                train_end_idx=tr_end,
                test_start_idx=ts_start,
                test_end_idx=ts_end,
                best_param=best_param,
                oos_sharpe=oos.sharpe,
                oos_win_rate=oos.win_rate,
                oos_profit_factor=oos.profit_factor,
                oos_expectancy_r=oos.expectancy_r,
                oos_n_trades=oos.n_trades,
                oos_score=oos.score,
                train_score=best_train_score,
            )
            fold_results.append(fold_result)

            logger.info(
                "[%s] WF fold %d: param=%.4f, train_score=%.3f, "
                "oos_sharpe=%.3f, oos_pf=%.3f, oos_wr=%.1f%%, oos_n=%d",
                symbol, fold_idx, best_param, best_train_score,
                oos.sharpe, oos.profit_factor,
                oos.win_rate * 100, oos.n_trades,
            )

        if not fold_results:
            return self._empty_result(symbol)

        return self._aggregate(symbol, fold_results)

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def _aggregate(
        self,
        symbol: str,
        folds: List[OOSFoldResult],
    ) -> WalkForwardResult:
        """Aggregate fold results into a single WalkForwardResult."""

        # Separate stable folds (OOS profit factor > threshold)
        stable_folds = [
            f for f in folds
            if f.oos_profit_factor > self.min_oos_pf
        ]

        n_stable = len(stable_folds)

        # Robust parameter: median of stable folds' best params
        if stable_folds:
            robust_param = float(np.median([f.best_param for f in stable_folds]))
        else:
            # Fallback: median of all folds
            robust_param = float(np.median([f.best_param for f in folds]))

        # Stability score: std of OOS Sharpe across ALL folds
        oos_sharpes = [f.oos_sharpe for f in folds]
        stability_score = float(np.std(oos_sharpes)) if len(oos_sharpes) > 1 else 999.0

        # Deploy gate
        deploy = (
            stability_score < self.stability_threshold
            and n_stable >= self.min_folds_to_deploy
        )

        # Aggregate metrics
        mean_oos_sharpe = float(np.mean(oos_sharpes)) if oos_sharpes else 0.0
        mean_oos_exp = float(np.mean([f.oos_expectancy_r for f in folds])) if folds else 0.0
        mean_train = float(np.mean([f.train_score for f in folds])) if folds else 0.0

        # Overfit ratio: mean(train) / mean(oos) — values >1 indicate overfitting
        mean_oos_score = float(np.mean([f.oos_score for f in folds])) if folds else 0.0
        if abs(mean_oos_score) > 1e-9:
            overfit_ratio = mean_train / mean_oos_score
        else:
            overfit_ratio = 0.0

        result = WalkForwardResult(
            symbol=symbol,
            robust_param=robust_param,
            stability_score=round(stability_score, 4),
            deploy=deploy,
            n_folds=len(folds),
            n_stable_folds=n_stable,
            mean_oos_sharpe=round(mean_oos_sharpe, 4),
            mean_oos_expectancy=round(mean_oos_exp, 4),
            mean_train_score=round(mean_train, 4),
            overfit_ratio=round(overfit_ratio, 4),
            folds=folds,
        )

        logger.info(
            "[%s] WF result: robust_param=%.4f, stability=%.4f, "
            "deploy=%s, folds=%d/%d stable, overfit_ratio=%.2f",
            symbol, robust_param, stability_score,
            deploy, n_stable, len(folds), overfit_ratio,
        )

        return result

    def _empty_result(self, symbol: str) -> WalkForwardResult:
        return WalkForwardResult(
            symbol=symbol,
            robust_param=0.0,
            stability_score=999.0,
            deploy=False,
            n_folds=0,
            n_stable_folds=0,
        )
