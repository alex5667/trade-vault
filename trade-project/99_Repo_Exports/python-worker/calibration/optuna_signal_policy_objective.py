"""
calibration/optuna_signal_policy_objective.py — Plan 3 / Step 4.

Builds an Optuna objective that searches signal-policy parameter space and
penalizes results that fail the promotion gate. Optuna ONLY produces candidate
parameters — it never promotes anything on its own. The caller wires the best
trial into the promotion-manifest workflow (report-only by default).

Why no `import optuna` at module top:
  * optuna is an optional extra (heavy dep, only needed by the runner service).
  * The factory `build_objective(...)` accepts any object that exposes the
    Optuna Trial API (`suggest_float`, `suggest_int`, `report`, etc.). This
    keeps the module importable in tests without optuna installed and lets us
    test the objective with a tiny in-house fake trial.

Search space mirrors the user's plan: ml_p_min, edge_min_bps, tp_k_atr,
sl_k_atr, max_spread_bps. Extend by passing a `space_extension(trial)` callable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol


class _TrialLike(Protocol):
    """Minimal subset of optuna.Trial used by build_objective."""

    def suggest_float(self, name: str, low: float, high: float, *,
                       step: float | None = ..., log: bool = ...) -> float: ...
    def suggest_int(self, name: str, low: int, high: int, *,
                     step: int = ..., log: bool = ...) -> int: ...


@dataclass(frozen=True)
class WalkForwardResult:
    """What `run_purged_walk_forward` MUST return.

    `oos_trades` is the join across all OOS folds, used to enforce min sample size
    before we trust the score.
    """

    oos_trades: int
    mean_oos_profit_factor: float
    mean_oos_sharpe: float
    deflated_sharpe: float
    pbo: float
    ece: float
    pass_rate: float
    max_drawdown_penalty: float = 0.0


def default_search_space(trial: _TrialLike) -> dict[str, float]:
    return {
        "ml_p_min":       trial.suggest_float("ml_p_min", 0.50, 0.85),
        "edge_min_bps":   trial.suggest_float("edge_min_bps", 0.0, 25.0),
        "tp_k_atr":       trial.suggest_float("tp_k_atr", 0.5, 2.5),
        "sl_k_atr":       trial.suggest_float("sl_k_atr", 0.5, 2.5),
        "max_spread_bps": trial.suggest_float("max_spread_bps", 3.0, 30.0),
    }


def score_walk_forward(wf: WalkForwardResult) -> float:
    """Plan 3 weighted-sum scoring with hard penalties.

    Pure function: same input → same output. Kept separate so unit tests can
    exercise the scoring rules without simulating an Optuna trial.
    """
    score = (
        0.40 * wf.mean_oos_profit_factor
        + 0.25 * wf.mean_oos_sharpe
        + 0.20 * wf.deflated_sharpe
        - 0.15 * wf.max_drawdown_penalty
    )

    # Hard penalties — same gates as PromotionMetrics.can_promote, expressed as
    # large negative shifts so the optimizer learns to avoid these regions.
    if wf.pbo > 0.25:
        score -= 10.0
    if wf.deflated_sharpe <= 0.0:
        score -= 10.0
    if wf.ece > 0.07:
        score -= 5.0
    if wf.pass_rate < 0.02:
        score -= 5.0

    return score


def build_objective(
    *,
    run_purged_walk_forward: Callable[[dict[str, float]], WalkForwardResult],
    min_oos_trades: int = 300,
    search_space: Callable[[_TrialLike], dict[str, float]] = default_search_space,
    prune_exc: type[Exception] | None = None,
) -> Callable[[_TrialLike], float]:
    """Return an objective(trial) -> float closure for `study.optimize`.

    Args:
        run_purged_walk_forward: caller-supplied evaluator that takes params dict
            and returns a WalkForwardResult.
        min_oos_trades: prune trials whose OOS coverage is too thin to trust.
        search_space:    factory that uses trial.suggest_* to define the params.
        prune_exc:       optional Optuna TrialPruned class (caller passes
                         optuna.TrialPruned when wiring). When None, we raise
                         ValueError for "too few trades" — caller's loop should
                         catch as needed.
    """
    def objective(trial: _TrialLike) -> float:
        params = search_space(trial)
        wf = run_purged_walk_forward(params)

        if wf.oos_trades < min_oos_trades:
            if prune_exc is not None:
                raise prune_exc("too_few_oos_trades")
            raise ValueError("too_few_oos_trades")

        return score_walk_forward(wf)

    return objective


def run_study(
    *,
    run_purged_walk_forward: Callable[[dict[str, float]], WalkForwardResult],
    n_trials: int = 200,
    timeout_sec: int | None = None,
    study_name: str = "signal_policy_v1",
    storage_url: str | None = None,
    direction: str = "maximize",
    min_oos_trades: int = 300,
) -> Any:
    """Thin wrapper around optuna.create_study + study.optimize.

    Imports optuna lazily so the module loads without the dep installed. Returns
    the completed Study object; the caller picks `study.best_trial` and builds
    a promotion manifest.
    """
    import optuna  # noqa: PLC0415 — lazy

    study = optuna.create_study(
        study_name=study_name,
        storage=storage_url,
        direction=direction,
        load_if_exists=True,
    )
    objective = build_objective(
        run_purged_walk_forward=run_purged_walk_forward,
        min_oos_trades=min_oos_trades,
        prune_exc=optuna.TrialPruned,
    )
    study.optimize(objective, n_trials=n_trials, timeout=timeout_sec)
    return study
