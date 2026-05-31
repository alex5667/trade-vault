"""Plan 3 / Step 4 — Optuna objective wiring (with fake trial)."""
from __future__ import annotations

import pytest

from calibration.optuna_signal_policy_objective import (
    WalkForwardResult,
    build_objective,
    default_search_space,
    score_walk_forward,
)


class _FakeTrial:
    """Just enough of optuna.Trial to test `build_objective` without optuna installed."""

    def __init__(self, values: dict[str, float] | None = None):
        self.values = values or {}
        self.suggested: list[str] = []

    def suggest_float(self, name, low, high, *, step=None, log=False):
        self.suggested.append(name)
        v = self.values.get(name, (low + high) / 2.0)
        assert low <= v <= high
        return v

    def suggest_int(self, name, low, high, *, step=1, log=False):
        self.suggested.append(name)
        v = self.values.get(name, (low + high) // 2)
        return int(v)


class _PrunedExc(Exception):
    pass


# ─── score_walk_forward (pure) ───────────────────────────────────────────────


def _wf(**overrides) -> WalkForwardResult:
    base = dict(
        oos_trades=500,
        mean_oos_profit_factor=1.2,
        mean_oos_sharpe=0.5,
        deflated_sharpe=0.3,
        pbo=0.15,
        ece=0.04,
        pass_rate=0.07,
        max_drawdown_penalty=0.0,
    )
    base.update(overrides)
    return WalkForwardResult(**base)


def test_score_rewards_good_walkforward():
    score = score_walk_forward(_wf())
    assert score > 0


def test_score_penalises_high_pbo():
    bad = score_walk_forward(_wf(pbo=0.6))
    good = score_walk_forward(_wf(pbo=0.1))
    assert bad < good - 5  # ≥10 penalty applied


def test_score_penalises_non_positive_dsr():
    score = score_walk_forward(_wf(deflated_sharpe=-0.1))
    assert score < 0  # heavy penalty


def test_score_penalises_high_ece():
    bad = score_walk_forward(_wf(ece=0.2))
    good = score_walk_forward(_wf(ece=0.04))
    assert bad < good - 4


def test_score_penalises_low_pass_rate():
    bad = score_walk_forward(_wf(pass_rate=0.001))
    good = score_walk_forward(_wf(pass_rate=0.05))
    assert bad < good - 4


# ─── default_search_space exposes all five params ────────────────────────────


def test_search_space_suggests_expected_params():
    trial = _FakeTrial()
    params = default_search_space(trial)
    assert set(params.keys()) == {
        "ml_p_min", "edge_min_bps", "tp_k_atr", "sl_k_atr", "max_spread_bps",
    }
    assert trial.suggested == list(params.keys())


# ─── build_objective wiring ──────────────────────────────────────────────────


def test_objective_returns_score_when_trades_sufficient():
    def evaluator(params: dict[str, float]) -> WalkForwardResult:
        return _wf(oos_trades=400)

    objective = build_objective(
        run_purged_walk_forward=evaluator,
        min_oos_trades=300,
    )
    trial = _FakeTrial()
    score = objective(trial)
    assert score > 0


def test_objective_prunes_when_oos_trades_too_low():
    def evaluator(params: dict[str, float]) -> WalkForwardResult:
        return _wf(oos_trades=50)

    objective = build_objective(
        run_purged_walk_forward=evaluator,
        min_oos_trades=300,
        prune_exc=_PrunedExc,
    )
    with pytest.raises(_PrunedExc):
        objective(_FakeTrial())


def test_objective_raises_valueerror_when_no_prune_exc():
    """No custom prune class → ValueError fallback (caller catches itself)."""
    objective = build_objective(
        run_purged_walk_forward=lambda p: _wf(oos_trades=10),
        min_oos_trades=300,
    )
    with pytest.raises(ValueError, match="too_few"):
        objective(_FakeTrial())


def test_objective_passes_suggested_params_to_evaluator():
    captured: dict[str, dict] = {}

    def evaluator(params: dict[str, float]) -> WalkForwardResult:
        captured["params"] = params
        return _wf()

    objective = build_objective(run_purged_walk_forward=evaluator, min_oos_trades=10)
    trial = _FakeTrial(values={
        "ml_p_min": 0.7, "edge_min_bps": 12.0, "tp_k_atr": 1.5,
        "sl_k_atr": 1.0, "max_spread_bps": 8.0,
    })
    objective(trial)
    assert captured["params"]["ml_p_min"] == 0.7
    assert captured["params"]["edge_min_bps"] == 12.0


def test_objective_supports_custom_search_space():
    def custom_space(trial):
        return {"only_param": trial.suggest_float("only_param", 0.0, 1.0)}

    objective = build_objective(
        run_purged_walk_forward=lambda p: _wf(),
        min_oos_trades=10,
        search_space=custom_space,
    )
    trial = _FakeTrial(values={"only_param": 0.5})
    score = objective(trial)
    assert score > 0
    assert trial.suggested == ["only_param"]


def test_run_study_raises_without_optuna_installed():
    """run_study lazy-imports optuna; absent dep should surface ImportError."""
    from calibration.optuna_signal_policy_objective import run_study
    try:
        import optuna  # type: ignore  # noqa
        pytest.skip("optuna installed; this asserts only the absent-dep case")
    except ImportError:
        pass
    with pytest.raises(ImportError):
        run_study(run_purged_walk_forward=lambda p: _wf(), n_trials=1)
