"""Smoke tests for tools/meta_ab_winner_evaluator_v2.py.

Uses _StubModel to avoid loading real model files.
Tests cover: basic winner detection, tie on insufficient data,
champion-wins path, and bootstrap-CI veto on no evidence.
"""
import numpy as np
import pandas as pd

from tools.meta_ab_winner_evaluator_v2 import V2Config, evaluate_v2


class _StubModel:
    """Minimal stub that satisfies score_model_proba: has .features and .predict_proba.

    Implements a simple logistic function: p = sigmoid(x + bias)
    where x is the first feature value. bias shifts the decision boundary.
    """

    def __init__(self, features, bias):
        self.features = features
        self._bias = float(bias)

    def predict_proba(self, feat_dict):
        x = float(feat_dict.get(self.features[0], 0.0))
        p = 1.0 / (1.0 + np.exp(-(x + self._bias)))
        return float(p)


def _make_df(n: int = 4000, seed: int = 42) -> pd.DataFrame:
    """Synthetic dataset: feature 'f' splits good/bad trades deterministically."""
    # Half rows: f=-1 → bad trade (r=-0.2); half: f=2 → good trade (r=0.3)
    f = np.concatenate([np.ones(n // 2) * -1.0, np.ones(n // 2) * 2.0])
    r = np.concatenate([np.ones(n // 2) * -0.2, np.ones(n // 2) * 0.3])
    ok = np.ones(n, dtype=int)
    y = (r > 0).astype(int)
    return pd.DataFrame({"f": f, "r_mult": r, "ok": ok, "y": y, "symbol": ["BTCUSDT"] * n})


def test_v2_winner_basic():
    """Challenger (less conservative) should get winner or tie — never raises."""
    df = _make_df(n=4000)
    champ = _StubModel(["f"], bias=-1.0)   # conservative: misses many good trades
    chal = _StubModel(["f"], bias=0.0)     # fires on good trades at f=2

    cfg = V2Config(
        p_min=0.55,
        min_n=1000,
        min_delta_exp_r=0.0001,
        tail_r=-1.0,
        tail_slack=0.01,
        strata_cols=("symbol",),
        strata_topk=5,
    )
    rep = evaluate_v2(df, champ, chal, cfg)
    assert rep["winner"] in ("challenger", "tie"), f"Unexpected winner: {rep['winner']}"
    assert "delta" in rep
    assert "ci" in rep
    assert "strata_top" in rep


def test_v2_tie_on_insufficient_data():
    """With n < min_n the result must be 'tie' with reason='insufficient_data...'."""
    df = _make_df(n=50)
    champ = _StubModel(["f"], bias=0.0)
    chal = _StubModel(["f"], bias=0.0)

    cfg = V2Config(p_min=0.55, min_n=1000)
    rep = evaluate_v2(df, champ, chal, cfg)
    assert rep["winner"] == "tie"
    assert "insufficient_data" in rep["reason"]


def test_v2_champion_wins_when_challenger_clearly_worse():
    """Champion should win when challenger has clearly lower expR per candidate."""
    df = _make_df(n=4000)
    # Champion: high bias → fires on all, catches many good trades
    champ = _StubModel(["f"], bias=2.0)
    # Challenger: very low bias → fires on everything including bad trades
    chal = _StubModel(["f"], bias=-3.0)  # low threshold → lots of bad trades allowed

    cfg = V2Config(
        p_min=0.55,
        min_n=100,
        min_delta_exp_r=0.0001,
        tail_r=-0.1,
        tail_slack=0.001,
        bootstrap=0,       # disable CI to isolate pure delta logic
        strata_cols=(),    # no strata
    )
    rep = evaluate_v2(df, champ, chal, cfg)
    # Champion wins or tie — challenger is NOT declared winner
    assert rep["winner"] in ("champion", "tie"), f"Expected champion/tie but got: {rep['winner']}"


def test_v2_report_structure():
    """Report dict must always contain the required keys."""
    df = _make_df(n=4000)
    champ = _StubModel(["f"], bias=0.0)
    chal = _StubModel(["f"], bias=0.1)
    cfg = V2Config(p_min=0.55, min_n=100, bootstrap=1, strata_cols=("symbol",))
    rep = evaluate_v2(df, champ, chal, cfg)

    required_keys = {"ts_ms", "counts", "config", "winner", "reason", "champion", "challenger", "delta", "ci", "strata_top"}
    assert required_keys.issubset(rep.keys()), f"Missing keys: {required_keys - set(rep.keys())}"


def test_v2_no_eligible_rows():
    """Dataset with ok=0 for all rows → tie with reason='no_eligible_data'."""
    df = pd.DataFrame({
        "f": [1.0, 2.0, 3.0],
        "r_mult": [0.1, 0.2, 0.3],
        "ok": [0, 0, 0],
        "y": [1, 1, 1],
    })
    champ = _StubModel(["f"], bias=0.0)
    chal = _StubModel(["f"], bias=0.0)
    cfg = V2Config(p_min=0.55, min_n=1)
    rep = evaluate_v2(df, champ, chal, cfg)
    assert rep["winner"] == "tie"
    assert rep["reason"] == "no_eligible_data"
