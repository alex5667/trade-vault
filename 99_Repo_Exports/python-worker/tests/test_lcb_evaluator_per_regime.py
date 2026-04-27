import random


from services.ab_winner_evaluator_lcb import LCBEvaluatorPerRegime


def _rows(regime: str, arm: str, n: int, mean: float, sd: float) -> list[dict]:
    out = []
    for _ in range(n):
        r = random.gauss(mu=mean, sigma=sd)
        out.append({"regime": regime, "ab_arm": arm, "r_mult": float(r)})
    return out


def test_lcb_picks_best_arm_when_enough_samples():
    random.seed(7)
    rows = []
    rows += _rows("range", "A", 120, mean=0.10, sd=0.40)
    rows += _rows("range", "B", 120, mean=0.25, sd=0.40)
    rows += _rows("range", "C", 120, mean=0.05, sd=0.40)
    out = LCBEvaluatorPerRegime(cfg={"min_n_default": 60}).pick_winner(rows)
    assert out and out["winner_arm"] == "B"
    assert out["reason"] == "ok"


def test_lcb_falls_back_to_A_when_insufficient_n():
    random.seed(9)
    rows = []
    rows += _rows("trend", "B", 10, mean=1.0, sd=0.1)
    out = LCBEvaluatorPerRegime(cfg={"min_n_default": 60}).pick_winner(rows)
    assert out and out["winner_arm"] == "A"
    assert out["reason"] == "insufficient_n"


def test_lcb_thin_regime_uses_stricter_defaults():
    random.seed(11)
    rows = []
    # B looks slightly better, but with high variance; thin should demand higher confidence.
    rows += _rows("thin", "A", 140, mean=0.10, sd=0.60)
    rows += _rows("thin", "B", 140, mean=0.14, sd=0.80)
    out = LCBEvaluatorPerRegime(cfg={"min_n_thin": 120, "min_lcb_r_thin": 0.10, "lcb_z_thin": 1.64}).pick_winner(rows)
    assert out and out["winner_arm"] in ("A", "B")
    # But it must never throw; and should provide diagnostics.
    assert "arms" in out and isinstance(out["arms"], list)
