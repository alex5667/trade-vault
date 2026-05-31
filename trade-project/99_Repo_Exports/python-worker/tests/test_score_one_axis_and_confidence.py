from __future__ import annotations

from handlers.signal_scoring.score_model import ScoreModel


class Ctx:
    symbol = "BTCUSDT"


def test_one_axis_final_score_and_confidence_bounds():
    m = ScoreModel()
    out = m.score(raw_score=0.80, conf_factor01=0.50, kind="breakout", ctx=Ctx(), parts_in={"x": 1.0})
    assert abs(out.final_score - 0.40) < 1e-9
    assert 0.0 <= out.confidence_pct <= 100.0
    assert out.parts["final_score"] == out.final_score
    assert out.parts["conf_factor01"] == 0.50
