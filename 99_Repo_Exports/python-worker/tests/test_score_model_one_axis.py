from __future__ import annotations

from handlers.scoring.score_model import ScoreModel


def test_one_axis_final_score_and_confidence_pct():
    m = ScoreModel()
    r = m.score(raw_score=2.0, conf_factor01=0.5)
    assert r.final_score == 1.0
    assert 0.0 <= r.conf_factor01 <= 1.0
    assert 0.0 <= r.confidence_pct <= 100.0


def test_zero_conf_factor_means_zero_confidence():
    m = ScoreModel()
    r = m.score(raw_score=10.0, conf_factor01=0.0)
    assert r.final_score == 0.0
    assert r.confidence_pct == 0.0