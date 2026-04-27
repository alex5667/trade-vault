from __future__ import annotations

from news_pipeline.grade import compute_grade_id


def test_grade_bounds():
    assert compute_grade_id(risk=0.0, surprise=0.0, confidence=0.0) == 0
    g = compute_grade_id(risk=1.0, surprise=1.0, confidence=1.0)
    assert 0 <= g <= 4


def test_grade_monotonic_risk():
    g1 = compute_grade_id(risk=0.10, surprise=0.0, confidence=1.0)
    g2 = compute_grade_id(risk=0.40, surprise=0.0, confidence=1.0)
    g3 = compute_grade_id(risk=0.80, surprise=0.0, confidence=1.0)
    assert g1 <= g2 <= g3


def test_grade_confidence_suppresses():
    # Same intensity, lower confidence should not increase grade.
    hi = compute_grade_id(risk=0.70, surprise=0.0, confidence=1.0)
    lo = compute_grade_id(risk=0.70, surprise=0.0, confidence=0.0)
    assert lo <= hi


def test_grade_surprise_sign_matters():
    # Same magnitude: positive surprise should not be weaker than negative in our rule set.
    gp = compute_grade_id(risk=0.0, surprise=+0.9, confidence=1.0)
    gn = compute_grade_id(risk=0.0, surprise=-0.9, confidence=1.0)
    assert gp >= gn