from news_pipeline.grade import compute_grade_id, compute_horizon_sec_with_grade


def test_grade_basic_thresholds():
    assert compute_grade_id(risk=0.0, surprise=0.0, confidence=1.0) == 0
    assert compute_grade_id(risk=0.2, surprise=0.0, confidence=1.0) in (1, 2)
    assert compute_grade_id(risk=0.9, surprise=0.0, confidence=1.0) == 4


def test_grade_confidence_suppresses():
    g_hi = compute_grade_id(risk=0.6, surprise=0.0, confidence=1.0)
    g_lo = compute_grade_id(risk=0.6, surprise=0.0, confidence=0.0)
    assert g_lo <= g_hi


def test_horizon_with_grade():
    base = 4 * 3600
    assert compute_horizon_sec_with_grade(base_horizon_sec=base, grade_id=0) == 0
    assert compute_horizon_sec_with_grade(base_horizon_sec=base, grade_id=2) == base
    assert compute_horizon_sec_with_grade(base_horizon_sec=base, grade_id=4) >= base
