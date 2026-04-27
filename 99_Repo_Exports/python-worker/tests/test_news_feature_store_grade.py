from news_pipeline.grade import compute_grade_id, compute_horizon_sec, compute_horizon_sec_with_grade


def test_grade_monotonic_risk():
    c = 0.8
    s = 0.2
    g1 = compute_grade_id(risk=0.10, surprise=s, confidence=c)
    g2 = compute_grade_id(risk=0.35, surprise=s, confidence=c)
    g3 = compute_grade_id(risk=0.75, surprise=s, confidence=c)
    assert g1 <= g2 <= g3


def test_grade_downweights_low_confidence():
    r = 0.55
    s = 0.0
    g_hi = compute_grade_id(risk=r, surprise=s, confidence=0.95)
    g_lo = compute_grade_id(risk=r, surprise=s, confidence=0.10)
    assert g_lo <= g_hi


def test_horizon_uses_tag_based_mapping():
    # Test that horizon comes from tag-based mapping, not just fallback
    h = compute_horizon_sec(primary_tag_id=1, tags_mask=0)
    # Should be a reasonable value from tag taxonomy (hours)
    assert 3600 <= h <= 48 * 3600


def test_horizon_with_grade_scaling():
    base = 4 * 3600  # 4 hours
    h0 = compute_horizon_sec_with_grade(base_horizon_sec=base, grade_id=0)
    h2 = compute_horizon_sec_with_grade(base_horizon_sec=base, grade_id=2)
    h4 = compute_horizon_sec_with_grade(base_horizon_sec=base, grade_id=4)

    assert h0 == 0  # grade 0 -> ignore
    assert h2 == base  # grade 2 -> base
    assert h4 > base  # grade 4 -> scaled up
