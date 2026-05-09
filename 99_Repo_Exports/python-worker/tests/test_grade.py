from news_pipeline.grade import compute_horizon_sec, compute_news_grade_id


def test_grade_basic():
    assert compute_news_grade_id(news_risk=0.0, confidence=0.0, primary_tag_id=0) == 0
    assert compute_news_grade_id(news_risk=0.9, confidence=0.9, primary_tag_id=0) in (3,4)

def test_grade_tag_floor_macro():
    # Primary tag ensures at least grade 2 if risk >= 0.20 and confidence >= 0.35
    assert compute_news_grade_id(news_risk=0.25, confidence=0.9, primary_tag_id=3) >= 2
    # But not if risk is too low
    assert compute_news_grade_id(news_risk=0.05, confidence=0.9, primary_tag_id=3) == 0

def test_horizon_liquidation_short():
    assert compute_horizon_sec(4, primary_tag_id=16) == 24*3600  # grade 4 + tag 16 = max(24h, 2h) = 24h

def test_horizon_geopolitics_long():
    assert compute_horizon_sec(4, primary_tag_id=6) == 24*3600  # grade 4 + tag 6 = max(24h, 24h) = 24h
