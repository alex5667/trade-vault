# python-worker/tests/test_tags.py
from news_pipeline.tags import tags_to_mask, pick_primary_tag

def test_tags_to_mask():
    m = tags_to_mask(["cpi", "fomc", "CPI", "unknown"])
    assert m != 0
    assert (m & (1 << 0)) != 0  # cpi bit
    assert (m & (1 << 2)) != 0  # fomc bit

def test_pick_primary_tag():
    # smallest id wins in your current impl
    assert pick_primary_tag(["earnings", "cpi"]) == 1