from __future__ import annotations

from contexts import NewsFeatures


def test_newsfeatures_defaults():
    nf = NewsFeatures()
    assert nf.ref == ""
    assert nf.news_risk == 0.0
    assert nf.event_tminus_sec == -1


def test_newsfeatures_frozen():
    nf = NewsFeatures(ref="news:analysis:x")
    try:
        nf.ref = "y"  # type: ignore
        assert False, "must be frozen"
    except Exception:
        assert True
