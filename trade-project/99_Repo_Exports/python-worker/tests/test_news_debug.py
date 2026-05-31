from types import SimpleNamespace

from news_pipeline.news_debug import (
    extract_news_mini,
    resolve_news_ref,
    stable_sample_pct,
)


def test_stable_sample_pct_deterministic():
    assert stable_sample_pct("BTCUSDT", 1700000000000, 1) == stable_sample_pct(
        "BTCUSDT", 1700000000000, 1
    )


def test_stable_sample_pct_bounds():
    assert stable_sample_pct("X", 1, 0) is False
    assert stable_sample_pct("X", 1, 100) is True


def test_resolve_news_ref_uid_only():
    assert resolve_news_ref("abc") == "news:analysis:abc"


def test_resolve_news_ref_full_key_passthrough():
    assert resolve_news_ref("news:analysis:abc") == "news:analysis:abc"


def test_extract_news_mini_missing_news():
    ctx = SimpleNamespace(symbol="BTCUSDT", ts=1, news=None)
    assert extract_news_mini(ctx) is None


def test_extract_news_mini_ok():
    news = SimpleNamespace(news_risk=0.5, event_tminus_sec=12, news_grade_id=3, tags_mask=5)
    ctx = SimpleNamespace(symbol="BTCUSDT", ts=1700000000000, news=news)
    assert extract_news_mini(ctx) == ("BTCUSDT", 1700000000000, 0.5, 12, 3, 5)
