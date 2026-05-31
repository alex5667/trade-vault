import os
from unittest.mock import MagicMock, patch


def test_get_news_enricher_disabled():
    """Test that get_news_enricher returns None when disabled."""
    with patch.dict(os.environ, {"NEWS_ENRICHER_ENABLE": "0"}):
        from news_pipeline.enricher_singleton import get_news_enricher
        assert get_news_enricher() is None


def test_get_news_enricher_enabled_singleton():
    """Test that get_news_enricher returns a singleton instance when enabled."""
    with patch.dict(os.environ, {"NEWS_ENRICHER_ENABLE": "1"}):
        with patch("news_pipeline.enricher_singleton._enricher", None):
            with patch("core.redis_client.get_redis_fast_news") as mock_get_redis:
                with patch("news_pipeline.enricher_shadow.NewsEnricherShadow") as mock_enricher_cls:
                    mock_redis = MagicMock()
                    mock_get_redis.return_value = mock_redis

                    mock_enricher = MagicMock()
                    mock_enricher_cls.return_value = mock_enricher

                    from news_pipeline.enricher_singleton import get_news_enricher

                    # First call
                    result1 = get_news_enricher()
                    assert result1 is mock_enricher
                    mock_enricher_cls.assert_called_once_with(redis=mock_redis)
                    mock_enricher.start.assert_called_once()

                    # Second call should return the same instance
                    mock_enricher_cls.reset_mock()
                    mock_enricher.start.reset_mock()
                    result2 = get_news_enricher()
                    assert result2 is mock_enricher
                    mock_enricher_cls.assert_not_called()
                    mock_enricher.start.assert_not_called()


def test_get_news_enricher_fail_open():
    """Test that get_news_enricher fails open on exceptions."""
    with patch.dict(os.environ, {"NEWS_ENRICHER_ENABLE": "1"}):
        with patch("news_pipeline.enricher_singleton._enricher", None):
            with patch("core.redis_client.get_redis_fast_news", side_effect=Exception("Redis down")):
                from news_pipeline.enricher_singleton import get_news_enricher
                assert get_news_enricher() is None
