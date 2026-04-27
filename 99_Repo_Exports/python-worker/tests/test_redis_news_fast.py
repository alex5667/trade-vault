from core.redis_client import get_redis_fast_news, reset_redis_fast_news


def test_news_fast_pool_timeouts(monkeypatch):
    reset_redis_fast_news()

    monkeypatch.setenv("NEWS_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("NEWS_REDIS_SOCKET_TIMEOUT_MS", "50")
    monkeypatch.setenv("NEWS_REDIS_CONNECT_TIMEOUT_MS", "80")
    monkeypatch.setenv("NEWS_REDIS_MAX_CONNECTIONS", "16")

    r = get_redis_fast_news()
    kw = r.connection_pool.connection_kwargs

    assert float(kw["socket_timeout"]) <= 0.1
    assert float(kw["socket_connect_timeout"]) <= 0.2
    assert kw.get("retry_on_timeout") in (False, None)
