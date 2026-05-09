
from core.redis_client import get_redis_fast_news, reset_redis_connection


def test_get_redis_fast_news_reads_env(monkeypatch):
    # Avoid touching a real Redis; we just verify the pool is created with
    # expected timeouts. The client won't connect until used.
    monkeypatch.setenv("NEWS_REDIS_HOST", "example")
    monkeypatch.setenv("NEWS_REDIS_PORT", "6379")
    monkeypatch.setenv("NEWS_REDIS_SOCKET_TIMEOUT_SEC", "0.05")
    monkeypatch.setenv("NEWS_REDIS_CONNECT_TIMEOUT_SEC", "0.2")

    client = get_redis_fast_news()
    pool = client.connection_pool
    assert float(pool.connection_kwargs.get("socket_timeout")) == 0.05
    assert float(pool.connection_kwargs.get("socket_connect_timeout")) == 0.2


def teardown_module():
    # Ensure globals are reset between test runs
    reset_redis_connection()
