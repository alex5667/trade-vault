import os
import redis

from news_pipeline.redis_fast import load_fast_config, make_fast_redis


def test_fast_redis_client_has_small_timeouts(monkeypatch):
    monkeypatch.setenv("NEWS_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("NEWS_REDIS_SOCKET_TIMEOUT_MS", "50")
    monkeypatch.setenv("NEWS_REDIS_CONNECT_TIMEOUT_MS", "80")
    monkeypatch.setenv("NEWS_REDIS_MAX_CONNECTIONS", "16")
    monkeypatch.setenv("NEWS_REDIS_HEALTHCHECK_SEC", "0")
    monkeypatch.setenv("NEWS_REDIS_RETRY_ON_TIMEOUT", "0")

    r = make_fast_redis(load_fast_config())
    assert isinstance(r, redis.Redis)

    kw = r.connection_pool.connection_kwargs
    assert float(kw.get("socket_timeout")) <= 0.5
    assert float(kw.get("socket_timeout")) <= 0.1  # 50ms => 0.05
    assert float(kw.get("socket_connect_timeout")) <= 1.0
    assert float(kw.get("socket_connect_timeout")) <= 0.2  # 80ms => 0.08

    # retry_on_timeout должен быть false
    assert kw.get("retry_on_timeout") in (False, None)


def test_fast_redis_safety_clamp(monkeypatch):
    # слишком маленькие значения должны "поджаться" до >=10ms
    monkeypatch.setenv("NEWS_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("NEWS_REDIS_SOCKET_TIMEOUT_MS", "1")
    monkeypatch.setenv("NEWS_REDIS_CONNECT_TIMEOUT_MS", "1")

    r = make_fast_redis(load_fast_config())
    kw = r.connection_pool.connection_kwargs
    assert float(kw.get("socket_timeout")) >= 0.01
    assert float(kw.get("socket_connect_timeout")) >= 0.01
