from __future__ import annotations

import os
import types

import pytest


def test_make_fast_redis_from_env_config(monkeypatch):
    # Arrange
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("NEWS_REDIS_SOCKET_TIMEOUT_MS", "50")
    monkeypatch.setenv("NEWS_REDIS_CONNECT_TIMEOUT_MS", "200")
    monkeypatch.setenv("NEWS_REDIS_MAX_CONNECTIONS", "16")

    from news_pipeline.redis_fast import make_fast_redis_from_env

    # Act
    r = make_fast_redis_from_env()

    # Assert - check connection pool parameters
    pool_kwargs = r.connection_pool.connection_kwargs
    assert pool_kwargs["decode_responses"] is True
    assert pool_kwargs["retry_on_timeout"] is False
    # ms -> seconds
    assert abs(pool_kwargs["socket_timeout"] - 0.05) < 1e-9
    assert abs(pool_kwargs["socket_connect_timeout"] - 0.2) < 1e-9
    assert r.connection_pool.max_connections == 32
