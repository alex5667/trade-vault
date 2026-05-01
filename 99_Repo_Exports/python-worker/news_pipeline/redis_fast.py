from __future__ import annotations

import os
from typing import Optional, Union, Dict, Any

import redis


def make_fast_redis(
    config: Optional[Dict[str, Any]] = None,
    *,
    redis_url: str = "",
    socket_timeout_ms: int = 50,
    socket_connect_timeout_ms: int = 200,
    max_connections: int = 16,
    decode_responses: bool = True,
) -> redis.Redis:
    """Create a Redis client intended for *hot loops* (tick processing).

    Key idea: never block the tick loop on Redis for more than a small bounded time.

    Can be called with config dict or individual parameters.

    Defaults:
      socket_timeout=50ms
      connect_timeout=200ms
      max_connections=16
      retry_on_timeout=False,

    NOTE: set health_check_interval to keep connections fresh without long pings.
    """
    if config is not None:
        redis_url = config.get("redis_url", redis_url)
        socket_timeout_ms = config.get("socket_timeout_ms", socket_timeout_ms)
        socket_connect_timeout_ms = config.get("socket_connect_timeout_ms", socket_connect_timeout_ms)
        max_connections = config.get("max_connections", max_connections)

    # Defensive clamps: don't let someone set nonsense.
    socket_timeout_ms = max(10, min(socket_timeout_ms, 2000))
    socket_connect_timeout_ms = max(10, min(socket_connect_timeout_ms, 5000))
    max_connections = max(5, min(max_connections, 200))

    pool = redis.ConnectionPool.from_url(
        redis_url,
        max_connections=max_connections,
        socket_timeout=socket_timeout_ms / 1000.0,
        socket_connect_timeout=socket_connect_timeout_ms / 1000.0,
        retry_on_timeout=False,
        decode_responses=decode_responses,
        health_check_interval=30,
    )
    return redis.Redis(connection_pool=pool)


def load_fast_config() -> dict:
    """Load fast Redis config from environment variables."""
    return {
        "redis_url": os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        "socket_timeout_ms": int(os.getenv("NEWS_REDIS_SOCKET_TIMEOUT_MS", "50")),
        "socket_connect_timeout_ms": int(os.getenv("NEWS_REDIS_CONNECT_TIMEOUT_MS", "200")),
        "max_connections": int(os.getenv("NEWS_REDIS_MAX_CONNECTIONS", "16")),
    }


def make_fast_redis_from_env(*, redis_url: Optional[str] = None) -> redis.Redis:
    redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return make_fast_redis(
        redis_url=redis_url,
        socket_timeout_ms=int(os.getenv("NEWS_REDIS_SOCKET_TIMEOUT_MS", "50")),
        socket_connect_timeout_ms=int(os.getenv("NEWS_REDIS_CONNECT_TIMEOUT_MS", "200")),
        max_connections=int(os.getenv("NEWS_REDIS_MAX_CONNECTIONS", "16")),
        decode_responses=True,
    )
