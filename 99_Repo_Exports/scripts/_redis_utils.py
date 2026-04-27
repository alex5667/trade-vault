#!/usr/bin/env python3
"""
Shared Redis utility helpers for scripts/.

Usage:
    from _redis_utils import make_redis_client, make_redis_client_from_env, ping_or_raise
"""
from __future__ import annotations

import logging
import os

import redis

logger = logging.getLogger(__name__)

__all__ = [
    "make_redis_client",
    "make_redis_client_from_env",
    "ping_or_raise",
]


def make_redis_client(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    decode_responses: bool = True,
    socket_timeout: float = 10.0,
    socket_connect_timeout: float = 5.0,
) -> redis.Redis:
    """Create a Redis client (no connection made until first command).

    Args:
        host: Redis host.
        port: Redis port.
        db: Database index.
        decode_responses: Decode byte responses to str when True.
        socket_timeout: Timeout for socket operations (seconds).
        socket_connect_timeout: Timeout for connection attempt (seconds).

    Returns:
        Configured :class:`redis.Redis` instance.
    """
    return redis.Redis(
        host=host,
        port=port,
        db=db,
        decode_responses=decode_responses,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
    )


def make_redis_client_from_env(
    host_env: str = "REDIS_HOST",
    port_env: str = "REDIS_PORT",
    default_host: str = "localhost",
    default_port: int = 6379,
    decode_responses: bool = True,
) -> redis.Redis:
    """Create a Redis client using environment variables.

    Falls back to *default_host*/*default_port* when env vars are absent.

    Args:
        host_env: Name of the env var for the Redis host.
        port_env: Name of the env var for the Redis port.
        default_host: Fallback host.
        default_port: Fallback port.
        decode_responses: Decode byte responses to str when True.

    Returns:
        Configured :class:`redis.Redis` instance.
    """
    host = os.getenv(host_env, default_host)
    port = int(os.getenv(port_env, str(default_port)))
    return make_redis_client(host=host, port=port, decode_responses=decode_responses)


def ping_or_raise(client: redis.Redis, label: str = "Redis") -> None:
    """Ping *client* and raise :class:`redis.ConnectionError` if it fails.

    Args:
        client: Redis client to test.
        label: Human-readable label used in the error message.

    Raises:
        redis.ConnectionError: When the ping fails.
    """
    try:
        client.ping()
    except Exception as exc:
        raise redis.ConnectionError(f"Cannot reach {label}: {exc}") from exc
