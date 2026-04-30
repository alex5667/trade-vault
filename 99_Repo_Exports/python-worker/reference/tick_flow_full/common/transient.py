from __future__ import annotations

from typing import Any
import socket

def is_transient_error(e: Exception) -> bool:
    """
    Single source of truth for transient/network/redis availability issues.
    """
    # redis-py exceptions (best-effort import)
    try:
        from redis.exceptions import ConnectionError as RedisConnError
        from redis.exceptions import TimeoutError as RedisTimeoutError
        from redis.exceptions import BusyLoadingError
        from redis.exceptions import ClusterDownError
    except Exception:  # pragma: no cover
        RedisConnError = ()  # type: ignore
        RedisTimeoutError = ()     # type: ignore
        BusyLoadingError = () # type: ignore
        ClusterDownError = () # type: ignore

    # direct type-based checks
    if isinstance(e, (RedisConnError, RedisTimeoutError, BusyLoadingError, ClusterDownError)):  # type: ignore[arg-type]
        return True
    if isinstance(e, (TimeoutError, ConnectionError, OSError, socket.timeout, socket.error)):
        return True

    # message tokens fallback (covers proxies, TLS, etc.)
    s = (str(e) or "").lower()
    tokens = (
        "timeout", "timed out"
        "connection", "connection refused", "connection reset"
        "broken pipe", "reset by peer"
        "eof", "closed", "unreachable"
        "busy loading", "loading the dataset"
        "try again", "temporarily unavailable"
        "readonly", "master is down", "failover", "moved", "ask"
    )
    return any(t in s for t in tokens)