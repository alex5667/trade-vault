"""python-worker/core/redis_client.py

This module provides Redis clients for different latency budgets.

Why two clients:
- get_redis(): used by background workers / bulk operations where longer
  operations are acceptable (historical reads, large streams, etc.).
  It keeps the existing generous timeouts.
- get_redis_fast_news(): used inside (or close to) the tick-loop path for
  news/calendar feature reads. It enforces a strict upper bound on blocking
  (socket_timeout) and must fail-open quickly.

Important:
- The fast client uses a separate ConnectionPool so it does not inherit
  the 120s timeout.
- retry_on_timeout is disabled by default to keep the upper bound.

Environment (fast client):
- NEWS_REDIS_URL (optional): if set, used as single URL.
- NEWS_REDIS_HOST, NEWS_REDIS_PORT (fallback)
- NEWS_REDIS_DB (default 0)
- NEWS_REDIS_SOCKET_TIMEOUT_SEC (default 0.05)   # 50ms
- NEWS_REDIS_CONNECT_TIMEOUT_SEC (default 0.2)   # 200ms
- NEWS_REDIS_MAX_CONNECTIONS (default 20)

"""

from __future__ import annotations

import os
import sys
import time
from threading import Lock
from typing import Optional

import redis  # type: ignore


def get_env(key: str, default_value: str) -> str:
    """Get env var or default."""
    return os.environ.get(key, default_value)


# ---------------------------------------------------------------------------
# DEFAULT (existing) client: tolerant timeouts for heavy/bulk workers
# ---------------------------------------------------------------------------

_redis_client: Optional[redis.Redis] = None
_redis_lock = Lock()
_connection_pool: Optional[redis.ConnectionPool] = None


def get_redis(retry_attempts: int = 10, retry_delay: float = 2.0) -> redis.Redis:
    """Create and return Redis client (singleton) with BusyLoading retries.

    NOTE: This keeps the existing large timeouts. Do NOT use this client
    in the tick-loop.
    """

    global _redis_client, _connection_pool

    if _redis_client is not None:
        try:
            _redis_client.ping()
            return _redis_client
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, AttributeError):
            _redis_client = None
            _connection_pool = None

    with _redis_lock:
        if _redis_client is not None:
            try:
                _redis_client.ping()
                return _redis_client
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, AttributeError):
                _redis_client = None
                _connection_pool = None

        redis_host = get_env("REDIS_HOST", "redis-worker-1")
        redis_port = int(get_env("REDIS_PORT", "6379"))

        current_delay = float(retry_delay)
        for attempt in range(int(retry_attempts)):
            try:
                if _connection_pool is None:
                    _connection_pool = redis.ConnectionPool(
                        host=redis_host,
                        port=redis_port,
                        db=0,
                        socket_timeout=120,           # heavy workers
                        socket_connect_timeout=30,
                        max_connections=100,
                        socket_keepalive=True,
                        decode_responses=True,
                        health_check_interval=30,
                    )

                client = redis.Redis(connection_pool=_connection_pool)
                client.ping()

                _redis_client = client
                print(f"✅ Redis connection established: {redis_host}:{redis_port}")
                sys.stdout.flush()
                return client

            except redis.exceptions.BusyLoadingError as e:
                if attempt < retry_attempts - 1:
                    print(f"⚠️ Redis is loading dataset ({attempt+1}/{retry_attempts}): {e}")
                    print(f"⏳ Retry in {current_delay:.1f}s...")
                    sys.stdout.flush()
                    time.sleep(current_delay)
                    current_delay = min(current_delay * 1.5, 30.0)
                    _connection_pool = None
                else:
                    raise

            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                if attempt < retry_attempts - 1:
                    print(f"⚠️ Redis connect error ({attempt+1}/{retry_attempts}): {e}")
                    print(f"⏳ Retry in {current_delay:.1f}s...")
                    sys.stdout.flush()
                    time.sleep(current_delay)
                    current_delay = min(current_delay * 1.2, 10.0)
                    _connection_pool = None
                else:
                    raise

            except Exception as e:
                err = str(e)
                if "recursion" in err.lower() or "maximum recursion depth" in err.lower():
                    print(f"❌ Recursion detected in Redis connect: {e}")
                    sys.stdout.flush()
                    raise

                if attempt < retry_attempts - 1:
                    print(f"⚠️ Unexpected Redis connect error ({attempt+1}/{retry_attempts}): {e}")
                    print(f"⏳ Retry in {current_delay:.1f}s...")
                    sys.stdout.flush()
                    time.sleep(current_delay)
                    current_delay = min(current_delay * 1.2, 10.0)
                    _connection_pool = None
                else:
                    raise

    raise redis.exceptions.ConnectionError("Failed to connect to Redis")


def reset_redis_connection() -> None:
    """Reset default Redis connection (heavy client)."""
    global _redis_client, _connection_pool

    with _redis_lock:
        if _redis_client is not None:
            try:
                _redis_client.close()
            except Exception:
                pass
            _redis_client = None

        if _connection_pool is not None:
            try:
                _connection_pool.disconnect()
            except Exception:
                pass
            _connection_pool = None


def close_redis_connection() -> None:
    """Alias for reset_redis_connection."""
    reset_redis_connection()


# ---------------------------------------------------------------------------
# FAST client for tick-loop news/calendar feature reads (strict latency budget)
# ---------------------------------------------------------------------------

_fast_news_lock = Lock()
_fast_news_pool: Optional[redis.ConnectionPool] = None
_fast_news_client: Optional[redis.Redis] = None


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


def get_redis_fast_news() -> redis.Redis:
    """Redis client with tight timeouts for news/calendar reads.

    Target budget (user-confirmed):
    - socket_timeout <= 50ms
    - connect_timeout <= 200ms

    Must be fail-open: if Redis is slow/unreachable, callers should catch
    exceptions and continue without blocking the tick-loop.
    """

    global _fast_news_pool, _fast_news_client

    if _fast_news_client is not None:
        return _fast_news_client

    with _fast_news_lock:
        if _fast_news_client is not None:
            return _fast_news_client

        url = os.getenv("NEWS_REDIS_URL", "").strip()
        host = os.getenv("NEWS_REDIS_HOST", os.getenv("REDIS_HOST", "redis-worker-1")).strip() or "redis-worker-1"
        port = int(os.getenv("NEWS_REDIS_PORT", os.getenv("REDIS_PORT", "6379")) or "6379")
        db = _int_env("NEWS_REDIS_DB", 0)

        socket_timeout = _float_env("NEWS_REDIS_SOCKET_TIMEOUT_SEC", 0.05)
        connect_timeout = _float_env("NEWS_REDIS_CONNECT_TIMEOUT_SEC", 0.2)
        max_conns = _int_env("NEWS_REDIS_MAX_CONNECTIONS", 20)

        # Separate pool => independent timeouts from the heavy client.
        if url:
            _fast_news_pool = redis.ConnectionPool.from_url(
                url,
                db=db,
                socket_timeout=socket_timeout,
                socket_connect_timeout=connect_timeout,
                max_connections=max_conns,
                decode_responses=True,
                retry_on_timeout=False,
                socket_keepalive=True,
                health_check_interval=30,
            )
        else:
            _fast_news_pool = redis.ConnectionPool(
                host=host,
                port=port,
                db=db,
                socket_timeout=socket_timeout,
                socket_connect_timeout=connect_timeout,
                max_connections=max_conns,
                decode_responses=True,
                retry_on_timeout=False,
                socket_keepalive=True,
                health_check_interval=30,
            )

        _fast_news_client = redis.Redis(connection_pool=_fast_news_pool)
        return _fast_news_client
