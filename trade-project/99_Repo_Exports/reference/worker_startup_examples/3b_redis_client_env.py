"""core.redis_client

This file is a drop-in replacement for your existing redis_client.py.

It keeps the existing behavior of get_redis() (long timeouts) for heavy/background
work and adds a *separate* fast client for the tick-loop news enrichment:

- get_redis_fast_news(): uses its own connection pool
  * socket_timeout ~50ms (configurable)
  * socket_connect_timeout ~200ms (configurable)
  * retry_on_timeout=False

Important: we DO NOT change the existing get_redis() timeouts, because other
parts of the system intentionally rely on long timeouts while Redis is loading
large datasets.

Recommended usage
-----------------
Use get_redis_fast_news() ONLY for:
- reading small HASHes (news:agg:*, calendar:agg:*)
- watchdog heartbeats

Do NOT use it for:
- large scans
- long blocking ops
- XREADGROUP loops
"""

import os
import sys
import time
from threading import Lock
from typing import Optional, Any

# redis-py is an optional dependency in unit-test environments. Importing it
# unconditionally breaks tests that do not require Redis connectivity.
try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


def get_env(key, default_value):
    """Get environment variable value or default."""
    return os.environ.get(key, default_value)


# ---------------------------------------------------------------------------
# Main Redis client (existing behavior) - long timeouts
# ---------------------------------------------------------------------------

_redis_client: Optional[Any] = None
_redis_lock = Lock()
_connection_pool: Optional[Any] = None


def get_redis(retry_attempts=10, retry_delay=2):
    """Create/reuse main Redis client.

    This is intentionally configured with *long* timeouts for heavy loads / when
    Redis is BusyLoading (large AOF/RDB). Keep this behavior.
    """

    if redis is None:
        raise RuntimeError("redis package is not installed. Install it to enable Redis connectivity: pip install redis")

    global _redis_client, _connection_pool

    # Fast path: reuse working singleton
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

        current_delay = retry_delay
        for attempt in range(retry_attempts):
            try:
                if _connection_pool is None:
                    _connection_pool = redis.ConnectionPool(
                        host=redis_host,
                        port=redis_port,
                        db=0,
                        socket_timeout=120,
                        socket_connect_timeout=30,
                        max_connections=int(get_env("REDIS_MAX_CONNECTIONS", "20")),  # P2: было 100
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
                    print(f"⚠️ Redis BusyLoading (attempt {attempt+1}/{retry_attempts}): {e}")
                    print(f"⏳ retry in {current_delay} sec...")
                    sys.stdout.flush()
                    time.sleep(current_delay)
                    current_delay = min(current_delay * 1.5, 30.0)
                    _connection_pool = None
                else:
                    print(f"❌ Redis still BusyLoading after retries: {e}")
                    sys.stdout.flush()
                    raise

            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                if attempt < retry_attempts - 1:
                    print(f"⚠️ Redis connect error (attempt {attempt+1}/{retry_attempts}): {e}")
                    print(f"⏳ retry in {current_delay} sec...")
                    sys.stdout.flush()
                    time.sleep(current_delay)
                    current_delay = min(current_delay * 1.2, 10.0)
                    _connection_pool = None
                else:
                    print(f"❌ Redis connect failed after retries: {e}")
                    sys.stdout.flush()
                    raise

            except Exception as e:
                error_str = str(e)
                if "maximum recursion depth" in error_str.lower() or "recursion" in error_str.lower():
                    print(f"❌ recursion detected while connecting Redis: {e}")
                    sys.stdout.flush()
                    raise

                if attempt < retry_attempts - 1:
                    print(f"⚠️ unexpected Redis error (attempt {attempt+1}/{retry_attempts}): {e}")
                    print(f"⏳ retry in {current_delay} sec...")
                    sys.stdout.flush()
                    time.sleep(current_delay)
                    current_delay = min(current_delay * 1.2, 10.0)
                    _connection_pool = None
                else:
                    print(f"❌ unexpected Redis error after retries: {e}")
                    sys.stdout.flush()
                    raise

        raise redis.exceptions.ConnectionError("Failed to connect to Redis after all retries")


def reset_redis_connection():
    """Reset main Redis singleton + pool."""
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


def close_redis_connection():
    reset_redis_connection()


# ---------------------------------------------------------------------------
# Fast Redis client for tick-loop news enrichment (NEW)
# ---------------------------------------------------------------------------

_news_redis_client: Optional[Any] = None
_news_redis_lock = Lock()
_news_connection_pool: Optional[Any] = None


def get_redis_fast_news():
    """Return a Redis client with *tight* timeouts for tick-loop safe reads.

    Defaults are aligned with your stated budgets:
    - connect_timeout: 200ms
    - socket_timeout: 50ms

    Environment overrides (milliseconds):
    - NEWS_REDIS_CONNECT_TIMEOUT_MS (default 200)
    - NEWS_REDIS_SOCKET_TIMEOUT_MS (default 50)
    - NEWS_REDIS_MAX_CONNECTIONS (default 50)

    Uses separate pool/singleton so it doesn't affect the long-timeout client.
    """

    if redis is None:
        raise RuntimeError("redis package is not installed. Install it to enable Redis connectivity: pip install redis")

    global _news_redis_client, _news_connection_pool

    if _news_redis_client is not None:
        return _news_redis_client

    with _news_redis_lock:
        if _news_redis_client is not None:
            return _news_redis_client

        redis_url = get_env("REDIS_URL", "redis://redis-worker-1:6379/0")

        connect_timeout_ms = int(get_env("NEWS_REDIS_CONNECT_TIMEOUT_MS", "200"))
        socket_timeout_ms = int(get_env("NEWS_REDIS_SOCKET_TIMEOUT_MS", "50"))
        max_conn = int(get_env("NEWS_REDIS_MAX_CONNECTIONS", "50"))

        # Defensive clamps
        connect_timeout_s = max(0.01, min(connect_timeout_ms / 1000.0, 2.0))
        socket_timeout_s = max(0.005, min(socket_timeout_ms / 1000.0, 2.0))
        max_conn = max(5, min(max_conn, 200))

        _news_connection_pool = redis.ConnectionPool.from_url(
            redis_url,
            socket_timeout=socket_timeout_s,
            socket_connect_timeout=connect_timeout_s,
            max_connections=max_conn,
            socket_keepalive=True,
            decode_responses=True,
            health_check_interval=30,
            retry_on_timeout=False,
        )

        _news_redis_client = redis.Redis(connection_pool=_news_connection_pool)
        return _news_redis_client


def reset_redis_fast_news():
    """Reset fast-news Redis singleton + pool."""
    global _news_redis_client, _news_connection_pool
    with _news_redis_lock:
        if _news_redis_client is not None:
            try:
                _news_redis_client.close()
            except Exception:
                pass
            _news_redis_client = None

        if _news_connection_pool is not None:
            try:
                _news_connection_pool.disconnect()
            except Exception:
                pass
            _news_connection_pool = None


async def wait_for_redis_async(client, max_retries: int = 30, delay: float = 10.0) -> bool:
    """
    Pings Redis and waits if BusyLoadingError is raised.
    Returns True if Redis is ready, False if still loading after max_retries.
    """
    import asyncio
    import redis.exceptions
    for attempt in range(max_retries):
        try:
            await client.ping()
            return True
        except redis.exceptions.BusyLoadingError:
            print(f"⚠️ Redis is loading dataset (attempt {attempt+1}/{max_retries}). Waiting {delay}s...")
            sys.stdout.flush()
            await asyncio.sleep(delay)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            print(f"⚠️ Redis connection error (attempt {attempt+1}/{max_retries}). Waiting {delay}s...")
            sys.stdout.flush()
            await asyncio.sleep(delay)
        except Exception as e:
            print(f"⚠️ Unexpected Redis error while waiting: {e}")
            sys.stdout.flush()
            await asyncio.sleep(delay)
    return False


def wait_for_redis(client, max_retries: int = 30, delay: float = 10.0) -> bool:
    """
    Synchronous version of wait_for_redis.
    """
    import redis.exceptions
    for attempt in range(max_retries):
        try:
            client.ping()
            return True
        except redis.exceptions.BusyLoadingError:
            print(f"⚠️ Redis is loading dataset (attempt {attempt+1}/{max_retries}). Waiting {delay}s...")
            sys.stdout.flush()
            time.sleep(delay)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            print(f"⚠️ Redis connection error (attempt {attempt+1}/{max_retries}). Waiting {delay}s...")
            sys.stdout.flush()
            time.sleep(delay)
        except Exception as e:
            print(f"⚠️ Unexpected Redis error while waiting: {e}")
            sys.stdout.flush()
            time.sleep(delay)
    return False
