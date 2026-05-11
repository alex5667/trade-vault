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
from typing import Any
import contextlib

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

_redis_client: Any | None = None
_redis_lock = Lock()
_connection_pool: Any | None = None


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
        redis_port = int(get_env("REDIS_PORT", "6379"))  # type: ignore

        import urllib.parse
        redis_url = get_env("REDIS_URL", "")
        # Start with empty credentials; only fall back to env-var defaults when
        # the URL itself carries no auth info.  This prevents accidental
        # AUTH commands being sent to no-auth Redis instances (e.g. redis-ticks).
        redis_user = ""
        redis_pass = ""
        if redis_url.startswith("redis://"):  # type: ignore
            parsed = urllib.parse.urlparse(redis_url)
            if parsed.username:
                redis_user = parsed.username
            if parsed.password:
                redis_pass = parsed.password
            if parsed.hostname and not get_env("REDIS_HOST", ""):
                redis_host = parsed.hostname
            if parsed.port and not get_env("REDIS_PORT", ""):
                redis_port = parsed.port
        # Only apply env-var credential fallbacks when URL carries no auth at all
        if not redis_user and not redis_pass:
            redis_user = get_env("REDIS_USER", get_env("REDIS_WORKER_USERNAME", ""))
            redis_pass = get_env("REDIS_PASS", get_env("GO_WORKER_REDIS_PASS", ""))

        current_delay = retry_delay
        for attempt in range(retry_attempts):
            try:
                if _connection_pool is None:
                    _connection_pool = redis.ConnectionPool(
                        host=redis_host,
                        port=redis_port,
                        db=0,
                        username=redis_user if redis_user else None,
                        password=redis_pass if redis_pass else None,
                        socket_timeout=120,
                        socket_connect_timeout=30,
                        max_connections=int(get_env("REDIS_MAX_CONNECTIONS", "20")),  # P2: было 100  # type: ignore
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
            with contextlib.suppress(Exception):
                _redis_client.close()
            _redis_client = None

        if _connection_pool is not None:
            with contextlib.suppress(Exception):
                _connection_pool.disconnect()
            _connection_pool = None


def close_redis_connection():
    reset_redis_connection()


# ---------------------------------------------------------------------------
# Fast Redis client for tick-loop news enrichment (NEW)
# ---------------------------------------------------------------------------

_news_redis_client: Any | None = None
_news_redis_lock = Lock()
_news_connection_pool: Any | None = None


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

        connect_timeout_ms = int(get_env("NEWS_REDIS_CONNECT_TIMEOUT_MS", "200"))  # type: ignore
        socket_timeout_ms = int(get_env("NEWS_REDIS_SOCKET_TIMEOUT_MS", "50"))  # type: ignore
        max_conn = int(get_env("NEWS_REDIS_MAX_CONNECTIONS", "50"))  # type: ignore

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
            with contextlib.suppress(Exception):
                _news_redis_client.close()
            _news_redis_client = None

        if _news_connection_pool is not None:
            with contextlib.suppress(Exception):
                _news_connection_pool.disconnect()
            _news_connection_pool = None


# ---------------------------------------------------------------------------
# ATR services Redis client – tight pool to prevent connection flood
# ---------------------------------------------------------------------------
# Problem: ATR services (operating charter, enforcement router, policy gate, etc.)
# each create their own redis.Redis.from_url() → unbounded per-instance pool.
# Under multi-symbol workers this causes 3000+ connections to Redis.
#
# Solution: one shared pool (max_connections=5) with tight timeouts for all
# ATR governance services. They do lightweight reads/writes, not blocking ops.
# ---------------------------------------------------------------------------

_atr_redis_client: Any | None = None
_atr_redis_lock = Lock()
_atr_connection_pool: Any | None = None


def get_atr_redis():
    """Return a shared Redis client for ATR governance services.

    Characteristics:
    - max_connections=5 (shared across all ATR service singletons)
    - socket_timeout=1.0s (ATR services should not block the hot path)
    - socket_connect_timeout=1.0s
    - decode_responses=True

    Environment overrides:
    - ATR_REDIS_MAX_CONNECTIONS (default 5)
    - ATR_REDIS_SOCKET_TIMEOUT_MS (default 1000)
    """
    if redis is None:
        raise RuntimeError("redis package is not installed.")

    global _atr_redis_client, _atr_connection_pool

    if _atr_redis_client is not None:
        return _atr_redis_client

    with _atr_redis_lock:
        if _atr_redis_client is not None:
            return _atr_redis_client

        redis_url = get_env("REDIS_URL", "redis://redis-worker-1:6379/0")
        max_conn = max(2, min(int(get_env("ATR_REDIS_MAX_CONNECTIONS", "5")), 20))  # type: ignore
        timeout_ms = int(get_env("ATR_REDIS_SOCKET_TIMEOUT_MS", "1000"))  # type: ignore
        timeout_s = max(0.1, min(timeout_ms / 1000.0, 5.0))

        _atr_connection_pool = redis.ConnectionPool.from_url(
            redis_url,
            max_connections=max_conn,
            socket_timeout=timeout_s,
            socket_connect_timeout=timeout_s,
            socket_keepalive=True,
            decode_responses=True,
            health_check_interval=60,
            retry_on_timeout=False,
        )
        _atr_redis_client = redis.Redis(connection_pool=_atr_connection_pool)
        print(f"✅ ATR Redis shared pool created (max_connections={max_conn}, timeout={timeout_s}s)")
        return _atr_redis_client


def reset_atr_redis():
    """Reset ATR Redis singleton + pool."""
    global _atr_redis_client, _atr_connection_pool
    with _atr_redis_lock:
        if _atr_redis_client is not None:
            with contextlib.suppress(Exception):
                _atr_redis_client.close()
            _atr_redis_client = None
        if _atr_connection_pool is not None:
            with contextlib.suppress(Exception):
                _atr_connection_pool.disconnect()
            _atr_connection_pool = None


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

# ---------------------------------------------------------------------------
# Async Redis clients for crypto_orderflow_service and others
# ---------------------------------------------------------------------------

def normalize_redis_url(url: str) -> str:
    """Normalize redis URL to prevent duplicate pools (e.g. add /0 if missing)."""
    if not url or not isinstance(url, str) or not url.startswith("redis://"):
        return url
    import urllib.parse
    try:
        parsed = urllib.parse.urlparse(url)
        path = parsed.path
        if not path or path == "/":
            path = "/0"

        # netloc contains user:pass@host:port
        return urllib.parse.urlunparse((
            parsed.scheme,
            parsed.netloc,
            path,
            parsed.params,
            parsed.query,
            parsed.fragment
        ))
    except Exception:
        return url


_async_clients: dict[tuple, Any] = {}
_async_lock = Lock()


def get_async_redis_client(
    url: str,
    max_connections: int,
    socket_timeout: float,
    socket_connect_timeout: float,
    decode_responses: bool = True,
    health_check_interval: int = 0
):
    """Return a shared async Redis client based on URL and config config."""
    global _async_clients

    try:
        import redis.asyncio as aioredis
    except ImportError:
        try:
            import aioredis
        except ImportError:
            raise RuntimeError("Neither redis.asyncio nor aioredis are installed.")

    # ✅ Normalize URL to prevent duplicate pools (redis-1 vs redis-1/0)
    url = normalize_redis_url(url or "")

    import asyncio
    try:
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
    except RuntimeError:
        loop_id = 0

    key = (url, max_connections, socket_timeout, socket_connect_timeout, decode_responses, health_check_interval, loop_id)

    if key in _async_clients:
        return _async_clients[key]

    with _async_lock:
        if key not in _async_clients:
            try:
                from redis.asyncio import BlockingConnectionPool
                pool = BlockingConnectionPool.from_url(
                    url,
                    decode_responses=decode_responses,
                    socket_connect_timeout=socket_connect_timeout,
                    socket_timeout=socket_timeout,
                    socket_keepalive=True,
                    health_check_interval=health_check_interval,
                    max_connections=max_connections,
                    retry_on_timeout=False,
                    # P-LATENCY-FIX: was 30s → 2s fail-fast.
                    # With 3660 connections, pool exhaustion made every coroutine
                    # block up to 30 s waiting for a slot → Worker Lag P99 = 1997ms.
                    # 2 s allows the gather() error handler to retry gracefully.
                    timeout=int(os.environ.get("REDIS_POOL_ACQUIRE_TIMEOUT", "2")),
                )
                client = aioredis.Redis(connection_pool=pool)
            except ImportError:
                # Fallback if BlockingConnectionPool is somehow unavailable
                client = aioredis.from_url(
                    url,
                    decode_responses=decode_responses,
                    socket_connect_timeout=socket_connect_timeout,
                    socket_timeout=socket_timeout,
                    socket_keepalive=True,
                    health_check_interval=health_check_interval,
                    max_connections=max_connections,
                    retry_on_timeout=False,
                )
            _async_clients[key] = client
            # Use logger if possible, else print
            msg = f"🔗 [Async] Created Redis client pool (max_conn={max_connections}, health_check={health_check_interval}): {url}"
            if "logging" in sys.modules:
                import logging
                logging.getLogger("core.redis_client").info(msg)
            else:
                print(msg)
            sys.stdout.flush()

        return _async_clients[key]


async def close_all_async_redis_clients():
    """Gracefully close all pooled aioredis instances."""
    global _async_clients
    clients = list(_async_clients.values())
    _async_clients.clear()
    for client in clients:
        with contextlib.suppress(Exception):
            await client.close()

