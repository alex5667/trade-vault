"""Shared guard for detecting `redis.asyncio.Redis` instances on sync paths.

Sync code that grabs a redis client off ``ctx`` (e.g. ``ctx.redis`` /
``ctx.redis_client``) must check whether the client is actually async before
calling methods like ``hgetall``/``xadd``/``get`` on it — otherwise the call
returns an un-awaited coroutine, the surrounding ``try/except`` swallows the
``TypeError`` from the next line, and Python emits a
``RuntimeWarning: coroutine 'Redis.execute_command' was never awaited``.

`inspect.iscoroutinefunction(rc.get)` is unreliable: redis-py's async client
methods are not declared with ``async def`` at the class level — they are
wrapped, so the inspect check returns ``False`` even for async clients.
Module-name check is the canonical fix.
"""

from __future__ import annotations

from typing import Any


def is_async_redis_client(rc: Any) -> bool:
    """Return True iff ``rc`` is a redis.asyncio.* (or aioredis) client.

    Fail-safe: any introspection error returns False (treat as sync).
    """
    if rc is None:
        return False
    try:
        mod = type(rc).__module__ or ""
        return "asyncio" in mod or "aioredis" in mod
    except Exception:
        return False


def sync_or_none(rc: Any) -> Any:
    """Return ``rc`` if it is a sync client, else ``None``.

    Convenience wrapper for the common pattern:

        rc = getattr(ctx, "redis", None)
        if is_async_redis_client(rc):
            rc = None
    """
    return None if is_async_redis_client(rc) else rc
