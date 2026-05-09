import asyncio

import pytest


def test_redis_lock_setnx_ex():
    try:
        import fakeredis.aioredis
    except ImportError:
        pytest.skip("fakeredis.aioredis not available", allow_module_level=True)

    from core.redis_lock import release_lock, try_acquire_lock

    async def _async_test():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        k = "lock:test:v1"

        l1 = await try_acquire_lock(r, key=k, ttl_sec=10)
        assert l1 is not None
        l2 = await try_acquire_lock(r, key=k, ttl_sec=10)
        assert l2 is None

        await release_lock(r, l1, key=k)
        l3 = await try_acquire_lock(r, key=k, ttl_sec=10)
        assert l3 is not None

    asyncio.run(_async_test())
