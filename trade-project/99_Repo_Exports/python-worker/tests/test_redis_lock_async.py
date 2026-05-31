import asyncio

import pytest


@pytest.mark.asyncio
async def test_redis_lock_async_acquire_release():
    # Use real redis.asyncio if available or a mock
    # For CI/dev we use fakeredis if available
    try:
        import fakeredis.aioredis
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    except ImportError:
        pytest.skip("fakeredis not installed")

    from core.redis_lock_async import acquire_lock, release_lock

    lock = await acquire_lock(r=r, key="lock:test", ttl_sec=10)
    assert lock is not None
    assert await r.get("lock:test") == lock.value

    lock2 = await acquire_lock(r=r, key="lock:test", ttl_sec=10)
    assert lock2 is None

    ok = await release_lock(r=r, lock=lock)
    assert ok is True
    assert await r.get("lock:test") is None


@pytest.mark.asyncio
async def test_redis_lock_async_ttl_expire():
    try:
        import fakeredis.aioredis
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    except ImportError:
        pytest.skip("fakeredis not installed")

    from core.redis_lock_async import acquire_lock

    lock = await acquire_lock(r=r, key="lock:test_ttl", ttl_sec=1)
    assert lock is not None

    await asyncio.sleep(1.1)

    lock2 = await acquire_lock(r=r, key="lock:test_ttl", ttl_sec=1)
    assert lock2 is not None
