# -*- coding: utf-8 -*-

import asyncio

from orderflow_services.redis_lock_v1 import acquire_lock, release_lock


class FakeRedis:
    def __init__(self):
        self.kv = {}

    async def set(self, key, val, nx=False, ex=None):
        if nx and key in self.kv:
            return False
        self.kv[key] = val
        return True

    async def eval(self, script, numkeys, key, token):
        if self.kv.get(key) == token:
            del self.kv[key]
            return 1
        return 0


async def _run():
    r = FakeRedis()
    tok1 = await acquire_lock(r, key="lock:k", ttl_sec=10)
    assert tok1
    tok2 = await acquire_lock(r, key="lock:k", ttl_sec=10)
    assert tok2 == ""
    ok = await release_lock(r, key="lock:k", token="bad")
    assert ok is False
    ok = await release_lock(r, key="lock:k", token=tok1)
    assert ok is True


def test_lock_acquire_release():
    asyncio.run(_run())
