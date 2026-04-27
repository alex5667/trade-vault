from __future__ import annotations

from news_pipeline.leader_lock import LeaderLock
from tests.fake_redis import FakeRedis  # type: ignore


def test_leader_lock_acquire_and_renew():
    r = FakeRedis()
    lock = LeaderLock.new(r=r, key="k", ttl_sec=1.0, prefix="t")
    assert lock.try_acquire() is True
    assert lock.renew() is True
    # another lock cannot acquire
    lock2 = LeaderLock.new(r=r, key="k", ttl_sec=1.0, prefix="t2")
    assert lock2.try_acquire() is False
