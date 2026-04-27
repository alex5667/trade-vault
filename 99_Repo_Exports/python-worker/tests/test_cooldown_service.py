import pytest

from handlers.cooldown_service import CooldownService


class DummyRedis:
    """
    Minimal Redis stub supporting set(nx, px) and exists with TTL emulation.
    """
    def __init__(self):
        self.store = {}  # key -> (value, expires_at_ms)

    def set(self, key, value, nx=False, px=None):
        now = getattr(self, '_now', 0)
        # expire old
        if key in self.store and self.store[key][1] <= now:
            self.store.pop(key, None)

        if nx and key in self.store:
            return None
        exp = now + int(px or 0) if px else now + 10**12
        self.store[key] = (value, exp)
        return True

    def exists(self, key):
        now = getattr(self, '_now', 0)
        if key in self.store and self.store[key][1] <= now:
            self.store.pop(key, None)
        return 1 if key in self.store else 0


@pytest.fixture
def clock():
    return {"t": 1_000_000}


def test_memory_acquire_blocks_until_expire(clock):
    cd = CooldownService(symbol="BTCUSDT", redis_client=None)
    # BREAKOUT -> 30_000ms (проверяем lowercasing)
    assert cd.acquire(kind="BREAKOUT", level_key="PIVOT:1", ts_ms=clock["t"]) is True
    assert cd.acquire(kind="breakout", level_key="PIVOT:1", ts_ms=clock["t"] + 1) is False
    # after 30s
    assert cd.acquire(kind="breakout", level_key="PIVOT:1", ts_ms=clock["t"] + 30_000) is True


def test_memory_acquire_with_family_timeframe():
    cd = CooldownService(symbol="BTCUSDT", redis_client=None)
    # Разные family/tf должны иметь разные cooldown
    assert cd.acquire(kind="breakout", level_key="R1", family="crypto", timeframe_s=60, ts_ms=1000) is True
    assert cd.acquire(kind="breakout", level_key="R1", family="xau", timeframe_s=60, ts_ms=1000) is True  # разные family
    assert cd.acquire(kind="breakout", level_key="R1", family="crypto", timeframe_s=300, ts_ms=1000) is True  # разные tf

    # Но одинаковые параметры должны блокироваться
    assert cd.acquire(kind="breakout", level_key="R1", family="crypto", timeframe_s=60, ts_ms=1001) is False


def test_redis_atomic_acquire(clock):
    r = DummyRedis()
    r._now = clock["t"]
    cd1 = CooldownService(symbol="BTCUSDT", redis_client=r)
    cd2 = CooldownService(symbol="BTCUSDT", redis_client=r)

    # first worker acquires
    assert cd1.acquire(kind="breakout", level_key="L1", ts_ms=clock["t"]) is True
    # second worker should fail immediately (same key, ttl active)
    assert cd2.acquire(kind="breakout", level_key="L1", ts_ms=clock["t"] + 5) is False

    # move time forward beyond cooldown
    clock["t"] += 30_001
    r._now = clock["t"]
    assert cd2.acquire(kind="breakout", level_key="L1", ts_ms=clock["t"]) is True
