import asyncio
import os

from services.orderflow.microbar_publish import publish_microbar_closed


class FakeRedis:
    def __init__(self):
        self.calls = []

    async def xadd(self, key, payload, maxlen=None, approximate=None):
        self.calls.append(("xadd", key))
        return "0-1"

    async def sadd(self, key, sym):
        self.calls.append(("sadd", key, sym))
        return 1

    async def expire(self, key, ttl):
        self.calls.append(("expire", key, ttl))
        return True


def test_publish_microbar_closed_split(monkeypatch):
    monkeypatch.setenv("MICROBAR_SPLIT_STREAMS_ENABLE", "1")
    monkeypatch.setenv("MICROBAR_SPLIT_DUAL_WRITE", "0")
    r = FakeRedis()
    asyncio.run(publish_microbar_closed(r, symbol="BTCUSDT", payload_obj={"ts_ms": "1"}))
    keys = [c[1] for c in r.calls if c[0] == "xadd"]
    assert any(k.endswith("events:microbar_closed:BTCUSDT") for k in keys)
















