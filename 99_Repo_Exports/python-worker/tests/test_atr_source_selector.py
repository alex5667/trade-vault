from utils.time_utils import get_ny_time_millis
import json
import time

from core.atr_source_selector_v2 import ATRSourceSelector


class FakeRedis:
    def __init__(self):
        self._kv = {}
        self._h = {}
        self._sets = {}

    def hgetall(self, k):
        return self._h.get(k, {})

    def get(self, k):
        return self._kv.get(k, None)

    def set(self, k, v, ex=None):
        self._kv[k] = v

    def pipeline(self):
        return self

    def execute(self):
        return True

    # pipeline methods
    def sadd(self, *a, **kw):
        return 1

    def expire(self, *a, **kw):
        return True


def test_selector_picks_freshest_hash(monkeypatch):
    monkeypatch.setenv("ATR_SELECTOR_ENABLE", "1")
    monkeypatch.setenv("ATR_SELECTOR_TFS", "1m,5m")
    r = FakeRedis()
    sel = ATRSourceSelector(r)

    now = get_ny_time_millis()
    r._h["ATR:BTCUSDT:1m"] = {b"atr": b"10", b"ts_ms": str(now - 10_000).encode()}
    r._h["ATR:BTCUSDT:5m"] = {b"atr": b"10", b"ts_ms": str(now - 100_000).encode()}

    c = sel.select("BTCUSDT", px=1000.0)
    assert c is not None
    assert c.tf == "1m"
    # meta written
    meta_raw = r._kv.get("cfg:atr_sel_meta:BTCUSDT")
    assert meta_raw is not None
















