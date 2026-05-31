# python-worker/tests/test_risk_cfg_cache.py
from common.risk_cfg_cache import resolve_risk_cfg_cached


class R:
    def __init__(self):
        self.n = 0

    def resolve(self, symbol):
        self.n += 1
        return {"X": 1, "SYM": symbol}


def test_resolve_risk_cfg_cached_hits_cache():
    r = R()
    cache = {}
    out1 = resolve_risk_cfg_cached(resolver=r, symbol="BTCUSDT", cache=cache, ttl_sec=0.0)
    out2 = resolve_risk_cfg_cached(resolver=r, symbol="BTCUSDT", cache=cache, ttl_sec=0.0)

    assert r.n == 1
    assert out1["SYM"] == "BTCUSDT"
    assert out2["SYM"] == "BTCUSDT"
