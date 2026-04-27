from utils.time_utils import get_ny_time_millis
import time
from types import SimpleNamespace

from handlers.crypto_orderflow.utils.pre_publish_gates import RegimeSessionGate


class FakeRedis:
    """
    Minimal Redis with:
      - get/set with nx/px semantics (cooldown)
      - hash storage for EMA reads (hgetall)
    """

    def __init__(self):
        self.data = {}
        self.hashes = {}
        self.now = get_ny_time_millis()  # ms

    def get(self, key):
        v = self.data.get(key)
        if v is None:
            return None
        if isinstance(v, dict) and "expire" in v:
            if self.now >= v["expire"]:
                del self.data[key]
                return None
            return v["value"]
        return v

    def set(self, key, value, px=None, nx=False):
        if nx and key in self.data:
            return None
        expire = None
        if px is not None:
            expire = self.now + px
        self.data[key] = {"value": str(value), "expire": expire} if expire else str(value)
        return True

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def hset(self, key, mapping=None, **kwargs):
        if key not in self.hashes:
            self.hashes[key] = {}
        if mapping:
            self.hashes[key].update(mapping)
        self.hashes[key].update(kwargs)
        return len(self.hashes[key])


def _ctx(r):
    return SimpleNamespace(
        redis=r,
        ts_ms=1700000000000,
        symbol="BTCUSDT",
        venue="binance_futures",
        session="us_main",
        tf="1m",
        depth_bid_5=9999.0,
        depth_ask_5=9999.0,
        depth_bid_20=250.0,
        depth_ask_20=250.0,
    )


def test_rs_depth20_default_profile_power1_ok(monkeypatch):
    monkeypatch.delenv("GATE_PROFILE", raising=False)
    monkeypatch.delenv("GATES_STRICT", raising=False)
    monkeypatch.setenv("RS_GATE_ENABLED", "1")  # enable gate
    monkeypatch.setenv("RS_DEPTH_MIN", "0")
    monkeypatch.setenv("RS_DEPTH20_MIN_DEFAULT", "100")  # use DEFAULT suffix
    monkeypatch.delenv("RS_DRIFT_POWER", raising=False)  # default by profile

    r = FakeRedis()
    # drift active factor=2.0
    r.hset("drift:active:v1:BTCUSDT:binance_futures:us_main:1m", mapping={"factor": "2.0", "score": "3.0", "feature": "spread_bps"})
    g = RegimeSessionGate.from_env()
    d = g.evaluate(ctx=_ctx(r), symbol="BTCUSDT", kind="breakout")
    assert d.veto is False


def test_rs_depth20_strict_profile_power2_veto(monkeypatch):
    monkeypatch.setenv("GATE_PROFILE", "strict")
    monkeypatch.setenv("RS_GATE_ENABLED", "1")  # enable gate
    monkeypatch.setenv("RS_DEPTH_MIN", "0")
    monkeypatch.setenv("RS_DEPTH20_MIN_DEFAULT", "100")  # use DEFAULT suffix
    monkeypatch.delenv("RS_DRIFT_POWER", raising=False)  # default by profile (2)

    r = FakeRedis()
    r.hset("drift:active:v1:BTCUSDT:binance_futures:us_main:1m", mapping={"factor": "2.0", "score": "3.0", "feature": "spread_bps"})
    g = RegimeSessionGate.from_env()
    d = g.evaluate(ctx=_ctx(r), symbol="BTCUSDT", kind="breakout")
    assert d.veto is True
    assert d.reason_code == "VETO_RS_DEPTH20"
