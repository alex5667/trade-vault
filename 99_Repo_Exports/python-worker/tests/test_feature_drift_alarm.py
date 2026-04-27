from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import time
from types import SimpleNamespace

from services.feature_drift_alarm import FeatureDriftAlarm, FeatureDriftConfig


class FakeRedis:
    def __init__(self) -> None:
        self._h = {}
        self._kv = {}
        self._streams = {}
        self._ttl = {}

    def hgetall(self, key: str):
        # Redis hgetall returns bytes-to-bytes, but our fake will return str-to-str for simplicity
        # as FeatureDriftAlarm handles string conversion.
        return dict(self._h.get(key, {}))

    def hset(self, key: str, mapping=None, **kwargs):
        if mapping is None:
            mapping = {}
        d = dict(self._h.get(key, {}))
        for k, v in dict(mapping).items():
            d[str(k)] = str(v)
        self._h[key] = d
        return 1

    def pexpire(self, key: str, ttl_ms: int):
        self._ttl[key] = int(ttl_ms)
        return True

    def expire(self, key: str, ttl_s: int):
        self._ttl[key] = int(ttl_s * 1000)
        return True

    def xadd(self, stream: str, fields: dict, maxlen=None, approximate=True):
        arr = self._streams.setdefault(stream, [])
        arr.append(dict(fields))
        return f"{len(arr)}-0"


def test_feature_drift_alarm_sets_active_and_emits_alert():
    r = FakeRedis()
    cfg = FeatureDriftConfig(
        enabled=True,
        include_kind=False,
        base_alpha=0.05,
        fast_alpha=0.30,
        z_threshold=1.0,
        tighten_mult=0.5,
        min_samples=5,
        active_ttl_ms=60000,
        diag_stream="stream:test:drift",
    )
    alarm = FeatureDriftAlarm(cfg=cfg)

    now_ms = get_ny_time_millis()
    # FeatureDriftAlarm expects ctx with certain fields or attributes
    ctx = SimpleNamespace(
        ts_ms=now_ms,
        symbol="BTCUSDT",
        venue="binance",
        session="us_main",
        tf="1m",
        kind="absorption",
        obi=1.0,
        z_delta=0.2,
        spread_bps=2.0,
        depth_bid_5=100.0,
        depth_ask_5=100.0,
    )

    # Build baseline with stable values
    for _ in range(6):
        alarm.update(redis_client=r, ctx=ctx, symbol="BTCUSDT", kind="absorption")
        
    state_key = "drift:state:v1:BTCUSDT:binance:us_main:1m"
    st = r.hgetall(state_key)
    assert int(st.get("n", 0)) >= 6

    # Create a sharp drift
    ctx.obi = 10.0
    ctx.z_delta = 5.0

    alarm.update(redis_client=r, ctx=ctx, symbol="BTCUSDT", kind="absorption")
    
    st2 = r.hgetall(state_key)
    assert int(st2.get("active", 0)) == 1
    assert float(st2.get("factor", 1.0)) > 1.0

    active_key = "drift:active:v1:BTCUSDT:binance:us_main:1m"
    h = r.hgetall(active_key)
    assert float(h.get("factor", "1")) > 1.0
    assert float(h.get("score", "0")) >= 1.0

    # Alert event must be in stream
    items = r._streams.get("stream:test:drift", [])
    assert len(items) >= 1
    payload = json.loads(items[-1]["data"])
    assert payload["symbol"] == "BTCUSDT"
    assert payload["active"] == 1
    assert payload["factor"] > 1.0
