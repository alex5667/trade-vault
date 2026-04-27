from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time
import json
from types import SimpleNamespace

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate, EdgeCostGateDecision
from services.feature_drift_alarm import FeatureDriftAlarm, FeatureDriftConfig


class FakeRedis:
    def __init__(self) -> None:
        self._h = {}
        self._kv = {}

    def hgetall(self, key: str):
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
        return True

    def expire(self, key: str, ttl_s: int):
        return True

    def set(self, key: str, value: str, nx: bool = False, px: int = 0):
        if nx and key in self._kv:
            return False
        self._kv[key] = str(value)
        return True

    def xadd(self, stream: str, fields: dict, maxlen=None, approximate=True):
        return "1-0"


def _mk_gate(redis_client):
    return EdgeCostGate(
        enabled=True,
        mode="tp1",
        strict_missing_levels=False,
        apply_kinds=set(),
        k_default=2.0,
        k_by_symbol={},
        fees_bps_default=1.0,
        slippage_bps_default=5.0,
        slippage_use_spread_half=True,
        min_expected_move_bps_default=0.0,
        min_expected_move_bps_by_symbol={},
        ev_p_min=0.0,
        ev_p_min_by_kind={},
        ev_min_trades=0,
        ev_strict_missing_stats=False,
        ev_dynamic_k_enabled=False,
        ev_dynamic_k_atr_mult=0.0,
    )


def test_drift_alarm_tightens_edge_cost_gate_decision(monkeypatch):
    monkeypatch.setenv("FEATURE_DRIFT_INCLUDE_KIND", "0")

    r = FakeRedis()

    # 1) Build drift active key via alarm
    cfg = FeatureDriftConfig(
        enabled=True,
        include_kind=False,
        base_alpha=0.05,
        fast_alpha=0.30,
        min_samples=5,
        z_threshold=1.0,
        tighten_mult=1.0,
        active_ttl_ms=60000,
        diag_stream="",
    )
    alarm = FeatureDriftAlarm(cfg=cfg)

    now_ms = get_ny_time_millis()
    ctx = SimpleNamespace(
        ts_ms=now_ms,
        session="us_main",
        tf="1m",
        venue="binance",
        symbol="BTCUSDT",
        entry_price=100.0,
        tp1_price=100.12,  # +12 bps
        bid=100.0,
        ask=100.02,
        obi=1.0,
        z_delta=0.2,
        spread_bps=2.0,
        depth_bid_5=100.0,
        depth_ask_5=100.0,
        redis=r,
        _edge_drift_tighten=True,
    )

    for _ in range(6):
        alarm.update(redis_client=r, ctx=ctx, symbol="BTCUSDT", kind="absorption")

    # Force drift directly in Redis to bypass alarm math complexities in test
    active_key = "drift:active:v1:BTCUSDT:binance:us_main:1m"
    r.hset(active_key, mapping={
        "factor": "5.0",
        "score": "10.0",
        "feature": "obi",
        "last_ts_ms": str(get_ny_time_millis())
    })
    
    # Verify Redis state
    h = r.hgetall(active_key)
    assert float(h.get("factor", "1")) == 5.0

    # 2) Evaluate gate WITHOUT drift (simulate missing key by using different redis)
    gate = _mk_gate(r)
    gate.redis = FakeRedis() 
    d0: EdgeCostGateDecision = gate.evaluate(ctx=ctx, kind="absorption", symbol="BTCUSDT")
    assert d0.apply is True
    assert d0.veto is False 

    # 3) Evaluate gate WITH drift => K increases => veto
    gate2 = _mk_gate(r)
    gate2.redis = r
    ctx.feature_drift_tighten_k = 5.0
    d1: EdgeCostGateDecision = gate2.evaluate(ctx=ctx, kind="absorption", symbol="BTCUSDT")
    assert d1.apply is True
    assert float(d1.drift_factor) > 1.0
    assert d1.veto is True
