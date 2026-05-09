from __future__ import annotations

import os
from collections import defaultdict
from typing import Any

from services.reliability_calibrator import RelCalConfig, update_reliability_curves


class FakeRedis:
    def __init__(self) -> None:
        self.h: dict[str, dict[str, int]] = defaultdict(dict)
        self.exp: dict[str, int] = {}

    # pipeline emulation
    def pipeline(self, transaction: bool = False) -> FakeRedis:
        return self

    def hincrby(self, key: str, field: str, amount: int) -> int:
        cur = int(self.h[key].get(field, 0) or 0)
        cur += int(amount)
        self.h[key][field] = cur
        return cur

    def hset(self, key: str, field: str, value: Any) -> None:
        # store as string-ish, but for tests ints are enough
        try:
            self.h[key][field] = int(value)
        except Exception:
            self.h[key][field] = 0

    def expire(self, key: str, ttl: int) -> None:
        self.exp[key] = int(ttl)

    def execute(self) -> None:
        return None

    def hgetall(self, key: str) -> dict[str, Any]:
        # edge_cost_gate uses hgetall; keep compatible
        return dict(self.h.get(key) or {})


def _cfg(outcomes: str) -> RelCalConfig:
    os.environ["REL_CAL_ENABLED"] = "1"
    os.environ["REL_CAL_OUTCOMES"] = outcomes
    os.environ["REL_CAL_BUCKET_STEP_PCT"] = "5"
    os.environ["REL_CAL_TTL_SEC"] = "3600"
    return RelCalConfig.from_env()


def test_relcal_tp1_hit() -> None:
    r = FakeRedis()
    cfg = _cfg("tp1")
    pos = {
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "entry_ts_ms": 1700000000000,
        "signal_payload": {"confidence": 42.0, "venue": "binance_futures"},
    }
    closed = {"strategy": "absorption", "symbol": "BTCUSDT", "tf": "1m", "tp1_hit": True, "close_reason": "TP2"}
    update_reliability_curves(r, cfg=cfg, pos=pos, trade_closed=closed, now_ms=1700000001000)
    keys = list(r.h.keys())
    assert keys, "no keys written"
    k = keys[0]
    assert r.h[k]["samples_total"] == 1
    assert r.h[k]["hits_total"] == 1
    assert r.h[k]["b40:n"] == 1  # 42 -> bucket 40 with step=5
    assert r.h[k]["b40:h"] == 1


def test_relcal_tp2_hit_default_compromise() -> None:
    r = FakeRedis()
    cfg = _cfg("tp2")
    pos = {
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "entry_ts_ms": 1700000000000,
        "signal_payload": {"confidence": 87.0, "venue": "binance_futures"},
    }
    closed = {"strategy": "absorption", "symbol": "BTCUSDT", "tf": "1m", "tp2_hit": True, "close_reason": "TP3"}
    update_reliability_curves(r, cfg=cfg, pos=pos, trade_closed=closed, now_ms=1700000001000)
    k = list(r.h.keys())[0]
    assert r.h[k]["samples_total"] == 1
    assert r.h[k]["hits_total"] == 1
    assert r.h[k]["b85:n"] == 1  # 87 -> bucket 85
    assert r.h[k]["b85:h"] == 1


def test_relcal_win_pnl_net_positive() -> None:
    r = FakeRedis()
    cfg = _cfg("win")
    pos = {
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "entry_ts_ms": 1700000000000,
        "signal_payload": {"confidence": 10.0, "venue": "binance_futures"},
    }
    closed = {"strategy": "absorption", "symbol": "BTCUSDT", "tf": "1m", "pnl_net": 1.0, "close_reason": "TP1"}
    update_reliability_curves(r, cfg=cfg, pos=pos, trade_closed=closed, now_ms=1700000001000)
    k = list(r.h.keys())[0]
    assert r.h[k]["hits_total"] == 1
    assert r.h[k]["b10:n"] == 1
    assert r.h[k]["b10:h"] == 1


def test_relcal_nosl_after_tp1() -> None:
    r = FakeRedis()
    cfg = _cfg("nosl_after_tp1")
    pos = {
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "entry_ts_ms": 1700000000000,
        "signal_payload": {"confidence": 55.0, "venue": "binance_futures"},
    }
    # TP1 hit and final close not SL => hit=True
    closed = {"strategy": "absorption", "symbol": "BTCUSDT", "tf": "1m", "tp1_hit": True, "close_reason": "TP2"}
    update_reliability_curves(r, cfg=cfg, pos=pos, trade_closed=closed, now_ms=1700000001000)
    k = list(r.h.keys())[0]
    assert r.h[k]["hits_total"] == 1
    # If SL after TP1 => hit must be False
    r2 = FakeRedis()
    closed2 = {"strategy": "absorption", "symbol": "BTCUSDT", "tf": "1m", "tp1_hit": True, "close_reason": "SL"}
    update_reliability_curves(r2, cfg=cfg, pos=pos, trade_closed=closed2, now_ms=1700000001000)
    k2 = list(r2.h.keys())[0]
    assert r2.h[k2]["hits_total"] == 0


def test_relcal_nosl_after_tp1_strict_horizon_ms() -> None:
    r = FakeRedis()
    cfg = _cfg("nosl_after_tp1_t500")
    pos = {
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "entry_ts_ms": 1700000000000,
        "signal_payload": {"confidence": 55.0, "venue": "binance_futures"},
        # dynamic tp1 hit timestamp (as in your pipeline)
        "tp1_hit_ts_ms": 1700000000500,
        "tp1_hit": True,
    }
    # survived >= 500ms after TP1 and not SL => hit=True
    closed_ok = {
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "tp1_hit": True,
        "tp1_hit_ts_ms": 1700000000500,
        "exit_ts_ms": 1700000001100,  # +600ms
        "close_reason": "TP2",
    }
    update_reliability_curves(r, cfg=cfg, pos=pos, trade_closed=closed_ok, now_ms=1700000001200)
    k = list(r.h.keys())[0]
    assert r.h[k]["hits_total"] == 1

    # if trade ended too early (<T) => conservative miss (hit=False)
    r2 = FakeRedis()
    closed_short = dict(closed_ok)
    closed_short["exit_ts_ms"] = 1700000000900  # +400ms
    update_reliability_curves(r2, cfg=cfg, pos=pos, trade_closed=closed_short, now_ms=1700000001200)
    k2 = list(r2.h.keys())[0]
    assert r2.h[k2]["hits_total"] == 0


def test_relcal_fail_open_missing_confidence() -> None:
    r = FakeRedis()
    cfg = _cfg("tp2")
    pos = {"strategy": "absorption", "symbol": "BTCUSDT", "tf": "1m", "entry_ts_ms": 1700000000000, "signal_payload": {}}
    closed = {"strategy": "absorption", "symbol": "BTCUSDT", "tf": "1m", "tp2_hit": True, "close_reason": "TP3"}
    update_reliability_curves(r, cfg=cfg, pos=pos, trade_closed=closed, now_ms=1700000001000)
    assert not r.h, "should not write anything when confidence is missing"
