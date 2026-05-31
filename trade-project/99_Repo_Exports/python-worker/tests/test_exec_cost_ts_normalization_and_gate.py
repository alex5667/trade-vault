import pytest

from domain.time_utils import session_from_ts_ms
from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps
from services.execution_cost_ema import (
    ExecCostEmaConfig,
    build_exec_cost_ema_key,
    update_exec_cost_ema,
)


class FakeRedis:
    def __init__(self):
        self.h = {}

    def hmget(self, key, *fields):
        d = self.h.get(key, {})
        return [d.get(f) for f in fields]

    def hget(self, key, field):
        return self.h.get(key, {}).get(field)

    def hset(self, key, field, value):
        self.h.setdefault(key, {})[field] = value

    def expire(self, key, ttl):
        return True

    def pipeline(self, transaction=False):
        return self

    def execute(self):
        return True


class Ctx:
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


def test_ts_in_seconds_is_normalized_and_ema_is_used(monkeypatch):
    # Make EMA trusted with 1 sample for deterministic test.
    monkeypatch.setenv("EXEC_COST_EMA_ENABLED", "1")
    monkeypatch.setenv("EXEC_COST_EMA_MIN_SAMPLES", "1")
    monkeypatch.setenv("EXEC_COST_EMA_DIM_TF", "1")
    monkeypatch.setenv("EXEC_COST_EMA_DIM_KIND", "1")
    monkeypatch.setenv("EXEC_COST_EMA_KEY_PREFIX", "slipema")
    monkeypatch.setenv("EXEC_COST_EMA_READ_LEGACY_FALLBACK", "1")

    r = FakeRedis()
    cfg = ExecCostEmaConfig.from_env()

    # seconds timestamp (10 digits)
    ts_sec = 1_700_000_000
    ts_ms = ts_sec * 1000
    sess = session_from_ts_ms(ts_ms)
    assert sess != "na"

    key = build_exec_cost_ema_key(cfg, symbol="BTCUSDT", venue="binance_futures", session=sess, tf="1m", kind="absorption")
    update_exec_cost_ema(r, cfg=cfg, key=key, now_ms=ts_ms, realized_slippage_bps=12.0, realized_spread_bps=4.0)

    ctx = Ctx(
        ts_ms=ts_ms,  # use normalized ms for ctx too
        venue="binance_futures",
        tf="1m",
        kind="absorption",
        spread_bps=6.0,  # half=3
    )
    out = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=ts_sec,
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    print(f"Test debug: sess={sess}, key={key}, r.h keys: {list(r.h.keys())}")
    assert out == pytest.approx(12.0)


def test_invalid_ts_disables_ema_and_is_fail_open(monkeypatch):
    monkeypatch.setenv("EXEC_COST_EMA_ENABLED", "1")
    monkeypatch.setenv("EXEC_COST_EMA_MIN_SAMPLES", "1")
    monkeypatch.setenv("EXEC_COST_EMA_KEY_PREFIX", "slipema")

    r = FakeRedis()
    # Even if Redis has a huge EMA, invalid ts must prevent EMA usage.
    r.h["slipema:BTCUSDT:binance_futures:us_main:1m:absorption"] = {
        "samples": "999",
        "ema_slippage_bps": "99.0",
        "ema_spread_bps": "10.0",
        "last_ts_ms": "0",
    }

    ctx = Ctx(
        ts_ms=0,  # invalid
        venue="binance_futures",
        tf="1m",
        kind="absorption",
        spread_bps=8.0,  # half=4
    )
    out = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=0,
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    # Must NOT be 99.0; should be max(default=5, half_spread=4) => 5.
    assert out == pytest.approx(5.0)
