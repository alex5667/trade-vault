from utils.time_utils import get_ny_time_millis

"""
"Закрывающий" интеграционный тест:
  TradeClosed dict (как оно реально попадает через closed.__dict__)
    -> StatsAggregator writer helper writes EMA
    -> EdgeCostGate.estimate_slippage_bps reads EMA

Тест фиксирует:
  - единый session_from_ts_ms + timestamp normalization
  - единый key format (writer/reader)
  - поведение fail-open при отсутствии session/ts
"""

import pytest

from domain.time_utils import session_from_ts_ms
from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps
from services.execution_cost_ema import ExecCostEmaConfig, maybe_update_exec_cost_ema_from_closed


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


def test_close_to_gate_roundtrip(monkeypatch):
    monkeypatch.setenv("EXEC_COST_EMA_ENABLED", "1")
    monkeypatch.setenv("EXEC_COST_EMA_MIN_SAMPLES", "1")
    monkeypatch.setenv("EXEC_COST_EMA_DIM_TF", "1")
    monkeypatch.setenv("EXEC_COST_EMA_DIM_KIND", "1")
    monkeypatch.setenv("EXEC_COST_EMA_KEY_PREFIX", "slipema")
    monkeypatch.setenv("EXEC_COST_EMA_WRITE_LEGACY", "1")
    monkeypatch.setenv("EXEC_COST_EMA_READ_LEGACY_FALLBACK", "1")

    r = FakeRedis()
    cfg = ExecCostEmaConfig.from_env()

    entry_ts_ms = 1_700_000_000_000
    ses = session_from_ts_ms(entry_ts_ms)
    assert ses != "na"

    # This is exactly what StatsAggregator gets: closed.__dict__
    trade_closed = {
        "entry_ts_ms": entry_ts_ms,
        "venue": "binance_futures",
        "entry_session": ses,
        "realized_slippage_bps": 18.0,
        "realized_spread_bps": 6.0,
    }

    now_ms = get_ny_time_millis()
    maybe_update_exec_cost_ema_from_closed(
        r,
        cfg=cfg,
        trade_closed=trade_closed,
        strategy="absorption",   # kind
        symbol="BTCUSDT",
        tf="1m",
        now_ms=now_ms,
    )

    ctx = Ctx(
        ts_ms=entry_ts_ms,
        venue="binance_futures",
        tf="1m",
        kind="absorption",
        spread_bps=10.0,  # half=5, default=5; EMA=18 must win
    )
    out = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=entry_ts_ms,
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    assert out == pytest.approx(18.0)
