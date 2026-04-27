import os
import json
import math
import inspect
from types import SimpleNamespace

import pytest

import sys
from pathlib import Path

# Add the tests directory to the path
test_root = Path(__file__).resolve().parents[2] / "tests"
if str(test_root) not in sys.path:
    sys.path.insert(0, str(test_root))

from fake_redis import FakeRedis

from domain.time_utils import session_from_ts_ms
from domain.models import Tick, PositionState
from domain.handlers import process_tick
from services.stats_aggregator import StatsAggregator

from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps


class _FakeSpec:
    def pnl_money(self, entry_price: float, price: float, lot: float, direction: str, symbol: str = "") -> float:
        sign = 1.0 if str(direction).upper() == "LONG" else -1.0
        return (float(price) - float(entry_price)) * sign * float(lot)


def _call_process_tick(pos, tick, spec, tp_ratios):
    """
    process_tick signature can drift over time.
    This helper tries the common variants used across your tests.
    """
    fn = process_tick
    try:
        return fn(pos, tick, spec, tp_ratios)
    except TypeError:
        pass
    try:
        return fn(pos=pos, tick=tick, spec=spec, tp_ratios=tp_ratios)
    except TypeError:
        pass
    try:
        return fn(pos, tick, spec, tp_ratios=tp_ratios)
    except TypeError:
        pass
    # last resort: include fill_policy if present
    try:
        return fn(pos, tick, spec, tp_ratios=tp_ratios, fill_policy="level")
    except TypeError:
        return fn(pos, tick, spec, tp_ratios, "level")


def _hget_str(redis, key: str, field: str) -> str:
    h = redis.hgetall(key) or {}
    for k, v in h.items():
        ks = k.decode("utf-8", errors="ignore") if isinstance(k, (bytes, bytearray)) else str(k)
        if ks == field:
            return v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else str(v)
    return ""


def _hget_int(redis, key: str, field: str) -> int:
    s = _hget_str(redis, key, field)
    try:
        return int(float(s))
    except Exception:
        return 0


def test_e2e_process_tick_finalize_trade_update_stats_writes_slipema_and_relcurve(monkeypatch):
    # Trust EMA with a single sample for the test.
    monkeypatch.setenv("SLIPPAGE_EMA_MIN_SAMPLES", "1")
    monkeypatch.setenv("RELIABILITY_TARGETS", "tp1")  # confirmed default
    monkeypatch.setenv("RELIABILITY_BUCKET_STEP", "5")

    r = FakeRedis()

    # Patch trigger_prices to make TP1 deterministic.
    import domain.handlers as dh
    monkeypatch.setattr(dh, "trigger_prices", lambda tick, direction: (101.0, 90.0, 100.5), raising=False)

    # Position: single TP so we definitely close on TP1.
    entry_ts = 1_700_000_000_000
    pos = PositionState(
        id="test_pos_1",
        sid="test_sid_1",
        strategy="breakout",
        source="test",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=entry_ts,
        lot=1.0,
        qty=1.0,
        quantity=1.0,
        remaining_qty=1.0,
        sl=90.0,
        tp_levels=[101.0],
    )
    # Dynamic dims (must flow into TradeClosed after patch).
    setattr(pos, "kind", "breakout")
    setattr(pos, "venue", "binance_futures")
    setattr(pos, "confidence", 57.0)
    setattr(pos, "entry_regime", "trend")
    # Provide signal_payload as a fallback source for finalize_trade.
    setattr(pos, "signal_payload", {"kind": "breakout", "venue": "binance_futures", "confidence": 57.0, "entry_regime": "trend"})

    tick = Tick(symbol="BTCUSDT", ts_ms=entry_ts + 500, price=101.0, bid=100.00, ask=100.02, last=101.0)

    spec = _FakeSpec()
    events, closed = _call_process_tick(pos, tick, spec, tp_ratios=[1.0])

    assert closed is not None

    # Execution-quality fields (already present in your finalize_trade)
    assert float(getattr(closed, "realized_slippage_bps", 0.0) or 0.0) > 0.0
    # New dynamic dims: required for writers
    assert str(getattr(closed, "kind", "")) == "breakout"
    assert str(getattr(closed, "venue", "")) == "binance_futures"
    assert float(getattr(closed, "confidence", 0.0) or 0.0) == 57.0
    assert str(getattr(closed, "entry_regime", "")) == "trend"

    # Run aggregator (TradeMonitor does: StatsAggregator.update_stats(redis, asdict(pos), asdict(closed)))
    from dataclasses import asdict
    StatsAggregator.update_stats(r, asdict(pos), asdict(closed))

    # Call again to simulate retries / duplicate deliveries.
    # Core Lua may dedupe (applied==0), but finally still runs -> writers must NOT double count.
    StatsAggregator.update_stats(r, asdict(pos), asdict(closed))

    # 1) slipema v2 key exists and has n==1 + ema_bps>0 (NOT double-counted)
    sess = str(session_from_ts_ms(entry_ts) or "na").lower()
    k2 = f"slipema:v2:BTCUSDT:binance_futures:{sess}:1m:breakout"
    assert _hget_int(r, k2, "n") == 1
    ema = float(_hget_str(r, k2, "ema_bps") or 0.0)
    assert ema > 0.0

    # 2) reliability curve increments bucket 55 (57 -> 55 with step=5) (NOT double-counted)
    rk = "relcurve:v1:tp1:breakout:BTCUSDT:1m:trend"
    assert _hget_int(r, rk, "n_total_55") == 1
    assert _hget_int(r, rk, "n_hit_55") == 1
