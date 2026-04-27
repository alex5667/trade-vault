"""
Closing integration test (in-repo, no real Redis):

OutboxPublisherAdapter.publish(payload: dict)
  -> SignalOutboxPublisher.publish(..., envelope=dict) [Lua contract: XADD field 'data' = envelope_json]
  -> read last stream entry fields {'data': '...json...'}
  -> runners.trade_monitor_runner._parse_signal(fields)  # parses 'data' json
  -> TradeMonitorService._normalize_signal(raw)
  -> domain.handlers.create_position(sig, spec)
  -> domain.handlers.finalize_trade(...) adds execution-quality fields:
       realized_slippage_bps / realized_spread_bps (dynamic attrs)

This test is intentionally "hard-fixed" to catch regressions:
  - field name in outbox stream MUST be exactly 'data'
  - payload must survive roundtrip without losing trail_after_tp1 flags
  - timestamps are treated as epoch ms end-to-end (seconds -> ms normalization in adapter)
  - close-time execution fields must be attached deterministically
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest


class FakeRedisLuaOutbox:
    """
    Minimal Redis stub for SignalOutboxPublisher.publish() Lua path:
    SET dedup_key NX PX ttl
    XADD stream ... 'data' envelope_json
    Also supports script_load/eval/evalsha.
    """
    def __init__(self):
        self.kv = {}
        self.streams = {}  # stream -> list[(id, fields)]
        self._seq = 0
        self._sha = "FAKE_SHA"

    def script_load(self, script: str):
        return self._sha

    def evalsha(self, sha, numkeys, *args):
        return self.eval("lua", numkeys, *args)

    def eval(self, script, numkeys, *args):
        # args = [dedup_key, stream_key, dedup_ttl_ms, maxlen, envelope_json]
        dedup_key = args[0]
        stream = args[1]
        envelope_json = args[4]
        if dedup_key in self.kv:
            return [0]
        self.kv[dedup_key] = "1"
        self._seq += 1
        msg_id = f"{self._seq}-0"
        self.streams.setdefault(stream, []).append((msg_id, {"data": envelope_json}))
        return [1, msg_id]


class _SpecStub:
    """
    Minimal Spec stub for:
      - TradeMonitorService._normalize_signal() defaults (trailing_profile_default)
      - create_position() risk_money()
      - finalize_trade() calculate_fees(), pnl_money()
    """

    trailing_profile_default = "rocket_v1"
    contract_size = 1.0

    def risk_money(self, entry: float, sl: float, lot: float, direction: str) -> float:
        return abs(float(entry) - float(sl)) * float(lot)

    def calculate_fees(self, *, entry_price, exit_price, lot, side, duration_ms) -> float:
        return 0.0

    def pnl_money(self, entry_price: float, price: float, lot: float, direction: str, symbol: str = "") -> float:
        sign = 1.0 if str(direction).upper() == "LONG" else -1.0
        return (float(price) - float(entry_price)) * sign * float(lot)


def _mk_trade_monitor_like():
    from services.trade_monitor import TradeMonitorService

    mon = TradeMonitorService.__new__(TradeMonitorService)
    mon._get_spec = lambda symbol: _SpecStub()
    mon.default_lot = 1.0
    mon.stop_atr_mult = 1.0
    mon.rr_levels = [1.0, 2.0, 3.0]
    mon.logger = SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    return mon


def test_outbox_adapter_to_trade_monitor_to_create_position_and_close_flags(monkeypatch):
    pass  # Remove env setup - not needed for this test

    from core.signal_outbox import OutboxSettings, SignalOutboxPublisher
    from handlers.emitter.outbox_publisher_adapter import OutboxPublisherAdapter
    from runners.trade_monitor_runner import _parse_signal
    from domain.handlers import create_position, finalize_trade

    r = FakeRedisLuaOutbox()

    # Build SignalOutboxPublisher without real redis init.
    pub = SignalOutboxPublisher.__new__(SignalOutboxPublisher)
    pub.redis = r
    pub.settings = OutboxSettings(
        outbox_stream="stream:test:signals:outbox",
        outbox_maxlen=1000,
        dedup_ttl_ms=60_000,
        dedup_bucket_ms=60_000,
    )
    pub._sha = None

    adapter = OutboxPublisherAdapter(
        outbox_publisher=pub,
        default_source="CryptoOrderFlow",
        default_strategy="absorption",
        dedup_bucket_ms=60_000,
        dedup_ttl_ms=60_000,
    )

    # ---- 1) Publish via adapter (dict -> provider.publish(...) -> Lua -> stream field 'data') ----
    payload = {
        # Adapter dims (must survive roundtrip)
        "source": "CryptoOrderFlow",
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "kind": "absorption",
        "level_key": "L1",
        # TradeMonitor normalize inputs
        "timeframe": "1m",
        "entry": 100.0,
        "sl": 99.0,
        "tp_levels": [101.0, 102.0, 103.0],
        # seconds on purpose -> adapter must normalize to epoch ms
        "ts": 1_700_000_000,
        # Conditional trailing policy (must persist -> SignalNorm.payload -> PositionState)
        "trail_after_tp1": 0,
        "trail_after_tp1_reason": "LOW_MOMO",
        # trailing profile params (optional but common)
        "trail_profile": "rocket_v1",
        "trailing_min_lock_r": 0.25,
        # include venue for future slippage EMA writer (not required by this test)
        "venue": "binance_futures",
    }

    msg_id = adapter.publish(payload)
    assert isinstance(msg_id, str) and msg_id

    # Ensure Lua/stream contract: EXACTLY one field named 'data' with envelope JSON.
    items = r.streams.get(pub.settings.outbox_stream) or []
    assert len(items) == 1
    _id, fields = items[0]
    assert "data" in fields
    assert set(fields.keys()) == {"data"}

    # ---- 2) Runner parsing: fields -> raw dict payload ----
    raw = _parse_signal(fields)
    assert isinstance(raw, dict)
    assert raw.get("trail_after_tp1") in (0, "0")
    assert raw.get("trail_after_tp1_reason") == "LOW_MOMO"
    assert raw.get("timeframe") == "1m"
    # adapter must harden ts to epoch ms for downstream
    assert int(raw.get("ts") or 0) == 1_700_000_000_000

    # ---- 3) TradeMonitor normalize -> SignalNorm; create_position copies flags into PositionState ----
    mon = _mk_trade_monitor_like()
    sig = mon._normalize_signal(raw)
    assert sig is not None

    pos = create_position(sig, _SpecStub())
    assert bool(getattr(pos, "trail_after_tp1", True)) is False
    assert str(getattr(pos, "trail_after_tp1_reason", "")) == "LOW_MOMO"
    assert str(getattr(pos, "trail_profile", "")) == "rocket_v1"
    assert float(getattr(pos, "trailing_min_lock_r", 0.0)) == pytest.approx(0.25, rel=1e-9)
    # seconds -> ms normalization must also be reflected in PositionState
    assert int(getattr(pos, "entry_ts_ms", 0)) == 1_700_000_000_000

    # ---- 4) Close -> execution-quality dynamic fields must be attached deterministically ----
    # finalize_trade() reads pos.exit_mid_price and pos.exit_spread_bps (best-effort).
    pos.exit_mid_price = 100.0
    pos.exit_spread_bps = 8.0
    pos.realized_pnl_gross = 0.0

    closed = finalize_trade(
        pos,
        _SpecStub(),
        exit_price=100.2,  # |100.2 - 100.0|/100.0*1e4 = 20 bps
        exit_ts_ms=int(pos.entry_ts_ms) + 10_000,
        close_reason_raw="SL",
        tp_ratios=[0.33, 0.33, 0.34],
    )

    assert float(getattr(closed, "realized_slippage_bps", 0.0)) == pytest.approx(20.0, rel=1e-9)
    assert float(getattr(closed, "realized_spread_bps", 0.0)) == pytest.approx(8.0, rel=1e-9)


def test_outbox_adapter_dedup_prevents_duplicate_stream_entries():
    """
    Same payload twice (same ts bucket) => second publish returns None (dedup),
    stream must contain only one entry.
    """
    from core.signal_outbox import OutboxSettings, SignalOutboxPublisher
    from handlers.emitter.outbox_publisher_adapter import OutboxPublisherAdapter

    r = FakeRedisLuaOutbox()
    pub = SignalOutboxPublisher.__new__(SignalOutboxPublisher)
    pub.redis = r
    pub.settings = OutboxSettings(
        outbox_stream="stream:test:signals:outbox",
        outbox_maxlen=1000,
        dedup_ttl_ms=60_000,
        dedup_bucket_ms=60_000,
    )
    pub._sha = None

    adapter = OutboxPublisherAdapter(
        outbox_publisher=pub,
        default_source="CryptoOrderFlow",
        default_strategy="absorption",
        dedup_bucket_ms=60_000,
        dedup_ttl_ms=60_000,
    )

    payload = {
        "source": "CryptoOrderFlow",
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "kind": "absorption",
        "level_key": "L1",
        "timeframe": "1m",
        "entry": 100.0,
        "sl": 99.0,
        "tp_levels": [101.0, 102.0, 103.0],
        "ts": 1_700_000_000,  # seconds => normalized; same => same dedup bucket
    }

    a = adapter.publish(payload)
    b = adapter.publish(payload)
    assert isinstance(a, str) and a
    assert b is None

    items = r.streams.get(pub.settings.outbox_stream) or []
    assert len(items) == 1
