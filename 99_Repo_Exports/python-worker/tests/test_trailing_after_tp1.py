from __future__ import annotations

import os
from dataclasses import dataclass
from types import SimpleNamespace

from domain.handlers import maybe_arm_trailing_after_tp1
from domain.models import PositionState


@dataclass
class DummySpec:
    def pnl_money(self, entry: float, price: float, lot: float, side: str, symbol="") -> float:
        sign = 1.0 if side == "LONG" else -1.0
        return (price - entry) * sign * lot


def test_trailing_armed_when_allowed(monkeypatch):
    monkeypatch.setenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", "0")
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")

    pos = PositionState(
        id="o1",
        sid="s1",
        strategy="CryptoOrderFlow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=1,
        lot=1.0,
        remaining_qty=1.0,
        sl=95.0,
        tp_levels=[101.0, 102.0, 103.0],
        signal_payload={"atr": 1.0, "trailing_tp1_offset_atr": 0.5},
    )
    pos.trail_after_tp1 = True
    pos.trail_after_tp1_reason = "MOMO_OK"

    ev = maybe_arm_trailing_after_tp1(pos, spec=DummySpec(), ts_ms=1000)
    assert ev is not None
    assert pos.trailing_started is True
    assert pos.trailing_active is True


def test_trailing_skipped_when_denied(monkeypatch):
    monkeypatch.setenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", "0")
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")

    pos = PositionState(
        id="o2",
        sid="s2",
        strategy="CryptoOrderFlow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=1,
        lot=1.0,
        remaining_qty=1.0,
        sl=95.0,
        tp_levels=[101.0, 102.0, 103.0],
        signal_payload={},
    )
    pos.trail_after_tp1 = False
    pos.trail_after_tp1_reason = "LOW_MOMO"

    ev = maybe_arm_trailing_after_tp1(pos, spec=DummySpec(), ts_ms=1000)
    assert ev is not None
    assert ev.event_type == "TRAILING_SKIPPED"
    assert pos.trailing_started is False
    assert bool(getattr(pos, "trailing_skipped_after_tp1", False)) is True
