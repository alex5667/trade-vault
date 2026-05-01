from __future__ import annotations
"""
tests/test_finalize_trade_tp_hit_flags.py
─────────────────────────────────────────
Verify that finalize_trade() propagates tp1_hit/tp2_hit/tp3_hit/tp_hits
from PositionState into TradeClosed.

Bug: prior to the fix, these fields were omitted from the TradeClosed
constructor, so they always defaulted to False/0 — breaking downstream
stats, calibration, and TP-hit analytics across 170K+ trades.
"""

import pytest


class FakeSpec:
    contract_size = 1.0

    def pnl_money(self, entry_price, price, lot, direction, symbol=""):
        sign = 1.0 if str(direction).upper() == "LONG" else -1.0
        return (float(price) - float(entry_price)) * sign * float(lot)


def _make_pos(**overrides):
    from domain.models import PositionState

    defaults = dict(
        id="pos1",
        sid="sid1",
        strategy="CryptoOrderFlow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=1000,
        lot=1.0,
        remaining_qty=0.0,
        sl=90.0,
        tp_levels=[101.0, 102.0, 103.0],
    )
    defaults.update(overrides)
    return PositionState(**defaults)


def test_tp1_hit_propagated():
    """When pos.tp1_hit=True, closed.tp1_hit must be True."""
    import domain.handlers as handlers

    pos = _make_pos(tp1_hit=True, tp_hits=1)
    closed = handlers.finalize_trade(
        pos, FakeSpec(),
        exit_price=101.0, exit_ts_ms=2000,
        close_reason_raw="SL_AFTER_TP1",
        tp_ratios=[0.3, 0.3, 0.4],
    )
    assert closed.tp1_hit is True
    assert closed.tp_hits == 1
    assert closed.tp_before_sl == 1


def test_tp2_hit_propagated():
    """When pos has tp1+tp2 hit, closed must reflect both."""
    import domain.handlers as handlers

    pos = _make_pos(tp1_hit=True, tp2_hit=True, tp_hits=2)
    closed = handlers.finalize_trade(
        pos, FakeSpec(),
        exit_price=102.0, exit_ts_ms=3000,
        close_reason_raw="SL_AFTER_TP2",
        tp_ratios=[0.3, 0.3, 0.4],
    )
    assert closed.tp1_hit is True
    assert closed.tp2_hit is True
    assert closed.tp3_hit is False
    assert closed.tp_hits == 2


def test_tp3_hit_propagated():
    """Full TP cascade: all three levels hit."""
    import domain.handlers as handlers

    pos = _make_pos(tp1_hit=True, tp2_hit=True, tp3_hit=True, tp_hits=3)
    closed = handlers.finalize_trade(
        pos, FakeSpec(),
        exit_price=103.0, exit_ts_ms=4000,
        close_reason_raw="TP3",
        tp_ratios=[0.3, 0.3, 0.4],
    )
    assert closed.tp1_hit is True
    assert closed.tp2_hit is True
    assert closed.tp3_hit is True
    assert closed.tp_hits == 3


def test_no_tp_hit_stays_false():
    """When no TP was hit, flags must remain False/0."""
    import domain.handlers as handlers

    pos = _make_pos()  # defaults: tp1_hit=False, tp_hits=0
    closed = handlers.finalize_trade(
        pos, FakeSpec(),
        exit_price=89.0, exit_ts_ms=5000,
        close_reason_raw="SL",
        tp_ratios=[0.3, 0.3, 0.4],
    )
    assert closed.tp1_hit is False
    assert closed.tp2_hit is False
    assert closed.tp3_hit is False
    assert closed.tp_hits == 0
    assert closed.tp_before_sl == 0
