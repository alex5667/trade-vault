# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Regression: PositionLeg policy — pure-math invariants (merge-blocker).

Tests:
  - blended_entry_price is qty-weighted average
  - worst_case_loss_usdt boundary cases (zero SL, empty legs, SL on wrong side)
  - max_add_qty_for_budget respects risk budget
  - build_scale_in_tp_schema: TP1 closes new leg, sum(tp_qtys) == total_qty
  - Round-trip PositionLeg serialization

Run:
    cd python-worker && python -m pytest tests/test_position_leg_policy_math.py -v
"""

import math
import pytest
from services.position_leg_policy import (
    PositionLeg,
    blended_entry_price,
    build_scale_in_tp_schema,
    max_add_qty_for_budget,
    worst_case_loss_usdt,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _leg(entry: float, qty: float, side: str = "LONG") -> PositionLeg:
    return PositionLeg(entry=entry, qty=qty, side=side)


# ---------------------------------------------------------------------------
# blended_entry_price
# ---------------------------------------------------------------------------

class TestBlendedEntry:
    def test_single_leg(self) -> None:
        result = blended_entry_price([_leg(100.0, 1.0)])
        assert result == pytest.approx(100.0)

    def test_two_legs_equal_qty(self) -> None:
        legs = [_leg(100.0, 1.0), _leg(200.0, 1.0)]
        assert blended_entry_price(legs) == pytest.approx(150.0)

    def test_two_legs_unequal_qty(self) -> None:
        legs = [_leg(100.0, 3.0), _leg(200.0, 1.0)]
        # (100*3 + 200*1) / 4 = 500/4 = 125
        assert blended_entry_price(legs) == pytest.approx(125.0)

    def test_empty_legs(self) -> None:
        assert blended_entry_price([]) == 0.0

    def test_zero_qty(self) -> None:
        assert blended_entry_price([_leg(100.0, 0.0)]) == 0.0


# ---------------------------------------------------------------------------
# worst_case_loss_usdt
# ---------------------------------------------------------------------------

class TestWorstCaseLoss:
    def test_long_sl_below_entry(self) -> None:
        legs = [_leg(100.0, 2.0, "LONG")]
        # (100 - 90) * 2 = 20
        assert worst_case_loss_usdt(legs, sl=90.0) == pytest.approx(20.0)

    def test_short_sl_above_entry(self) -> None:
        legs = [_leg(100.0, 2.0, "SHORT")]
        # (110 - 100) * 2 = 20
        assert worst_case_loss_usdt(legs, sl=110.0) == pytest.approx(20.0)

    def test_sl_on_profitable_side_long(self) -> None:
        """SL above entry for LONG → no loss (max(0, negative) = 0)."""
        legs = [_leg(100.0, 2.0, "LONG")]
        assert worst_case_loss_usdt(legs, sl=110.0) == 0.0

    def test_sl_on_profitable_side_short(self) -> None:
        """SL below entry for SHORT → no loss."""
        legs = [_leg(100.0, 2.0, "SHORT")]
        assert worst_case_loss_usdt(legs, sl=90.0) == 0.0

    def test_zero_sl(self) -> None:
        assert worst_case_loss_usdt([_leg(100.0, 1.0)], sl=0.0) == 0.0

    def test_empty_legs(self) -> None:
        assert worst_case_loss_usdt([], sl=90.0) == 0.0

    def test_multi_leg_mixed_profit_loss(self) -> None:
        """Two LONG legs, SL between their entries."""
        legs = [_leg(100.0, 1.0, "LONG"), _leg(80.0, 1.0, "LONG")]
        # SL=85: leg1 loss=(100-85)*1=15, leg2 loss=max(0,(80-85)*1)=0
        assert worst_case_loss_usdt(legs, sl=85.0) == pytest.approx(15.0)


# ---------------------------------------------------------------------------
# max_add_qty_for_budget
# ---------------------------------------------------------------------------

class TestMaxAddQty:
    def test_budget_remaining(self) -> None:
        legs = [_leg(100.0, 1.0, "LONG")]
        # Current WCL at SL=90: (100-90)*1 = 10 USDT
        # budget=50, remaining=40, per_unit=100-90=10 → max_add=4.0
        qty = max_add_qty_for_budget(legs, sl=90.0, budget_usdt=50.0, new_entry=100.0)
        assert qty == pytest.approx(4.0)

    def test_budget_exhausted(self) -> None:
        legs = [_leg(100.0, 5.0, "LONG")]
        # Current WCL at SL=90: (100-90)*5 = 50
        qty = max_add_qty_for_budget(legs, sl=90.0, budget_usdt=50.0)
        assert qty == 0.0

    def test_zero_budget(self) -> None:
        qty = max_add_qty_for_budget([_leg(100.0, 1.0)], sl=90.0, budget_usdt=0.0)
        assert qty == 0.0

    def test_empty_legs(self) -> None:
        # No existing legs, full budget available, needs new_entry
        qty = max_add_qty_for_budget([], sl=90.0, budget_usdt=100.0, new_entry=100.0)
        # per_unit = 100-90 = 10, max_qty = 100/10 = 10
        assert qty == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# build_scale_in_tp_schema
# ---------------------------------------------------------------------------

class TestBuildScaleInTPSchema:
    def test_tp1_closes_new_leg(self) -> None:
        existing = [_leg(100.0, 1.0, "LONG")]
        tp_prices_in = [105.0, 110.0, 115.0]
        tp_prices, tp_qtys, trail_tp = build_scale_in_tp_schema(
            existing, new_qty=0.5, tp_prices=tp_prices_in,
        )
        # TP1 qty should be the new leg qty
        assert tp_qtys[0] == pytest.approx(0.5)
        # trail_activate_tp_level = 1
        assert trail_tp == 1

    def test_sum_qtys_equals_total(self) -> None:
        existing = [_leg(100.0, 1.0, "LONG")]
        tp_prices_in = [105.0, 110.0]
        tp_prices, tp_qtys, _ = build_scale_in_tp_schema(
            existing, new_qty=0.5, tp_prices=tp_prices_in,
        )
        assert sum(tp_qtys) == pytest.approx(1.5)  # 1.0 + 0.5

    def test_single_tp_level(self) -> None:
        existing = [_leg(100.0, 1.0)]
        tp_prices, tp_qtys, _ = build_scale_in_tp_schema(
            existing, new_qty=0.5, tp_prices=[110.0],
        )
        # Single TP gets everything
        assert len(tp_qtys) == 1
        assert tp_qtys[0] == pytest.approx(1.5)

    def test_empty_tp_prices(self) -> None:
        tp_prices, tp_qtys, trail_tp = build_scale_in_tp_schema(
            [_leg(100.0, 1.0)], new_qty=0.5, tp_prices=[],
        )
        assert tp_prices == []
        assert tp_qtys == []

    def test_remaining_evenly_distributed(self) -> None:
        existing = [_leg(100.0, 1.0)]
        tp_prices_in = [105.0, 110.0, 115.0]
        _, tp_qtys, _ = build_scale_in_tp_schema(
            existing, new_qty=0.5, tp_prices=tp_prices_in,
        )
        # TP1=0.5, remaining=1.0, TP2=0.5, TP3=0.5
        assert tp_qtys[0] == pytest.approx(0.5)
        assert tp_qtys[1] == pytest.approx(0.5)
        assert tp_qtys[2] == pytest.approx(0.5)

    def test_no_dust_from_rounding(self) -> None:
        """Last TP level absorbs remainder to avoid dust."""
        existing = [_leg(100.0, 1.0)]
        tp_prices_in = [105.0, 110.0, 115.0, 120.0]
        _, tp_qtys, _ = build_scale_in_tp_schema(
            existing, new_qty=0.3, tp_prices=tp_prices_in,
        )
        assert sum(tp_qtys) == pytest.approx(1.3)


# ---------------------------------------------------------------------------
# PositionLeg serialization round-trip
# ---------------------------------------------------------------------------

class TestPositionLegSerialization:
    def test_roundtrip(self) -> None:
        leg = PositionLeg(entry=42123.45, qty=0.7, side="SHORT", signal_id="sig-42", ts_ms=170000, seq=2)
        d = leg.to_dict()
        restored = PositionLeg.from_dict(d)
        assert restored.entry == leg.entry
        assert restored.qty == leg.qty
        assert restored.side == leg.side
        assert restored.signal_id == leg.signal_id
        assert restored.ts_ms == leg.ts_ms
        assert restored.seq == leg.seq

    def test_from_dict_missing_fields(self) -> None:
        """from_dict should set safe defaults for missing keys."""
        leg = PositionLeg.from_dict({})
        assert leg.entry == 0.0
        assert leg.qty == 0.0
        assert leg.side == "LONG"
        assert leg.signal_id == ""
