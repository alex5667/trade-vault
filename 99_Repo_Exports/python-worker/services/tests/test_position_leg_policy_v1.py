from __future__ import annotations
"""Unit tests for position_leg_policy — pure-math scale-in module.

Tests:
1. blended_entry_price — weighted average across legs
2. worst_case_loss_usdt — WCL for LONG and SHORT legs at SL
3. max_add_qty_for_budget — remaining qty within risk budget
4. build_scale_in_tp_schema — TP1 closes second leg, monotonicity, edge cases
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from services.position_leg_policy import (
    PositionLeg,
    blended_entry_price,
    worst_case_loss_usdt,
    max_add_qty_for_budget,
    build_scale_in_tp_schema,
)


# ===========================================================================
# blended_entry_price
# ===========================================================================

class TestBlendedEntryPrice:
    def test_single_leg(self):
        legs = [PositionLeg(entry=100.0, qty=1.0, side="LONG")]
        assert blended_entry_price(legs) == pytest.approx(100.0)

    def test_two_legs_equal_qty(self):
        legs = [
            PositionLeg(entry=100.0, qty=1.0, side="LONG"),
            PositionLeg(entry=110.0, qty=1.0, side="LONG"),
        ]
        assert blended_entry_price(legs) == pytest.approx(105.0)

    def test_two_legs_unequal_qty(self):
        """Qty-weighted: (100*3 + 120*1) / 4 = 105."""
        legs = [
            PositionLeg(entry=100.0, qty=3.0, side="LONG"),
            PositionLeg(entry=120.0, qty=1.0, side="LONG"),
        ]
        assert blended_entry_price(legs) == pytest.approx(105.0)

    def test_empty_legs(self):
        assert blended_entry_price([]) == 0.0

    def test_zero_qty(self):
        legs = [PositionLeg(entry=100.0, qty=0.0, side="LONG")]
        assert blended_entry_price(legs) == 0.0


# ===========================================================================
# worst_case_loss_usdt
# ===========================================================================

class TestWorstCaseLoss:
    def test_long_loss(self):
        """LONG entry=100, SL=95 → loss = (100-95)*1 = 5."""
        legs = [PositionLeg(entry=100.0, qty=1.0, side="LONG")]
        assert worst_case_loss_usdt(legs, sl=95.0) == pytest.approx(5.0)

    def test_short_loss(self):
        """SHORT entry=100, SL=105 → loss = (105-100)*1 = 5."""
        legs = [PositionLeg(entry=100.0, qty=1.0, side="SHORT")]
        assert worst_case_loss_usdt(legs, sl=105.0) == pytest.approx(5.0)

    def test_long_sl_above_entry_no_loss(self):
        """LONG entry=100, SL=110 → no loss (SL above entry)."""
        legs = [PositionLeg(entry=100.0, qty=1.0, side="LONG")]
        assert worst_case_loss_usdt(legs, sl=110.0) == pytest.approx(0.0)

    def test_two_legs_combined_loss(self):
        """Two LONG legs, SL=95 → loss = (100-95)*1 + (105-95)*0.5 = 5 + 5 = 10."""
        legs = [
            PositionLeg(entry=100.0, qty=1.0, side="LONG"),
            PositionLeg(entry=105.0, qty=0.5, side="LONG"),
        ]
        assert worst_case_loss_usdt(legs, sl=95.0) == pytest.approx(10.0)

    def test_empty_legs(self):
        assert worst_case_loss_usdt([], sl=95.0) == 0.0

    def test_zero_sl(self):
        legs = [PositionLeg(entry=100.0, qty=1.0, side="LONG")]
        assert worst_case_loss_usdt(legs, sl=0.0) == 0.0


# ===========================================================================
# max_add_qty_for_budget
# ===========================================================================

class TestMaxAddQty:
    def test_basic_budget(self):
        """Budget=10, existing WCL=5, per-unit loss=5 → max_add = (10-5)/5 = 1.0."""
        legs = [PositionLeg(entry=100.0, qty=1.0, side="LONG")]
        result = max_add_qty_for_budget(legs, sl=95.0, budget_usdt=10.0, new_entry=100.0)
        assert result == pytest.approx(1.0)

    def test_budget_exhausted(self):
        """Budget = existing WCL → can't add anything."""
        legs = [PositionLeg(entry=100.0, qty=1.0, side="LONG")]
        result = max_add_qty_for_budget(legs, sl=95.0, budget_usdt=5.0, new_entry=100.0)
        assert result == pytest.approx(0.0)

    def test_zero_budget(self):
        legs = [PositionLeg(entry=100.0, qty=1.0, side="LONG")]
        assert max_add_qty_for_budget(legs, sl=95.0, budget_usdt=0.0) == 0.0

    def test_no_existing_legs(self):
        """No legs → WCL=0 → full budget available."""
        result = max_add_qty_for_budget([], sl=95.0, budget_usdt=50.0, new_entry=100.0)
        assert result == pytest.approx(10.0)  # 50 / (100-95) = 10


# ===========================================================================
# build_scale_in_tp_schema
# ===========================================================================

class TestBuildScaleInTpSchema:
    def test_tp1_closes_second_leg(self):
        """TP1 qty = new_qty (second leg size)."""
        legs = [PositionLeg(entry=100.0, qty=0.01, side="LONG")]
        tp_prices = [102.0, 104.0, 106.0]
        new_qty = 0.005

        prices, qtys, trail_level = build_scale_in_tp_schema(legs, new_qty, tp_prices)

        assert prices == tp_prices
        assert len(qtys) == 3
        assert qtys[0] == pytest.approx(0.005)  # TP1 = new leg qty
        assert sum(qtys) == pytest.approx(0.015)  # total = existing + new
        assert trail_level == 1

    def test_single_tp(self):
        """Only one TP → gets entire qty."""
        legs = [PositionLeg(entry=100.0, qty=0.01, side="LONG")]
        tp_prices = [105.0]
        new_qty = 0.005

        prices, qtys, trail_level = build_scale_in_tp_schema(legs, new_qty, tp_prices)

        assert qtys == [pytest.approx(0.015)]  # total qty

    def test_empty_tps(self):
        legs = [PositionLeg(entry=100.0, qty=0.01, side="LONG")]
        prices, qtys, trail_level = build_scale_in_tp_schema(legs, 0.005, [])
        assert prices == []
        assert qtys == []
        assert trail_level == 1

    def test_remaining_evenly_distributed(self):
        """TP2 and TP3 get even share of (total - TP1)."""
        legs = [PositionLeg(entry=100.0, qty=0.01, side="LONG")]
        tp_prices = [102.0, 104.0, 106.0]
        new_qty = 0.002

        prices, qtys, trail_level = build_scale_in_tp_schema(legs, new_qty, tp_prices)

        remaining = 0.012 - 0.002  # total - TP1
        assert qtys[1] == pytest.approx(remaining / 2)
        assert qtys[2] == pytest.approx(remaining / 2)

    def test_position_leg_serialization(self):
        """PositionLeg round-trips through to_dict/from_dict."""
        leg = PositionLeg(entry=100.0, qty=0.01, side="LONG", signal_id="s1", ts_ms=123, seq=1)
        d = leg.to_dict()
        restored = PositionLeg.from_dict(d)
        assert restored.entry == leg.entry
        assert restored.qty == leg.qty
        assert restored.side == leg.side
        assert restored.signal_id == leg.signal_id
