
import unittest
from dataclasses import dataclass
from domain.models import PositionState
from domain.handlers import finalize_trade

# Mock Spec class for PnL calculation
class MockSpec:
    def __init__(self):
        self.contract_size = 1.0
        self.report_min_risk_usd = 1.0
        self.report_fees_risk_mult = 3.0
        self.max_time_back_ms = 0

    def pnl_money(self, entry, exit, lot, direction, symbol=""):
        # Simple linear PnL: (exit - entry) * lot for LONG
        # (entry - exit) * lot for SHORT
        if direction == "LONG":
            return (exit - entry) * lot
        else:
            return (entry - exit) * lot

class TestMFEBucReproduction(unittest.TestCase):
    def test_mfe_pnl_not_calculated(self):
        # 1. Setup PositionState with a known favorable excursion
        pos = PositionState(
            id="test_mfe",
            sid="sid1",
            strategy="strat1",
            source="src1",
            symbol="BTCUSDT",
            tf="1m",
            direction="LONG",
            entry_price=100.0,
            entry_ts_ms=1000,
            lot=1.0,
            remaining_qty=1.0,
            sl=90.0,
            tp_levels=[110.0]
        )
        
        # Simulate that we saw a price of 120.0 (favorable)
        # This gives a theoretical MFE of (120 - 100) * 1 = 20.0
        pos.max_favorable_price = 120.0
        pos.max_price_seen = 120.0
        
        # We exited at 105.0 (profit of 5.0)
        exit_price = 105.0
        
        # 2. Call finalize_trade
        spec = MockSpec()
        closed_trade = finalize_trade(
            pos=pos,
            spec=spec,
            exit_price=exit_price,
            exit_ts_ms=2000,
            close_reason_raw="TP_HIT",
            tp_ratios=[1.0] 
        )
        
        # 3. Assertions
        print(f"DEBUG: Closed Trade MFE PnL: {closed_trade.mfe_pnl}")
        print(f"DEBUG: Max Favorable Price: {pos.max_favorable_price}")
        
        # The Bug: MFE PnL should be calculated from max_favorable_price but isn't
        # It currently reads pos.mfe_pnl which defaults to 0.0
        
        # If the bug exists, this will be 0.0. 
        # If fixed, it should be 20.0
        self.assertEqual(closed_trade.mfe_pnl, 20.0, "MFE PnL should be 20.0")
        
        # We also want to assert that it *should* have been 20.0 manually to prove our math
        expected_mfe = spec.pnl_money(pos.entry_price, pos.max_favorable_price, pos.lot, pos.direction)
        self.assertEqual(expected_mfe, 20.0)

if __name__ == '__main__':
    unittest.main()
