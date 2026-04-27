
import unittest
from domain.models import PositionState, Side, TradeClosed
from domain.handlers import finalize_trade
from services.trade_metrics_service import TradeMetricsService

class MockSpec:
    contract_size = 1.0
    report_min_risk_usd = 1.0
    report_fees_risk_mult = 3.0
    
    def calculate_fees(self, **kwargs):
        return 0.5
    
    def pnl_money(self, entry, exit, qty, direction, symbol):
        return (exit - entry) * qty if direction == "LONG" else (entry - exit) * qty

class TestTPFixLogic(unittest.TestCase):
    def test_tp_touched_logic(self):
        # 1. Create PositionState (LONG)
        # Entry: 100, TP1: 110. MFE: 112 (Touched!)
        pos = PositionState(
            id="test_id",
            sid="test_sid",
            strategy="test_strat",
            source="test_src",
            symbol="BTCUSDT",
            tf="1m",
            direction="LONG",
            entry_price=100.0,
            entry_ts_ms=1000,
            lot=1.0,
            remaining_qty=1.0,
            sl=90.0,
            tp_levels=[110.0, 120.0, 130.0],
            max_favorable_price=112.0, # > TP1
            max_favorable_ts=2000,
            tp_hits=0 # Executed hits = 0
        )
        
        spec = MockSpec()
        
        # 2. Finalize Trade (Timeout close at 105, profit but no TP execution)
        closed = finalize_trade(
            pos=pos,
            spec=spec,
            exit_price=105.0,
            exit_ts_ms=3000,
            close_reason_raw="TIMEOUT",
            tp_ratios=[0.3, 0.3, 0.4]
        )
        
        # 3. Verify tp1_touched is True
        print(f"DEBUG: tp1_touched={getattr(closed, 'tp1_touched', None)}")
        self.assertTrue(getattr(closed, "tp1_touched", False), "TP1 should be touched because MFE 112 > TP1 110")
        self.assertFalse(getattr(closed, "tp2_touched", False), "TP2 (120) should NOT be touched")
        
        # 4. Dictionary simulation (as if loaded from Redis)
        # RedisRepo would set tp1_touched="1" string, keep that in mind if testing full cycle
        # But TradeMetricsService handles bools/ints via _si helper.
        
        data = {
            "tp1_hit": "0",
            "tp1_touched": "1" if closed.tp1_touched else "0"
        }
        
        # 5. Verify Metrics Service counts it
        tm = TradeMetricsService()
        m = tm.new_metrics()
        tm.accumulate_trade(m, data)
        
        print(f"DEBUG: metrics tp1_hits={m['tp1_hits']}")
        self.assertEqual(m["tp1_hits"], 1, "Metrics should count 1 hit due to tp1_touched")
        self.assertEqual(m["tp2_hits"], 0)

if __name__ == '__main__':
    unittest.main()
