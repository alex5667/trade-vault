
import unittest
import json
from services.trade_metrics_service import TradeMetricsService

class TestMissingOFStats(unittest.TestCase):
    def test_reproduce_missing_stats(self):
        tm = TradeMetricsService()
        m = tm.new_metrics()
        
        # Mock DecisionRecordV1 payload (no top-level indicators)
        signal_payload = {
            "version": 1,
            "sid": "test_sid",
            "rule": {
                "ok": 1,
                "score": 0.85,
                "scenario": "trend_pullback",
                "have": 2,
                "need": 2
            },
            "ml": {
                "state": "allow",
                "p_edge": 0.123
            }
        }
        
        t = {
            "signal_payload": json.dumps(signal_payload),
            "pnl_net": "10.0",
            "close_reason": "TP1",
            "entry_ts_ms": "1600000000000",
            "exit_ts_ms": "1600000001000",
        }
        
        tm.accumulate_trade(m, t)
        
        # Check if OF Confirm Stats are populated
        stats = m.get("of_confirm_stats", {})
        print(f"Accumulated Stats: {stats}")
        
        # Verification:
        # If bug exists, stats should be empty because it looks for 'indicators'
        # If fix works, stats should contain 'trend_pullback_gate(2/2)'
        
        key = "trend_pullback_gate(2/2)"
        # self.assertIn(key, stats, "OF Confirm Stats should be populated from DecisionRecordV1")
        
        return stats

if __name__ == '__main__':
    t = TestMissingOFStats()
    stats = t.test_reproduce_missing_stats()
    if not stats:
        print("FAILURE REPRODUCED: Stats are empty.")
    else:
        print("SUCCESS: Stats are populated.")
