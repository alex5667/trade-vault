import unittest
import json
from services.trade_metrics_service import TradeMetricsService

class TestReportScenarios(unittest.TestCase):
    def setUp(self):
        self.service = TradeMetricsService()
        self.metrics = self.service.new_metrics()

    def test_accumulate_reversal(self):
        # Trade 1: Reversal, Enforce
        t1 = {
            "pnl_net": 100.0,
            "signal_payload": json.dumps({
                "scenario": "reversal",
                "indicators": {
                    "of_gate_mode": "ENFORCE"
                }
            })
        }
        self.service.accumulate_trade(self.metrics, t1)
        
        self.assertEqual(self.metrics["cnt_scenario_reversal"], 1)
        self.assertEqual(self.metrics["sum_pnl_scenario_reversal"], 100.0)
        self.assertEqual(self.metrics["cnt_gate_enforce"], 1)

    def test_accumulate_continuation_shadow(self):
        # Trade 2: Continuation, Shadow
        t2 = {
            "pnl_net": -50.0,
            "signal_payload": json.dumps({
                "scenario": "continuation",
                "indicators": {
                    "of_gate_mode": "SHADOW"
                }
            })
        }
        self.service.accumulate_trade(self.metrics, t2)
        
        self.assertEqual(self.metrics["cnt_scenario_continuation"], 1)
        self.assertEqual(self.metrics["sum_pnl_scenario_continuation"], -50.0)
        self.assertEqual(self.metrics["cnt_gate_shadow"], 1)

    def test_accumulate_shadow_veto(self):
        # Trade 3: Continuation, Shadow, Vetoed
        t3 = {
            "pnl_net": -20.0,
            "signal_payload": json.dumps({
                "scenario": "continuation",
                "indicators": {
                    "of_gate_mode": "SHADOW",
                    "strong_gate_shadow_veto": 1
                }
            })
        }
        self.service.accumulate_trade(self.metrics, t3)
        
        self.assertEqual(self.metrics["cnt_gate_shadow"], 1)
        self.assertEqual(self.metrics["cnt_gate_shadow_veto"], 1)
        self.assertEqual(self.metrics["sum_pnl_shadow_veto"], -20.0)

    def test_mixed_accumulation(self):
        # Mixed bag
        trades = [
            {"pnl_net": 10.0, "signal_payload": json.dumps({"scenario": "reversal", "indicators": {"of_gate_mode": "ENFORCE"}})},
            {"pnl_net": 20.0, "signal_payload": json.dumps({"scenario": "reversal", "indicators": {"of_gate_mode": "ENFORCE"}})},
            {"pnl_net": 5.0, "signal_payload": json.dumps({"scenario": "continuation", "indicators": {"of_gate_mode": "SHADOW"}})},
            {"pnl_net": -15.0, "signal_payload": json.dumps({"scenario": "continuation", "indicators": {"of_gate_mode": "SHADOW", "strong_gate_shadow_veto": 1}})},
        ]
        
        for t in trades:
            self.service.accumulate_trade(self.metrics, t)
            
        self.assertEqual(self.metrics["cnt_scenario_reversal"], 2)
        self.assertEqual(self.metrics["sum_pnl_scenario_reversal"], 30.0)
        self.assertEqual(self.metrics["cnt_scenario_continuation"], 2)
        self.assertEqual(self.metrics["sum_pnl_scenario_continuation"], -10.0)
        
        self.assertEqual(self.metrics["cnt_gate_enforce"], 2)
        self.assertEqual(self.metrics["cnt_gate_shadow"], 2)
        self.assertEqual(self.metrics["cnt_gate_shadow_veto"], 1)
        self.assertEqual(self.metrics["sum_pnl_shadow_veto"], -15.0)

    def test_strong_weak_stats(self):
        # Trade 4: Strong (strong_gate_ok=1)
        t4 = {
            "pnl_net": 50.0,
            "signal_payload": json.dumps({
                "indicators": {"strong_gate_ok": 1}
            })
        }
        # Trade 5: Weak (strong_gate_ok=0)
        t5 = {
            "pnl_net": -10.0,
            "signal_payload": json.dumps({
                "indicators": {"strong_gate_ok": 0}
            })
        }
        # Trade 6: Nested strong_gate_scn and of_confirm_ok
        t6 = {
            "pnl_net": 20.0,
            "signal_payload": json.dumps({
                "indicators": {
                    "strong_gate_scn": "reversal",
                    "of_confirm_ok": 1
                }
            })
        }
        
        self.service.accumulate_trade(self.metrics, t4)
        self.service.accumulate_trade(self.metrics, t5)
        self.service.accumulate_trade(self.metrics, t6)
        
        self.assertEqual(self.metrics["cnt_strong_ok"], 2)
        self.assertEqual(self.metrics["sum_pnl_strong_ok"], 70.0)
        
        self.assertEqual(self.metrics["cnt_strong_fail"], 1)
        self.assertEqual(self.metrics["sum_pnl_strong_fail"], -10.0)

        # check scenario from nested indicators
        self.assertEqual(self.metrics["cnt_scenario_reversal"], 1)
        self.assertEqual(self.metrics["sum_pnl_scenario_reversal"], 20.0)

if __name__ == '__main__':
    unittest.main()
