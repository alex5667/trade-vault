
import unittest
import json
from services.trade_metrics_service import TradeMetricsService

class TestMLStatsFix(unittest.TestCase):
    def setUp(self):
        self.tm = TradeMetricsService()
        self.m = self.tm.new_metrics()

    def test_standard_of_confirm_ml(self):
        """Test ML extraction from standard of_confirm.evidence.ml path."""
        t = {
            "pnl_net": "0.10",
            "indicators": json.dumps({
                "of_confirm": {
                    "scenario": "continuation",
                    "have": 1, "need": 1,
                    "evidence": {
                        "ml": {
                            "allow": True,
                            "p_edge": 0.65
                        }
                    }
                }
            })
        }
        self.tm.accumulate_trade(self.m, t)
        
        ml_stats = self.m["ml_stats"]
        self.assertEqual(ml_stats["pass"]["count"], 1)
        self.assertEqual(ml_stats["pass"]["pnl"], 0.10)
        
        ml_cond = self.m["ml_condition_stats"]
        self.assertEqual(ml_cond["total_evaluated"], 1)
        # Check threshold 0.65
        self.assertEqual(ml_cond["by_threshold"]["0.65"]["count"], 1)
        # Check scenario
        self.assertEqual(ml_cond["by_scenario"]["continuation"]["count"], 1)

    def test_decision_record_v1_ml(self):
        """Test ML extraction from top-level signal_payload.ml (DecisionRecordV1)."""
        # DecisionRecordV1 structure: ml and rule are at top level of signal_payload
        t = {
            "pnl_net": "0.20",
            "signal_payload": json.dumps({
                "ml": {
                    "allow": 1,
                    "p_edge": 0.72,
                    "state": "allow"
                },
                "rule": {
                    "scenario": "reversal",
                    "have": 2, "need": 2,
                    "ok": 1
                }
            })
        }
        self.tm.accumulate_trade(self.m, t)
        
        ml_stats = self.m["ml_stats"]
        self.assertEqual(ml_stats["pass"]["count"], 1)
        self.assertAlmostEqual(ml_stats["pass"]["pnl"], 0.20)
        
        ml_cond = self.m["ml_condition_stats"]
        self.assertEqual(ml_cond["total_evaluated"], 1)
        self.assertEqual(ml_cond["by_threshold"]["0.70"]["count"], 1)
        self.assertEqual(ml_cond["by_scenario"]["reversal"]["count"], 1)

    def test_indicator_direct_ml(self):
        """Test ML extraction from indicators.ml (Legacy/Alternative)."""
        t = {
            "pnl_net": "-0.05",
            "indicators": json.dumps({
                "ml": {
                    "allow": False,
                    "p_edge": 0.45
                }
            })
        }
        self.tm.accumulate_trade(self.m, t)
        
        ml_stats = self.m["ml_stats"]
        self.assertEqual(ml_stats["veto"]["count"], 1)
        self.assertAlmostEqual(ml_stats["veto"]["pnl"], -0.05)
        
        ml_cond = self.m["ml_condition_stats"]
        self.assertEqual(ml_cond["total_evaluated"], 1)
        self.assertEqual(ml_cond["by_threshold"]["0.50"]["count"], 0)
        self.assertEqual(ml_cond["by_scenario"]["none"]["count"], 1)

    def test_mixed_payload_fallback(self):
        """Test that it correctly handles partially populated of_confirm by falling back to sp.ml."""
        t = {
            "pnl_net": "0.15",
            # indicators has an of_confirm but NO ml
            "indicators": json.dumps({
                "of_confirm": {
                    "scenario": "continuation",
                    "ok": 1
                }
            }),
            # signal_payload has ml
            "signal_payload": json.dumps({
                "ml": {
                    "allow": True,
                    "p_edge": 0.58
                }
            })
        }
        self.tm.accumulate_trade(self.m, t)
        
        ml_stats = self.m["ml_stats"]
        self.assertEqual(ml_stats["pass"]["count"], 1)
        self.assertEqual(ml_cond := self.m["ml_condition_stats"]["total_evaluated"], 1)
        self.assertEqual(self.m["ml_condition_stats"]["by_scenario"]["continuation"]["count"], 1)

if __name__ == '__main__':
    unittest.main()
