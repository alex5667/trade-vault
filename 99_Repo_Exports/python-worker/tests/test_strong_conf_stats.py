
import json
import unittest

from services.trade_metrics_service import TradeMetricsService


class TestStrongConfStats(unittest.TestCase):
    def test_strong_conf_accumulation(self):
        tm = TradeMetricsService()
        m = tm.new_metrics()

        # Trade 1: Score 0.82 (82%) -> Should pass 70, 75, 80
        t1 = {
            "pnl": "10.0",
            "signal_payload": json.dumps({
                "indicators": {
                    "of_confirm": {"score": 0.82}
                }
            })
        }

        # Trade 2: Score 0.96 (96%) -> Should pass all thresholds up to 95
        t2 = {
            "pnl": "5.0",
            "signal_payload": json.dumps({
                "indicators": {
                    "of_confirm": {"score": 0.96}
                }
            })
        }

        # Trade 3: Score 0.65 (65%) -> Should not pass any threshold (min 70)
        t3 = {
            "pnl": "-5.0",
            "signal_payload": json.dumps({
                "indicators": {
                    "of_confirm": {"score": 0.65}
                }
            })
        }

        # Apply trades
        tm.accumulate_trade(m, t1)
        tm.accumulate_trade(m, t2)
        tm.accumulate_trade(m, t3)

        tm.finalize(m)

        stats = m.get("strong_high_conf_stats", {})

        # Verify Threshold 70 (passed by t1, t2)
        # Expected Count: 2
        # Expected PnL: 10 + 5 = 15
        self.assertEqual(stats["70"]["count"], 2)
        self.assertAlmostEqual(stats["70"]["pnl"], 15.0)

        # Verify Threshold 80 (passed by t1, t2)
        # Expected Count: 2
        self.assertEqual(stats["80"]["count"], 2)

        # Verify Threshold 85 (passed by t2 only)
        # Expected Count: 1
        # PnL: 5.0
        self.assertEqual(stats["85"]["count"], 1)
        self.assertAlmostEqual(stats["85"]["pnl"], 5.0)

        # Verify Threshold 95 (passed by t2 only)
        self.assertEqual(stats["95"]["count"], 1)

        # Verify Threshold 100 (passed by none)
        self.assertTrue("100" not in stats or stats["100"]["count"] == 0)

        print("Strong Conf Stats Verification Passed!")

if __name__ == '__main__':
    unittest.main()
