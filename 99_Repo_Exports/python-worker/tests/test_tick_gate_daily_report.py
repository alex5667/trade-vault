import unittest

from tools.tick_gate_daily_report import GateEvent, aggregate_events


class TestTickGateDailyReport(unittest.TestCase):
    def test_aggregate_counts_and_top(self):
        events = [
            GateEvent(msg_id="1-0", ts_ms=1, status="PASS", return_code=0, symbol="BTCUSDT", failures=[], raw={}),
            GateEvent(msg_id="2-0", ts_ms=2, status="FAIL", return_code=20, symbol="BTCUSDT", failures=[{"metric": "m1"}], raw={}),
            GateEvent(msg_id="3-0", ts_ms=3, status="FAIL", return_code=20, symbol="ETHUSDT", failures=[{"metric": "m1"}, {"metric": "m2"}], raw={}),
            GateEvent(msg_id="4-0", ts_ms=4, status="INSUFFICIENT_DATA", return_code=21, symbol="", failures=[], raw={}),
            GateEvent(msg_id="5-0", ts_ms=5, status="ERROR", return_code=22, symbol="", failures=[], raw={}),
        ]
        r = aggregate_events(events)
        self.assertEqual(r["counts"]["PASS"], 1)
        self.assertEqual(r["counts"]["FAIL"], 2)
        self.assertEqual(r["counts"]["INSUFFICIENT_DATA"], 1)
        self.assertEqual(r["counts"]["ERROR"], 1)
        tfm = {x["metric"]: x["count"] for x in r["top_fail_metrics"]}
        self.assertEqual(tfm.get("m1"), 2)
        self.assertEqual(tfm.get("m2"), 1)
        ts = {x["symbol"]: x for x in r["top_symbols"]}
        self.assertEqual(ts["BTCUSDT"]["FAIL"], 1)
        self.assertEqual(ts["ETHUSDT"]["FAIL"], 1)


if __name__ == "__main__":
    unittest.main()
