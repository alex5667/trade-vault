import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools.tick_gate_metrics_aggregator import LabelLimiter, _extract_event, _first_token, _norm_status


class TestTickGateAggregatorHelpers(unittest.TestCase):
    def test_norm_status(self):
        self.assertEqual(_norm_status("OK"), "pass")
        self.assertEqual(_norm_status("fail"), "fail")
        self.assertEqual(_norm_status("insufficient_data"), "insufficient")
        self.assertEqual(_norm_status("error"), "error")
        self.assertEqual(_norm_status("halt_ramp"), "fail")

    def test_first_token(self):
        self.assertEqual(_first_token("a|b|c"), "a")
        self.assertEqual(_first_token("a,b"), "a")
        self.assertEqual(_first_token(""), "")

    def test_label_limiter(self):
        ll = LabelLimiter(mode="collapse", allowlist=("skew", "unknown_side"))
        self.assertEqual(ll.label("skew|p99"), "skew")
        self.assertEqual(ll.label("something_else"), "__other__")
        ll2 = LabelLimiter(mode="skip", allowlist=("skew",))
        self.assertIsNone(ll2.label("unknown_side"))
        self.assertEqual(ll2.label("skew"), "skew")

    def test_extract_event(self):
        st, reason, sym, ts = _extract_event(
            {"status": "FAIL", "reason": "skew|p99", "symbol": "BTCUSDT", "ts_ms": "1700000000000"}
        )
        self.assertEqual(st, "fail")
        self.assertEqual(reason, "skew|p99")
        self.assertEqual(sym, "BTCUSDT")
        self.assertGreater(ts, 1_000_000_000_000)


if __name__ == "__main__":
    unittest.main()
