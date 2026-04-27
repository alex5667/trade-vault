import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools import tick_gate_metrics_aggregator_v2 as agg  # type: ignore


class TestReasonGuard(unittest.TestCase):
    def test_extract_reason(self):
        r = agg._extract_reason({"fail_reason": "skew:btc:123"})
        self.assertEqual(r, "skew")
        r2 = agg._extract_reason({"reason": "unknown_side"})
        self.assertEqual(r2, "unknown_side")

    def test_guard_reason_collapse(self):
        # Force env-like mode
        agg.REASON_ALLOWLIST.clear()
        agg.REASON_LABEL_MODE = "collapse"
        self.assertEqual(agg._guard_reason("x"), "__other__")
        agg.REASON_ALLOWLIST.update({"skew"})
        self.assertEqual(agg._guard_reason("skew"), "skew")


if __name__ == "__main__":
    unittest.main()
