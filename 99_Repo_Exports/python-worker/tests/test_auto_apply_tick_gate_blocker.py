import unittest

from tools.auto_apply_tick_gate_blocker import _label_limiter, _normalize_fail_mode, GateResult, AutoApplyBlocker


class TestAutoApplyTickGateBlocker(unittest.TestCase):
    def test_label_limiter_collapse(self):
        allow = ["unknown_side", "skew"]
        self.assertEqual(_label_limiter("unknown_side_ema_high", "collapse", allow), "unknown_side")
        self.assertEqual(_label_limiter("event_stream_skew_p99", "collapse", allow), "skew")
        self.assertEqual(_label_limiter("something_else", "collapse", allow), "__other__")

    def test_fail_mode(self):
        self.assertEqual(_normalize_fail_mode("fail_open"), "fail_open")
        self.assertEqual(_normalize_fail_mode("FAIL_CLOSED"), "fail_closed")
        self.assertEqual(_normalize_fail_mode("??"), "fail_open")

    def test_decide_hold(self):
        b = AutoApplyBlocker()
        b.hold_s = 10
        fail = GateResult(rc=2, status="fail", reasons=["x"], payload={})
        ok = GateResult(rc=0, status="pass", reasons=[], payload={})
        decided1, meta1 = b._decide_block(fail)
        self.assertTrue(decided1)
        decided2, meta2 = b._decide_block(ok)
        # hold keeps blocked briefly after fail
        self.assertTrue(decided2)
        self.assertTrue(meta2.get("hold_active"))


if __name__ == "__main__":
    unittest.main()
