import unittest
from unittest.mock import MagicMock
from handlers.crypto_orderflow.utils.pre_publish_gates import AtrFloorGate

class TestAtrFloorGate(unittest.TestCase):
    def setUp(self):
        # Default enabled gate with thresholds
        self.gate = AtrFloorGate(
            enabled=True,
            t0_bps=5.0,
            t1_bps=10.0,
            t2_bps=15.0,
            fail_open=True
        )

        self.ctx = MagicMock()
        self.ctx.indicators = {}
        self.ctx.config = {}
        # Make _get_epoch_ms return 100
        self.ctx.indicators["now_ts_ms"] = 100

    def test_disabled_gate(self):
        self.gate.enabled = False
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "ABSTAIN")
        self.assertEqual(res.reason_code, "OK")

    def test_above_threshold_allows(self):
        self.ctx.indicators["atr_bps"] = 20.0
        # By default it's trend regime, tier 0 = 5.0
        self.ctx.regime = "trending_bull"
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "ALLOW")

    def test_below_threshold_denies(self):
        self.ctx.indicators["atr_bps"] = 4.0
        self.ctx.regime = "trending_bull" # tier 0: 5.0
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "DENY")
        self.assertEqual(res.reason_code, "VETO_ATR_FLOOR")
        self.assertEqual(res.notes.get("thr"), 5.0)

    def test_below_threshold_range_regime(self):
        self.ctx.indicators["atr_bps"] = 6.0
        self.ctx.regime = "range_chop" # tier 1: 10.0
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "DENY")
        self.assertEqual(res.notes.get("thr"), 10.0)
        self.assertEqual(res.notes.get("tier"), 1)

    def test_fail_open_on_missing_atr(self):
        self.ctx.indicators.pop("atr_bps", None)
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "ALLOW")
        self.assertEqual(res.notes.get("msg"), "atr_missing_fail_open")

    def test_fail_open_on_zero_atr(self):
        self.ctx.indicators["atr_bps"] = 0.0
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "ALLOW")
        self.assertEqual(res.notes.get("msg"), "atr_missing_fail_open")

    def test_fail_closed_on_zero_atr_if_fail_open_false(self):
        self.ctx.indicators["atr_bps"] = 0.0
        self.gate.fail_open = False
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "DENY")
        self.assertEqual(res.reason_code, "VETO_ATR_MISSING")

    def test_fallback_to_atr_bps_exec(self):
        self.ctx.indicators["atr_bps"] = 0.0
        self.ctx.indicators["atr_bps_exec"] = 12.0
        self.ctx.regime = "range"
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "ALLOW")

    def test_fail_open_on_atr_bad(self):
        # ATR is technically below threshold, so normally it would deny
        self.ctx.indicators["atr_bps"] = 4.0
        self.ctx.regime = "trend" # th=5.0
        self.ctx.indicators["atr_bad"] = 1
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "ALLOW")
        self.assertIn("atr_not_ready_fail_open", res.notes.get("msg", ""))

    def test_fail_open_on_atr_not_ready(self):
        # ATR is below threshold, but calibrator is not ready
        self.ctx.indicators["atr_bps"] = 4.0
        self.ctx.regime = "trend" # th=5.0
        self.ctx.indicators["atr_floor_ready"] = 0
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "ALLOW")
        self.assertIn("atr_not_ready_fail_open", res.notes.get("msg", ""))

    def test_default_ready_allows_normal_deny(self):
        # When atr_floor_ready is absent, defaults to 1, should evaluate normally
        self.ctx.indicators["atr_bps"] = 4.0
        self.ctx.regime = "trend" # th=5.0

        self.assertNotIn("atr_floor_ready", self.ctx.indicators)
        res = self.gate.evaluate(ctx=self.ctx, symbol="BTCUSDT", kind="buy")
        self.assertEqual(res.decision, "DENY")

if __name__ == '__main__':
    unittest.main()
