import unittest
from unittest.mock import MagicMock
from handlers.crypto_orderflow.utils.pre_publish_gates import SmtCoherenceGate

class TestSmtCoherenceGate(unittest.TestCase):
    def setUp(self):
        # Default enabled gate with 0.65 threshold and observe mode
        self.gate = SmtCoherenceGate(
            enabled=True,
            mode="observe",
            bundle_id="btc_eth_v1",
            coh_min=0.65,
            state_stale_ms=5000,
            diag_stream="",
            diag_enabled=False,
            diag_maxlen=1000
        )
        self.ctx = MagicMock()
        self.ctx.indicators = {}
        self.ctx.config = {}
        self.ctx.ts_ms = 1000000000000  # 2001-09-09

        self.redis_mock = MagicMock()
        
    def test_disabled_gate(self):
        self.gate.enabled = False
        res = self.gate.evaluate(ctx=self.ctx, redis_client=self.redis_mock, symbol="ALGOUSDT", kind="buy", side="LONG")
        self.assertEqual(res.decision, "ABSTAIN")
        self.assertEqual(res.reason_code, "OK")

    def test_missing_state_fail_open(self):
        self.redis_mock.hgetall.return_value = {}
        res = self.gate.evaluate(ctx=self.ctx, redis_client=self.redis_mock, symbol="ALGOUSDT", kind="buy", side="LONG")
        self.assertEqual(res.decision, "ALLOW")
        self.assertEqual(res.notes.get("msg"), "no_state_or_stale")

    def test_stale_state_fail_open(self):
        # Current time is ~2026, state is 2001
        self.redis_mock.hgetall.return_value = {
            b"leader": b"BTCUSDT",
            b"leader_dir": b"UP",
            b"leader_confirm": b"1",
            b"coh": b"0.8",
            b"ts_ms": b"1000000000000"
        }
        res = self.gate.evaluate(ctx=self.ctx, redis_client=self.redis_mock, symbol="ALGOUSDT", kind="buy", side="LONG")
        self.assertEqual(res.decision, "ALLOW")
        self.assertEqual(res.notes.get("msg"), "no_state_or_stale")
        self.assertTrue(self.ctx.smt_state_stale)

    def test_observe_mode_always_allows(self):
        import time
        now_ms = time.time() * 1000
        self.redis_mock.hgetall.return_value = {
            b"leader": b"BTCUSDT",
            b"leader_dir": b"DOWN",
            b"leader_confirm": b"1",
            b"coh": b"0.9",
            b"ts_ms": str(now_ms).encode("utf-8")
        }
        # Countertrend (leader is DOWN, signal is LONG), but mode is observe
        self.gate.mode = "observe"
        res = self.gate.evaluate(ctx=self.ctx, redis_client=self.redis_mock, symbol="ALGOUSDT", kind="buy", side="LONG")
        self.assertEqual(res.decision, "ALLOW")
        self.assertEqual(res.notes.get("msg"), "observe_only")
        self.assertTrue(res.notes.get("countertrend"))
        
        # Verify context is enriched
        self.assertEqual(self.ctx.smt_leader_dir, "DOWN")
        self.assertEqual(self.ctx.smt_leader_confirm, 1)
        self.assertEqual(self.ctx.smt_coh, 0.9)
        self.assertFalse(self.ctx.smt_state_stale)

    def test_monitor_mode_alias_for_observe(self):
        import time
        now_ms = time.time() * 1000
        self.redis_mock.hgetall.return_value = {
            b"leader": b"BTCUSDT",
            b"leader_dir": b"DOWN",
            b"leader_confirm": b"1",
            b"coh": b"0.9",
            b"ts_ms": str(now_ms).encode("utf-8")
        }
        self.gate.mode = "monitor"
        res = self.gate.evaluate(ctx=self.ctx, redis_client=self.redis_mock, symbol="ALGOUSDT", kind="buy", side="LONG")
        self.assertEqual(res.decision, "ALLOW")
        self.assertEqual(res.notes.get("msg"), "observe_only")

    def test_veto_mode_allows_trend(self):
        import time
        now_ms = time.time() * 1000
        self.redis_mock.hgetall.return_value = {
            b"leader": b"BTCUSDT",
            b"leader_dir": b"UP",
            b"leader_confirm": b"1",
            b"coh": b"0.8",
            b"ts_ms": str(now_ms).encode("utf-8")
        }
        self.gate.mode = "veto"
        # Trend (leader is UP, signal is LONG)
        res = self.gate.evaluate(ctx=self.ctx, redis_client=self.redis_mock, symbol="ALGOUSDT", kind="buy", side="LONG")
        self.assertEqual(res.decision, "ALLOW")

    def test_veto_mode_blocks_countertrend(self):
        import time
        now_ms = time.time() * 1000
        self.redis_mock.hgetall.return_value = {
            b"leader": b"BTCUSDT",
            b"leader_dir": b"DOWN",
            b"leader_confirm": b"1",
            b"coh": b"0.8",
            b"ts_ms": str(now_ms).encode("utf-8")
        }
        self.gate.mode = "veto"
        # Countertrend (leader is DOWN, signal is LONG)
        res = self.gate.evaluate(ctx=self.ctx, redis_client=self.redis_mock, symbol="ALGOUSDT", kind="buy", side="LONG")
        self.assertEqual(res.decision, "DENY")
        self.assertEqual(res.reason_code, "VETO_SMT_COUNTERTREND")
        self.assertEqual(res.notes.get("coh"), 0.8)

    def test_veto_mode_allows_countertrend_low_coherence(self):
        import time
        now_ms = time.time() * 1000
        self.redis_mock.hgetall.return_value = {
            b"leader": b"BTCUSDT",
            b"leader_dir": b"DOWN",
            b"leader_confirm": b"1",
            b"coh": b"0.4", # Lower than 0.65 threshold
            b"ts_ms": str(now_ms).encode("utf-8")
        }
        self.gate.mode = "veto"
        res = self.gate.evaluate(ctx=self.ctx, redis_client=self.redis_mock, symbol="ALGOUSDT", kind="buy", side="LONG")
        self.assertEqual(res.decision, "ALLOW")

    def test_veto_mode_allows_countertrend_leader_not_confirmed(self):
        import time
        now_ms = time.time() * 1000
        self.redis_mock.hgetall.return_value = {
            b"leader": b"BTCUSDT",
            b"leader_dir": b"DOWN",
            b"leader_confirm": b"0", # Leader not confirmed
            b"coh": b"0.8",
            b"ts_ms": str(now_ms).encode("utf-8")
        }
        self.gate.mode = "veto"
        res = self.gate.evaluate(ctx=self.ctx, redis_client=self.redis_mock, symbol="ALGOUSDT", kind="buy", side="LONG")
        self.assertEqual(res.decision, "ALLOW")

if __name__ == '__main__':
    unittest.main()
