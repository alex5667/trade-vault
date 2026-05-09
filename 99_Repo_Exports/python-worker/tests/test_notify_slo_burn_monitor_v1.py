
import json
import unittest
from unittest.mock import MagicMock, patch

# Ensure we can import the tool
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.join(os.path.dirname(__file__), "../tools"))
import notify_slo_burn_monitor_v1 as monitor


class TestNotifySLOBurnMonitor(unittest.TestCase):



    def setUp(self):
        self.mock_redis = MagicMock()
        # Reset globals to default before each test
        monitor.NOTIFY_SLO_EMIT_SUGGESTIONS = 0
        monitor.NOTIFY_SLO_EMIT_TRADE_PAUSE = 0
        monitor.NOTIFY_SLO_UNPAUSE_ON_OK = 0
        monitor.CFG_SUGGESTIONS_PREFIX = "cfg:suggestions:entry_policy"
        # Fix: ensure ops are not empty so emit_suggestion doesn't skip
        monitor.NOTIFY_SLO_TRADE_PAUSE_OPS_JSON = '[{"op":"test"}]'
        monitor.NOTIFY_SLO_TRADE_UNPAUSE_OPS_JSON = '[{"op":"test"}]'

    def test_calculate_burn_rate(self):
        # 100 requests, 1 error => 1% error rate
        # SLO 99.9% => budget 0.1%
        # Burn = 1% / 0.1% = 10x
        stats = {"ok": 99, "err": 1}
        burn, total = monitor.calculate_burn_rate(stats, 0.999)
        self.assertAlmostEqual(burn, 10.0)
        self.assertEqual(total, 100)

        # No traffic
        stats = {"ok": 0, "err": 0}
        burn, total = monitor.calculate_burn_rate(stats, 0.999)
        self.assertEqual(burn, 0.0)

    @patch("notify_slo_burn_monitor_v1.get_redis_client")
    def test_emit_suggestion_basic(self, mock_get_redis):
        # Enable config
        monitor.NOTIFY_SLO_EMIT_SUGGESTIONS = 1

        r = self.mock_redis
        # Mock cooldown check -> False (not exists)
        r.exists.return_value = False

        ops = [{"op": "test"}]
        ops_json = json.dumps(ops)
        meta = {"reason": "test"}

        sid = monitor.emit_suggestion(r, "test_kind", "ALL", ops_json, meta, 60, "test_dedup")

        self.assertIsNotNone(sid)
        # Verify Redis calls
        # 1. Check cooldown
        r.exists.assert_called_with("sre:notify_slo:cooldown:test_dedup:ALL")
        # 2. Set cooldown
        r.setex.assert_any_call("sre:notify_slo:cooldown:test_dedup:ALL", 60, "1")
        # 3. Push suggestion
        # key: cfg:suggestions:entry_policy:meta:{sid}
        args, _ = r.setex.call_args_list[1]
        self.assertTrue(args[0].startswith("cfg:suggestions:entry_policy:meta:"))
        val = json.loads(args[2])
        self.assertEqual(val["kind"], "test_kind")
        self.assertEqual(val["ops"], ops)

    def test_emit_suggestion_cooldown(self):
        monitor.NOTIFY_SLO_EMIT_SUGGESTIONS = 1
        r = self.mock_redis
        r.exists.return_value = True # Cooldown active

        sid = monitor.emit_suggestion(r, "test_kind", "ALL", "[]", {}, 60, "test_dedup")
        self.assertIsNone(sid)

    def test_handle_trade_pause(self):
        monitor.NOTIFY_SLO_EMIT_SUGGESTIONS = 1
        monitor.NOTIFY_SLO_EMIT_TRADE_PAUSE = 1

        r = self.mock_redis
        r.exists.return_value = False

        monitor.handle_trade_pause(r, "ALL", {})

        # Should set the pause state key
        r.set.assert_called_with("sre:notify_slo:trade_pause_sid:ALL", unittest.mock.ANY)

    def test_handle_trade_unpause(self):
        monitor.NOTIFY_SLO_EMIT_SUGGESTIONS = 1
        monitor.NOTIFY_SLO_EMIT_TRADE_PAUSE = 1
        monitor.NOTIFY_SLO_UNPAUSE_ON_OK = 1
        monitor.CFG_SUGGESTIONS_PREFIX = "cfg:suggestions:test"

        r = self.mock_redis

        # 1. Setup: Pause active
        r.get.return_value = "pause-sid-123"
        # 2. Setup: Pause was APPLIED
        def exists_side_effect(key):
            if key == "cfg:suggestions:test:applied:pause-sid-123":
                return True
            return False # cooldown keys
        r.exists.side_effect = exists_side_effect

        monitor.handle_trade_unpause(r, "ALL", {})

        # Should emit unpause
        # Verify we cleared the state
        r.delete.assert_called_with("sre:notify_slo:trade_pause_sid:ALL")

    def test_handle_trade_unpause_not_applied(self):
        monitor.NOTIFY_SLO_EMIT_SUGGESTIONS = 1
        monitor.NOTIFY_SLO_EMIT_TRADE_PAUSE = 1
        monitor.NOTIFY_SLO_UNPAUSE_ON_OK = 1

        r = self.mock_redis
        r.get.return_value = "pause-sid-123"
        r.exists.return_value = False # Not applied

        monitor.handle_trade_unpause(r, "ALL", {})

        # Should NOT emit unpause
        # Should NOT delete state
        r.delete.assert_not_called()
        # Should check applied key
        r.exists.assert_any_call(f"{monitor.CFG_SUGGESTIONS_PREFIX}:applied:pause-sid-123")


if __name__ == "__main__":
    unittest.main()
