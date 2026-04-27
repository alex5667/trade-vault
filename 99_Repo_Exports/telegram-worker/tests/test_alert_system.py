"""
Tests for AlertSystem — no real Redis or Telegram needed (uses unittest.mock).
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import time
import unittest
from unittest.mock import MagicMock, call
import logging


class TestAlertSystem(unittest.TestCase):

    def _make_alert_system(self):
        from app.alert_system import AlertSystem
        r = MagicMock()
        r.xadd.return_value = "1-0"
        logger = logging.getLogger("test_alerts")
        return AlertSystem(r, logger, bot_token="fake-token", chat_ids=["123"]), r

    # ------------------------------------------------------------------ #
    # send_redis_alert
    # ------------------------------------------------------------------ #

    def test_send_redis_alert_calls_xadd(self):
        system, r = self._make_alert_system()
        result = system.send_redis_alert("test message", "info", "chan1")
        self.assertTrue(result)
        r.xadd.assert_called_once()
        args = r.xadd.call_args[0]
        self.assertEqual(args[0], "telegram:alerts")

    def test_send_redis_alert_handles_exception(self):
        system, r = self._make_alert_system()
        r.xadd.side_effect = Exception("redis unavailable")
        result = system.send_redis_alert("msg", "error")
        self.assertFalse(result)

    # ------------------------------------------------------------------ #
    # cooldown logic
    # ------------------------------------------------------------------ #

    def test_cooldown_suppresses_duplicate_alert(self):
        system, r = self._make_alert_system()
        system.alert_cooldown = 300  # 5 min cooldown

        # First call — should go through
        result1 = system.send_alert("same message", "warning", "chan1", send_telegram=False)
        self.assertTrue(result1)

        # Second identical call immediately — should be suppressed
        result2 = system.send_alert("same message", "warning", "chan1", send_telegram=False)
        self.assertFalse(result2)

        # Redis xadd should only have been called once
        self.assertEqual(r.xadd.call_count, 1)

    def test_cooldown_allows_different_alert(self):
        system, r = self._make_alert_system()
        result1 = system.send_alert("msg A", "warning", "chan1", send_telegram=False)
        result2 = system.send_alert("msg B", "warning", "chan1", send_telegram=False)
        # Both should succeed — different messages
        self.assertTrue(result1)
        self.assertTrue(result2)
        self.assertEqual(r.xadd.call_count, 2)

    def test_cooldown_expires(self):
        system, r = self._make_alert_system()
        system.alert_cooldown = 0  # Instant expiry for test

        result1 = system.send_alert("msg", "info", "chan", send_telegram=False)
        result2 = system.send_alert("msg", "info", "chan", send_telegram=False)
        self.assertTrue(result1)
        self.assertTrue(result2)  # Should pass because cooldown is 0

    # ------------------------------------------------------------------ #
    # alert_key uniqueness
    # ------------------------------------------------------------------ #

    def test_alert_key_is_type_channel_message(self):
        system, r = self._make_alert_system()
        # Same message, different type → both should go through
        result1 = system.send_alert("msg", "info", "chan", send_telegram=False)
        result2 = system.send_alert("msg", "error", "chan", send_telegram=False)
        self.assertTrue(result1)
        self.assertTrue(result2)
        self.assertEqual(r.xadd.call_count, 2)

    # ------------------------------------------------------------------ #
    # get_recent_alerts
    # ------------------------------------------------------------------ #

    def test_get_recent_alerts_returns_list(self):
        system, r = self._make_alert_system()
        r.xrevrange.return_value = [
            ("1-0", {"message": "test", "type": "info", "channel": "sys", "timestamp": "123", "data": "{}"}),
        ]
        alerts = system.get_recent_alerts(limit=5)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["message"], "test")

    def test_get_recent_alerts_on_exception_returns_empty(self):
        system, r = self._make_alert_system()
        r.xrevrange.side_effect = Exception("redis error")
        alerts = system.get_recent_alerts()
        self.assertEqual(alerts, [])

    # ------------------------------------------------------------------ #
    # cleanup_old_alerts
    # ------------------------------------------------------------------ #

    def test_cleanup_calls_xtrim(self):
        system, r = self._make_alert_system()
        system.cleanup_old_alerts(days=7)
        r.xtrim.assert_called_once()


if __name__ == "__main__":
    unittest.main()
