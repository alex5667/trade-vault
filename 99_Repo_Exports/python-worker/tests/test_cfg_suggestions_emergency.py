import unittest
from unittest.mock import MagicMock, patch

from tools.cfg_suggestions_sre_monitor_v2 import SugSREMonitor


class TestSugSREMonitorEmergency(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        self.monitor = SugSREMonitor("redis://localhost", dry_run=False, emergency_enable=True)
        self.monitor.redis = self.mock_redis
        self.monitor.now_ms = 1000000

    def test_routing_logic(self):
        # Test default routing
        with patch.dict('os.environ', {
            'NOTIFY_TELEGRAM_STREAM': 'base',
            'NOTIFY_TELEGRAM_STREAM_WARN': 'warn_stream',
            'NOTIFY_TELEGRAM_STREAM_CRIT': 'crit_stream',
            'NOTIFY_TELEGRAM_STREAM_PAGE': 'page_stream',
            'NOTIFY_TELEGRAM_MIRROR_BASE': '0'
        }):
            self.monitor.notify("test", severity="WARN")
            self.mock_redis.xadd.assert_called_with('warn_stream', unittest.mock.ANY)

            self.monitor.notify("test", severity="CRIT")
            self.mock_redis.xadd.assert_called_with('crit_stream', unittest.mock.ANY)

            self.monitor.notify("test", severity="PAGE")
            self.mock_redis.xadd.assert_called_with('page_stream', unittest.mock.ANY)

    def test_emergency_emission(self):
        # Setup conditions for emergency
        self.monitor.emergency_enable = True
        self.monitor.emergency_min_ms = 1000
        self.monitor.emergency_cooldown_sec = 60

        # Mock Redis get/exists
        self.mock_redis.get.side_effect = lambda k: None # No existing emergency, no cooldown
        self.mock_redis.exists.return_value = 0

        res = self.monitor.maybe_emit_emergency(
            prefix="test",
            kind="k",
            scope="s",
            sid="sid1",
            age_ms=2000,
            severity="CRIT",
            alerts=[]
        )

        self.assertTrue(res)
        self.mock_redis.setex.assert_called()
        self.mock_redis.hset.assert_called()

    def test_emergency_cooldown(self):
        self.monitor.emergency_enable = True
        # Mock cooldown exists
        self.mock_redis.get.side_effect = lambda k: "999000" if "emergency:last_ms" in k else None

        # 1000000 - 999000 = 1000ms. Cooldown is 3600s def -> should be blocked
        res = self.monitor.maybe_emit_emergency(
            prefix="test",
            kind="k",
            scope="s",
            sid="sid1",
            age_ms=2000,
            severity="CRIT",
            alerts=[]
        )
        self.assertFalse(res)

if __name__ == '__main__':
    unittest.main()
