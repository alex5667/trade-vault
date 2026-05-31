import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import datetime
from datetime import timezone

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock redis modules BEFORE imports
import sys
mock_redis = MagicMock()
mock_redis.from_url.return_value = MagicMock()
sys.modules['redis'] = mock_redis
sys.modules['redis.exceptions'] = MagicMock()

# Now import the module under test
import services.periodic_reporter as pr

class TestDailyReportExecution(unittest.TestCase):
    
    @patch('services.periodic_reporter.PeriodicReporter')
    @patch('services.periodic_reporter.time')
    @patch('services.periodic_reporter.datetime')
    @patch('services.periodic_reporter.os.getenv')
    def test_daily_report_all_symbols_at_1706(self, mock_getenv, mock_datetime, mock_time, MockReporter):
        # Setup Environment
        env_vars = {
            "DAILY_REPORT_ENABLED": "true",
            "DAILY_REPORT_UTC_HOUR": "17",
            "DAILY_REPORT_UTC_MINUTE": "5",
            "DAILY_REPORT_SYMBOLS": "ALL",
            "DAILY_REPORT_SOURCE": "TestSrc"
        }
        mock_getenv.side_effect = lambda k, d=None: env_vars.get(k, d)
        
        # Setup Time: 17:06 UTC
        fixed_now = datetime.datetime(2026, 1, 15, 17, 6, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_now
        mock_datetime.fromtimestamp.return_value = fixed_now 
        mock_time.time.return_value = 1700000000.0 
        
        # Setup Reporter Instance
        reporter_instance = MockReporter.return_value
        reporter_instance.redis = MagicMock()
        reporter_instance.redis.get.return_value = "2026-01-14" 
        reporter_instance._discover_pairs.return_value = [("TestSrc", "BTCUSDT"), ("TestSrc", "ETHUSDT")]
        
        # Setup Loop Exit
        mock_time.sleep.side_effect = KeyboardInterrupt("Stop Loop")
        
        # Run Main
        try:
            pr.main()
        except KeyboardInterrupt:
            pass
            
        # Verify
        reporter_instance._discover_pairs.assert_called()
        calls = reporter_instance.send_report_for_pair.call_args_list
        
        self.assertTrue(any(c[0] == ('TestSrc', 'BTCUSDT') and c[1].get('window_seconds') == 86400 for c in calls), "BTCUSDT report not sent")
        self.assertTrue(any(c[0] == ('TestSrc', 'ETHUSDT') and c[1].get('window_seconds') == 86400 for c in calls), "ETHUSDT report not sent")
        
        print("✅ Test Passed: Daily Report 'ALL' logic verified for 17:06 UTC")

    @patch('services.periodic_reporter.PeriodicReporter')
    @patch('services.periodic_reporter.time')
    @patch('services.periodic_reporter.datetime')
    @patch('services.periodic_reporter.os.getenv')
    def test_daily_report_skip_before_1705(self, mock_getenv, mock_datetime, mock_time, MockReporter):
        # Setup Environment: Time is 17:04 UTC
        env_vars = {
            "DAILY_REPORT_ENABLED": "true",
            "DAILY_REPORT_UTC_HOUR": "17",
            "DAILY_REPORT_UTC_MINUTE": "5",
            "DAILY_REPORT_SYMBOLS": "ALL",
            "DAILY_REPORT_SOURCE": "TestSrc"
        }
        mock_getenv.side_effect = lambda k, d=None: env_vars.get(k, d)
        
        fixed_now = datetime.datetime(2026, 1, 15, 17, 4, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_now
        
        reporter_instance = MockReporter.return_value
        reporter_instance.redis.get.return_value = "2026-01-14" 
        reporter_instance._discover_pairs.return_value = [("TestSrc", "BTCUSDT")]
        
        mock_time.sleep.side_effect = KeyboardInterrupt("Stop Loop")

        try:
            pr.main()
        except KeyboardInterrupt:
            pass
            
        calls = reporter_instance.send_report_for_pair.call_args_list
        daily_calls = [c for c in calls if c[1].get('window_seconds') == 86400]
        
        self.assertEqual(len(daily_calls), 0, "Daily report should NOT be sent before 17:05")
        print("✅ Test Passed: Daily Report skipped before 17:05 UTC")

if __name__ == '__main__':
    unittest.main()
