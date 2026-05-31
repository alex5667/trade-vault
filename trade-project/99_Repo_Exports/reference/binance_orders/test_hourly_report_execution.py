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

class TestHourlyReportExecution(unittest.TestCase):
    
    @patch('services.periodic_reporter.PeriodicReporter')
    @patch('services.periodic_reporter.time')
    @patch('services.periodic_reporter.datetime')
    @patch('services.periodic_reporter.os.getenv')
    def test_hourly_report_execution_at_minute_0(self, mock_getenv, mock_datetime, mock_time, MockReporter):
        # Setup Environment
        env_vars = {
            "HOURLY_REPORT_ENABLED": "true"
        }
        mock_getenv.side_effect = lambda k, d=None: env_vars.get(k, d)
        
        # Setup Time: 15:00 UTC
        fixed_now = datetime.datetime(2026, 1, 15, 15, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_now
        mock_datetime.fromtimestamp.return_value = fixed_now
        mock_time.time.return_value = 1700000000.0 
        
        # Setup Reporter Instance
        reporter_instance = MockReporter.return_value
        reporter_instance.redis = MagicMock()
        # Redis returns None for last hour to simulate not sent yet
        reporter_instance.redis.get.return_value = None 
        reporter_instance._discover_pairs.return_value = [("TestSrc", "BTCUSDT"), ("TestSrc", "PEPEUSDT")]
        
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
        
        # Verify call args: window should be 3600 (hourly)
        btc_calls = [c for c in calls if c[0] == ('TestSrc', 'BTCUSDT') and c[1].get('window_seconds') == 3600]
        pepe_calls = [c for c in calls if c[0] == ('TestSrc', 'PEPEUSDT') and c[1].get('window_seconds') == 3600]
        
        self.assertTrue(len(btc_calls) > 0, "BTCUSDT hourly report not sent")
        self.assertTrue(len(pepe_calls) > 0, "PEPEUSDT hourly report not sent")
        
        print("✅ Test Passed: Hourly Report logic verified for 15:00 UTC")

if __name__ == '__main__':
    unittest.main()
