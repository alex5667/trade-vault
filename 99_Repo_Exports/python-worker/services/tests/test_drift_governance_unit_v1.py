import unittest
from unittest.mock import MagicMock, patch
import json
from datetime import datetime, timedelta, timezone

# Mocking the analytics_db before importing the service
import sys
mock_db = MagicMock()
sys.modules['services.analytics_db'] = MagicMock()
sys.modules['services.analytics_db'].get_conn = MagicMock(return_value=mock_db)

from services.atr_model_config_drift_service import ATRModelConfigDriftService

class TestDriftGovernance(unittest.TestCase):
    def setUp(self):
        self.conn = mock_db.__enter__.return_value
        self.cur = self.conn.cursor.return_value.__enter__.return_value
        self.cur.execute.reset_mock()
        self.cur.fetchone.reset_mock()
        self.cur.fetchall.reset_mock()

    @patch('services.atr_model_config_drift_service.uuid')
    def test_detect_feature_drift(self, mock_uuid):
        mock_uuid.uuid4.return_value.hex = "testhex"
    
        # Test warn (0.35 < 0.4)
        ATRModelConfigDriftService.detect_feature_drift("BTCUSDT", 0.35, 0.4, {"feat": "vol"})
        self.cur.execute.assert_not_called()

        # Test error (0.5 > 0.4)
        ATRModelConfigDriftService.detect_feature_drift("BTCUSDT", 0.5, 0.4, {"feat": "vol"})
        self.assertGreaterEqual(self.cur.execute.call_count, 1)
        
        # Verify specific family in one of the calls
        found = False
        for call in self.cur.execute.call_args_list:
            if "FEATURE_DISTRIBUTION_DRIFT" in call[0][1]:
                found = True
                break
        self.assertTrue(found, "FEATURE_DISTRIBUTION_DRIFT not found in SQL calls")
        
    def test_check_dataset_validity_expired(self):
        # Mock DB response for expired dataset
        self.cur.fetchone.return_value = {
            'status': 'valid',
            'valid_until': datetime.now(timezone.utc) - timedelta(hours=1)
        }
        
        status, until = ATRModelConfigDriftService.check_dataset_validity("ds_123")
        self.assertEqual(status, "expired")
        self.cur.execute.assert_any_call("UPDATE atr_dataset_baseline_validity SET status = 'expired' WHERE dataset_id = %s", ("ds_123",))

    def test_is_release_blocked_by_drift(self):
        # Mock active drift events
        self.cur.fetchall.return_value = [
            {
                'drift_family': 'EXECUTION_COST_DRIFT',
                'severity': 'critical',
                'scope_value': 'BTCUSDT'
            }
        ]
        
        blockers = ATRModelConfigDriftService.is_release_blocked_by_drift("CRITICAL_EXECUTION_TOUCHING", "BTCUSDT")
        self.assertIn("critical drift EXECUTION_COST_DRIFT on BTCUSDT", blockers)

if __name__ == '__main__':
    unittest.main()
