import json
import unittest
from unittest.mock import MagicMock, patch

# Import the worker module (ensuring path is correct for import)
# Add python-worker directory to sys.path
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from services.telegram_notifier_worker_v2 import process_message
from core.redis_keys import RedisStreams as RS


class TestTelegramNotifierWorkerV2(unittest.TestCase):
    def setUp(self):
        self.redis = MagicMock()
        self.stream_key = RS.NOTIFY_TELEGRAM_PAGE
        self.message_id = "1000-0"

    @patch('services.telegram_notifier_worker_v2.send_telegram_message')
    def test_process_message_success_with_receipt(self, mock_send):
        # Setup
        mock_send.return_value = (True, "OK")

        payload = {
            "message": "Test Message",
            "receipt_id": "rec-123",
            "require_receipt": 1
        }
        message_data = {"payload": json.dumps(payload)}

        # Execute
        result = process_message(self.redis, self.stream_key, self.message_id, message_data)

        # Verify
        self.assertTrue(result)
        mock_send.assert_called()
        # Verify receipt was set
        self.redis.setex.assert_called_with("notify:receipt:rec-123", 3600, "1")

    @patch('services.telegram_notifier_worker_v2.send_telegram_message')
    def test_process_message_fail_retries(self, mock_send):
        # Setup
        mock_send.return_value = (False, "Error")

        payload = {
            "message": "Test Message"
        }
        message_data = {"payload": json.dumps(payload)}

        # Execute
        # We patch time.sleep to speed up test
        with patch('time.sleep', return_value=None):
            result = process_message(self.redis, self.stream_key, self.message_id, message_data)

        # Verify
        # Should return True (ACK) even on failure in current logic to avoid blocking,
        # but locally we log error.
        self.assertTrue(result)
        self.assertEqual(mock_send.call_count, 3) # Max retries

    @patch('services.telegram_notifier_worker_v2.send_telegram_message')
    def test_routing_crit(self, mock_send):
        mock_send.return_value = (True, "OK")
        payload = {"message": "Crit Message"}
        message_data = {"payload": json.dumps(payload)}

        # Patch the module-level constant directly
        with patch('services.telegram_notifier_worker_v2.NOTIFY_TELEGRAM_CHAT_ID_CRIT', "crit-chat"):
             process_message(self.redis, RS.NOTIFY_TELEGRAM_CRIT, self.message_id, message_data)

        args, _ = mock_send.call_args
        self.assertEqual(args[0], "crit-chat")

if __name__ == '__main__':
    unittest.main()
