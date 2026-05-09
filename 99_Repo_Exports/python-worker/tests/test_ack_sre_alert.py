
import sys
import unittest
from unittest.mock import MagicMock, patch

# Adjust path to include tools
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../tools')))
from tools import ack_sre_alert


class TestAckSreAlert(unittest.TestCase):
    @patch('ack_sre_alert.Redis')
    def test_ack_by_kind_scope(self, mock_redis_cls):
        mock_redis = MagicMock()
        mock_redis_cls.from_url.return_value = mock_redis

        args = ['ack_sre_alert.py', '--kind', 'test_kind', '--scope', 'TEST', '--ttl_sec', '60']
        with patch.object(sys, 'argv', args):
            ack_sre_alert.main()

        # Verify setex call
        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args
        self.assertEqual(call_args[0][0], "sre:ack:cfg_sugg:test_kind:TEST")
        self.assertEqual(call_args[0][1], 60)

    @patch('ack_sre_alert.Redis')
    def test_receipt(self, mock_redis_cls):
        mock_redis = MagicMock()
        mock_redis_cls.from_url.return_value = mock_redis

        args = ['ack_sre_alert.py', '--receipt_id', 'rcpt:12345', '--ttl_sec', '120']
        with patch.object(sys, 'argv', args):
            ack_sre_alert.main()

        # Verify setex call
        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args
        # Assuming defaults prefix is notify:receipt:
        # The script does: key = args.receipt_prefix + args.receipt_id if not args.receipt_id.startswith...
        # Here "rcpt:12345" doesn't start with "notify:receipt:", so it appends
        self.assertEqual(call_args[0][0], "notify:receipt:rcpt:12345")
        self.assertEqual(call_args[0][1], 120)

    @patch('ack_sre_alert.Redis')
    def test_receipt_with_full_key(self, mock_redis_cls):
        mock_redis = MagicMock()
        mock_redis_cls.from_url.return_value = mock_redis

        full_key = "notify:receipt:rcpt:999"
        args = ['ack_sre_alert.py', '--receipt_id', full_key]
        with patch.object(sys, 'argv', args):
            ack_sre_alert.main()

        call_args = mock_redis.setex.call_args
        self.assertEqual(call_args[0][0], full_key)

if __name__ == '__main__':
    unittest.main()
