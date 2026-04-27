
import unittest
from unittest.mock import MagicMock, patch
import json
import time
from services.periodic_reporter import PeriodicReporter

class TestReportGeneration(unittest.TestCase):
    def setUp(self):
        # Patch get_redis to return a mock
        self.get_redis_patcher = patch('services.periodic_reporter.get_redis')
        self.mock_get_redis = self.get_redis_patcher.start()
        self.mock_redis_instance = MagicMock()
        self.mock_get_redis.return_value = self.mock_redis_instance

        # Patch redis.from_url just in case
        self.from_url_patcher = patch('redis.from_url')
        self.mock_from_url = self.from_url_patcher.start()
        self.mock_from_url.return_value = self.mock_redis_instance
        
        # Instantiate reporter (now uses mock redis)
        self.reporter = PeriodicReporter()
        self.reporter.reporting = MagicMock()
        
    def tearDown(self):
        self.get_redis_patcher.stop()
        self.from_url_patcher.stop()
        
    def test_missing_signal_payload(self):
        # Trade without signal_payload
        trade = {
            "order_id": "ord_missing",
            "symbol": "ETHUSDT",
            "source": "CryptoOrderFlow",
            "status": "closed",
            "pnl": "10.0",
            "exit_ts_ms": str(int(time.time() * 1000)),
            # No signal_payload
        }
        
        # Mock _iter_recent_trades_window to return this trade
        self.reporter._iter_recent_trades_window = MagicMock(return_value=[trade])
        
        # Capture the report message
        self.reporter.send_report_for_pair("CryptoOrderFlow", "ETHUSDT", window_seconds=3600)
        
        # Verify call
        self.reporter.reporting.send_telegram_message.assert_called()
        msg = self.reporter.reporting.send_telegram_message.call_args[0][0]
        
        print("\n--- Report with MISSING payload ---")
        print(msg)
        
        # Assertions
        self.assertNotIn("OF Confirm Stats", msg)
        self.assertNotIn("ML Performance", msg)

    def test_with_signal_payload(self):
        # Trade WITH signal_payload
        # Trade WITH signal_payload (structure matching TradeMetricsService.accumulate_trade logic)
        payload = {
            "indicators": {
                "of_confirm": {
                    "count": 1, "wins": 1, "pnl": 10.0,
                    "evidence": {
                        "ml": {
                            "allow": True,
                            "p_edge": 0.60
                        }
                    }
                },
                "of_confirm_ok": 1
            },
            "validation_status": "passed"
        }
        
        trade = {
            "order_id": "ord_full",
            "symbol": "ETHUSDT",
            "source": "CryptoOrderFlow",
            "status": "closed",
            "pnl": "10.0",
            "exit_ts_ms": str(int(time.time() * 1000)),
            "signal_payload": json.dumps(payload)
        }
        
        self.reporter._iter_recent_trades_window = MagicMock(return_value=[trade])
        
        self.reporter.send_report_for_pair("CryptoOrderFlow", "ETHUSDT", window_seconds=3600)
        
        self.reporter.reporting.send_telegram_message.assert_called()
        msg = self.reporter.reporting.send_telegram_message.call_args[0][0]
        
        print("\n--- Report WITH payload ---")
        print(msg)
        
        # Assertions
        self.assertIn("OF Confirm Stats", msg)
        self.assertIn("ML Performance", msg)

    def test_hydrate_stream_payload(self):
        from services.trade_closed_hydrator import hydrate_trade_closed
        
        # Simulate stream fields with new signal_payload
        payload_dict = {
            "indicators": {"of_confirm": {"count": 1, "wins": 1, "pnl": 10.0}},
            "ml_stats": {"pass": {"count": 1}}
        }
        stream_fields = {
            "order_id": "ord_stream",
            "pnl": "10",
            "signal_payload": json.dumps(payload_dict)
        }
        
        # Hydrate (mock redis)
        mock_redis = MagicMock()
        mock_redis.hgetall.return_value = {} # Empty hash
        
        hydrated = hydrate_trade_closed(mock_redis, stream_fields, merge_precedence="stream")
        
        print("\n--- Hydrated Trade ---")
        print(hydrated)
        
        self.assertEqual(hydrated.get("signal_payload"), json.dumps(payload_dict))
        
        # Verify reporter uses it
        self.reporter._iter_recent_trades_window = MagicMock(return_value=[hydrated])
        self.reporter.send_report_for_pair("CryptoOrderFlow", "ETHUSDT", window_seconds=3600)
        
        msg = self.reporter.reporting.send_telegram_message.call_args[0][0]
        self.assertIn("OF Confirm Stats", msg)

if __name__ == "__main__":
    unittest.main()
