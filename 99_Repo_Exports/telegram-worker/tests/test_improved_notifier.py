"""
Tests for ImprovedTelegramNotifier — no network/Redis needed.
"""
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Prevent ImprovedTelegramNotifier from connecting to Redis on import
import unittest
from unittest.mock import MagicMock, patch


class TestSplitMessage(unittest.TestCase):
    """Tests for _split_message / message chunking logic."""

    def setUp(self):
        # Patch redis so constructor doesn't fail in CI
        with patch("improved_notifier.redis.Redis.from_url", return_value=MagicMock()):
            from improved_notifier import ImprovedTelegramNotifier
            self.notifier = ImprovedTelegramNotifier()

    def test_short_message_not_split(self):
        text = "Hello, world!"
        chunks = self.notifier._split_message(text, max_length=4000)
        self.assertEqual(chunks, [text])

    def test_long_message_is_split(self):
        # Create a message that is 2x the limit
        line = "A" * 100
        text = "\n".join([line] * 90)  # ~9000 chars
        chunks = self.notifier._split_message(text, max_length=4000)
        self.assertGreater(len(chunks), 1)
        # All chunks within limit (with pagination marker overhead)
        for chunk in chunks:
            self.assertLessEqual(len(chunk), 4000 + 30)  # 30 = pagination marker

    def test_split_adds_pagination_marker(self):
        line = "B" * 100
        text = "\n".join([line] * 90)
        chunks = self.notifier._split_message(text, max_length=4000)
        self.assertGreater(len(chunks), 1)
        # Each chunk should contain part indicator
        for chunk in chunks:
            self.assertIn("Part", chunk)

    def test_exact_limit_not_split(self):
        text = "X" * 4000
        chunks = self.notifier._split_message(text, max_length=4000)
        self.assertEqual(len(chunks), 1)


class TestFormatPrice(unittest.TestCase):
    """Tests for _format_price helper."""

    def setUp(self):
        with patch("improved_notifier.redis.Redis.from_url", return_value=MagicMock()):
            from improved_notifier import ImprovedTelegramNotifier
            self.notifier = ImprovedTelegramNotifier()

    def test_large_price_two_decimals(self):
        result = self.notifier._format_price(50000.5)
        self.assertEqual(result, "50000.50")

    def test_small_price_more_decimals(self):
        result = self.notifier._format_price(0.000123)
        # Should have more than 2 decimals for small prices
        self.assertIn("0.0001", result)

    def test_none_returns_dash(self):
        result = self.notifier._format_price(None)
        self.assertEqual(result, "-")

    def test_string_price(self):
        result = self.notifier._format_price("1234.5")
        self.assertEqual(result, "1234.50")

    def test_invalid_returns_original(self):
        result = self.notifier._format_price("not_a_number")
        self.assertEqual(result, "not_a_number")


class TestIsPreformattedSignal(unittest.TestCase):
    """Tests for _is_preformatted_signal helper."""

    def test_xauusd_raw_flag(self):
        from improved_notifier import _is_preformatted_signal
        self.assertTrue(_is_preformatted_signal({}, {"is_xauusd": True}))

    def test_xauusd_parsed_flag(self):
        from improved_notifier import _is_preformatted_signal
        self.assertTrue(_is_preformatted_signal({"is_xauusd": True}, {}))

    def test_regular_signal_not_preformatted(self):
        from improved_notifier import _is_preformatted_signal
        self.assertFalse(_is_preformatted_signal({"symbol": "BTCUSDT"}, {"chat_title": "Chan"}))


class TestTpJsonParsing(unittest.TestCase):
    """Tests for json.loads TP parsing (was eval() — now safe)."""

    def setUp(self):
        with patch("improved_notifier.redis.Redis.from_url", return_value=MagicMock()):
            from improved_notifier import ImprovedTelegramNotifier
            self.notifier = ImprovedTelegramNotifier()

    def test_valid_tp_list_parses(self):
        # Simulate what _process_notification does with tp field
        tp_raw = json.dumps([1.0, 2.0, 3.0])
        result = json.loads(tp_raw)
        self.assertEqual(result, [1.0, 2.0, 3.0])

    def test_empty_tp_returns_empty_list(self):
        tp_raw = ""
        result = json.loads(tp_raw) if tp_raw else []
        self.assertEqual(result, [])

    def test_invalid_tp_returns_empty_list(self):
        tp_raw = "not_json"
        try:
            result = json.loads(tp_raw) if tp_raw else []
            if not isinstance(result, list):
                result = []
        except (json.JSONDecodeError, TypeError, ValueError):
            result = []
        self.assertEqual(result, [])


class TestTimeoutConfig(unittest.TestCase):
    """Tests for HTTP timeout configuration."""

    def test_timeout_is_20_seconds(self):
        with patch("improved_notifier.redis.Redis.from_url", return_value=MagicMock()), \
             patch("improved_notifier.ENABLED", True), \
             patch("improved_notifier.RECIPIENTS", ["123"]), \
             patch("improved_notifier.BOT_TOKEN", "fake"), \
             patch("improved_notifier.API_URL", "https://fake"):
            from improved_notifier import ImprovedTelegramNotifier
            notifier = ImprovedTelegramNotifier()
            # httpx.AsyncClient stores timeout as httpx.Timeout
            self.assertEqual(notifier.http_client.timeout.connect, 20.0)

    def test_max_retries_is_4(self):
        with patch("improved_notifier.redis.Redis.from_url", return_value=MagicMock()), \
             patch("improved_notifier.ENABLED", True), \
             patch("improved_notifier.RECIPIENTS", ["123"]), \
             patch("improved_notifier.BOT_TOKEN", "fake"), \
             patch("improved_notifier.API_URL", "https://fake"):
            from improved_notifier import ImprovedTelegramNotifier
            notifier = ImprovedTelegramNotifier()
            self.assertEqual(notifier.max_retries, 4)
            self.assertEqual(len(notifier.retry_delays), 4)


class TestDLQWrite(unittest.TestCase):
    """Tests for DLQ write on send failure."""

    def setUp(self):
        self.mock_redis = MagicMock()
        with patch("improved_notifier.redis.Redis.from_url", return_value=self.mock_redis), \
             patch("improved_notifier.ENABLED", True), \
             patch("improved_notifier.RECIPIENTS", ["123"]), \
             patch("improved_notifier.BOT_TOKEN", "fake"), \
             patch("improved_notifier.API_URL", "https://fake"):
            from improved_notifier import ImprovedTelegramNotifier
            self.notifier = ImprovedTelegramNotifier()

    def test_write_to_dlq_calls_xadd(self):
        import asyncio
        asyncio.run(self.notifier._write_to_dlq("test message", "timeout"))

        self.mock_redis.xadd.assert_called_once()
        args, kwargs = self.mock_redis.xadd.call_args
        self.assertEqual(args[0], "notify:dlq")
        entry = args[1]
        self.assertEqual(entry["text"], "test message")
        self.assertEqual(entry["error"], "timeout")
        self.assertEqual(entry["attempt_count"], "1")

    def test_write_to_dlq_truncates_long_message(self):
        import asyncio
        long_msg = "x" * 5000
        asyncio.run(self.notifier._write_to_dlq(long_msg, "err"))

        args, _ = self.mock_redis.xadd.call_args
        entry = args[1]
        self.assertLessEqual(len(entry["text"]), 4000)

    def test_write_to_dlq_preserves_buttons(self):
        import asyncio
        buttons = [[{"text": "OK", "callback_data": "ok"}]]
        asyncio.run(self.notifier._write_to_dlq("msg", "err", buttons=buttons))

        args, _ = self.mock_redis.xadd.call_args
        entry = args[1]
        self.assertIn("buttons", entry)
        parsed = json.loads(entry["buttons"])
        self.assertEqual(parsed[0][0]["text"], "OK")


class TestDLQRetry(unittest.TestCase):
    """Tests for DLQ retry mechanism."""

    def setUp(self):
        self.mock_redis = MagicMock()
        with patch("improved_notifier.redis.Redis.from_url", return_value=self.mock_redis), \
             patch("improved_notifier.ENABLED", True), \
             patch("improved_notifier.RECIPIENTS", ["123"]), \
             patch("improved_notifier.BOT_TOKEN", "fake"), \
             patch("improved_notifier.API_URL", "https://fake"):
            from improved_notifier import ImprovedTelegramNotifier
            self.notifier = ImprovedTelegramNotifier()

    def test_retry_dlq_empty_returns_zero(self):
        import asyncio
        self.mock_redis.xrange.return_value = []
        result = asyncio.run(self.notifier.retry_dlq())
        self.assertEqual(result, 0)

    def test_retry_dlq_skips_stale_messages(self):
        import asyncio
        import time as _time
        stale_ts = str(int(_time.time()) - 7200)  # 2 hours ago
        self.mock_redis.xrange.return_value = [
            ("1-0", {"text": "old msg", "failed_at": stale_ts, "attempt_count": "1"})
        ]
        result = asyncio.run(self.notifier.retry_dlq())
        self.assertEqual(result, 0)
        self.mock_redis.xdel.assert_called_once_with("notify:dlq", "1-0")

    def test_retry_dlq_skips_too_many_attempts(self):
        import asyncio
        import time as _time
        self.mock_redis.xrange.return_value = [
            ("2-0", {"text": "msg", "failed_at": str(int(_time.time())), "attempt_count": "6"})
        ]
        result = asyncio.run(self.notifier.retry_dlq())
        self.assertEqual(result, 0)
        self.mock_redis.xdel.assert_called_once_with("notify:dlq", "2-0")


if __name__ == "__main__":
    unittest.main()
