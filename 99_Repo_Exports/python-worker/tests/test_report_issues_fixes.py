from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""
Tests for validation stats fix and ML p_edge warning logic (Issues 1, 2, 3, 4).
"""
import os
import unittest
from unittest.mock import MagicMock, patch

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from services.trade_metrics_service import TradeMetricsService


class TestValidationStatsBreakdown(unittest.TestCase):
    """Test that _get_validation_stats correctly separates passed/failed/bypassed."""

    def _make_reporter(self, stream_messages: dict[str, list]):
        """Build a minimal PeriodicReporter stub."""
        from services.periodic_reporter import PeriodicReporter
        mock_redis = MagicMock()

        def fake_xrevrange(stream, max="+", min="-", count=5000):
            return stream_messages.get(stream, [])

        mock_redis.xrevrange.side_effect = fake_xrevrange
        mock_redis.get.return_value = None
        mock_redis.hgetall.return_value = {}
        mock_redis.exists.return_value = 0
        mock_redis.smembers.return_value = set()

        reporter = PeriodicReporter.__new__(PeriodicReporter)
        reporter.redis = mock_redis
        reporter._symbol_trailing_enabled = MagicMock(return_value=None)
        return reporter

    def _make_signal_msg(self, validation_status: str, symbol="BTCUSDT", source="CryptoOrderFlow"):
        """Build a fake Redis xrevrange entry with the given validation_status."""
        import json
        ts_ms = get_ny_time_millis()
        msg_id = f"{ts_ms}-0"
        payload = {
            "symbol": symbol,
            "source": source,
            "validation_status": validation_status,
        }
        return (msg_id, {"payload": json.dumps(payload)})

    def test_all_bypassed_returns_na_pass_rate(self):
        """When all signals are bypassed, pass_rate should be 0.0 (no decided signals)."""
        msgs = [self._make_signal_msg("bypassed") for _ in range(10)]
        reporter = self._make_reporter({RS.OF_INPUTS: msgs})
        with patch("os.getenv", side_effect=lambda k, d=None: RS.OF_INPUTS if k == "OF_INPUTS_STREAM" else os.environ.get(k, d)):
            result = reporter._get_validation_stats("CryptoOrderFlow", "ALL", 3600)
        v_pass_rate, total, _, passed_count, bypassed_count, _, _ = result
        self.assertEqual(total, 10)
        self.assertEqual(passed_count, 0)
        self.assertEqual(bypassed_count, 10)
        self.assertAlmostEqual(v_pass_rate, 0.0)

    def test_mixed_passed_failed_bypassed(self):
        """Pass rate = passed / (passed + failed), bypassed excluded."""
        msgs = (
            [self._make_signal_msg("passed")] * 3
            + [self._make_signal_msg("failed")] * 2
            + [self._make_signal_msg("bypassed")] * 5
        )
        reporter = self._make_reporter({RS.OF_INPUTS: msgs})
        with patch("os.getenv", side_effect=lambda k, d=None: RS.OF_INPUTS if k == "OF_INPUTS_STREAM" else os.environ.get(k, d)):
            result = reporter._get_validation_stats("CryptoOrderFlow", "ALL", 3600)
        v_pass_rate, total, _, passed_count, bypassed_count, _, _ = result
        self.assertEqual(total, 10)
        self.assertEqual(passed_count, 3)
        self.assertEqual(bypassed_count, 5)
        # pass rate = 3 / (3+2) * 100 = 60%
        self.assertAlmostEqual(v_pass_rate, 60.0)

    def test_all_passed_returns_100(self):
        """All passed → 100%."""
        msgs = [self._make_signal_msg("passed") for _ in range(5)]
        reporter = self._make_reporter({RS.OF_INPUTS: msgs})
        with patch("os.getenv", side_effect=lambda k, d=None: RS.OF_INPUTS if k == "OF_INPUTS_STREAM" else os.environ.get(k, d)):
            result = reporter._get_validation_stats("CryptoOrderFlow", "ALL", 3600)
        v_pass_rate, total, _, passed_count, bypassed_count, _, _ = result
        self.assertAlmostEqual(v_pass_rate, 100.0)
        self.assertEqual(bypassed_count, 0)

    def test_no_signals_returns_zeros(self):
        """No signals → (0.0, 0, {}, 0, 0)."""
        reporter = self._make_reporter({})
        with patch("os.getenv", side_effect=lambda k, d=None: RS.OF_INPUTS if k == "OF_INPUTS_STREAM" else os.environ.get(k, d)):
            result = reporter._get_validation_stats("CryptoOrderFlow", "ALL", 3600)
        self.assertEqual(result, (0.0, 0, {}, 0, 0, {}, {}))


class TestDQExceptionCounter(unittest.TestCase):
    """Test that accumulate_trade does not silently swallow exceptions in metrics section."""

    def test_exception_counter_incremented_on_bad_signal_payload(self):
        """A signal_payload that causes attribute errors should increment _scenario_exception_count."""
        tm = TradeMetricsService()
        m = tm.new_metrics()

        # Use a MagicMock that raises when .get() is called (simulates corrupt internal object)
        bad_payload = MagicMock()
        bad_payload.get.side_effect = RuntimeError("simulated crash")

        t = {
            "pnl_net": "5.0",
            "pnl_gross": "5.5",
            "fees": "-0.5",
            "close_reason": "TP3",
            "entry_ts_ms": "1700000000000",
            "exit_ts_ms": "1700003969000",
            "signal_payload": bad_payload,  # triggers the outer except block
        }
        result = tm.accumulate_trade(m, t)
        # Must still return True (fail-open)
        self.assertTrue(result)
        # Counter must have been set
        self.assertGreater(m.get("_scenario_exception_count", 0), 0)

    def test_valid_trade_no_exception_counter(self):
        """Valid trade with no signal_payload should not set the exception counter."""
        tm = TradeMetricsService()
        m = tm.new_metrics()
        t = {
            "pnl_net": "3.0",
            "pnl_gross": "3.5",
            "fees": "-0.5",
            "close_reason": "TRAIL_SL",
            "entry_ts_ms": "1700000000000",
            "exit_ts_ms": "1700001000000",
        }
        tm.accumulate_trade(m, t)
        self.assertEqual(m.get("_scenario_exception_count", 0), 0)


class TestNegativeDurationQuarantine(unittest.TestCase):
    """Test that trades with exit_ts < entry_ts are quarantined (not included in financials)."""

    def test_negative_duration_quarantined(self):
        tm = TradeMetricsService()
        m = tm.new_metrics()
        t = {
            "pnl_net": "10.0",
            "fees": "-1.0",
            "close_reason": "TP3",
            "entry_ts_ms": "1700003969000",   # entry AFTER exit → negative duration
            "exit_ts_ms": "1700000000000",
        }
        result = tm.accumulate_trade(m, t)
        # Quarantined: returns False
        self.assertFalse(result)
        # Should be in bad_time counter
        self.assertEqual(m["bad_time"], 1)
        self.assertEqual(m["negative_duration_count"], 1)
        # Must NOT contribute to financial metrics
        self.assertEqual(m["total_trades"], 0)
        self.assertAlmostEqual(m["total_pnl"], 0.0)

    def test_tp_hit_but_zero_pnl_detected(self):
        tm = TradeMetricsService()
        m = tm.new_metrics()
        t = {
            "pnl_net": "0.0",      # TP hit but pnl = 0
            "pnl_gross": "0.0",
            "fees": "0.0",
            "tp1_hit": "1",        # TP was hit
            "close_reason": "TRAIL_SL",
            "entry_ts_ms": "1700000000000",
            "exit_ts_ms": "1700001000000",
        }
        tm.accumulate_trade(m, t)
        self.assertEqual(m["tp_hit_but_zero_pnl"], 1)


if __name__ == "__main__":
    unittest.main()
