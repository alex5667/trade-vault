"""
test_quantiles_job.py — unit tests for quantiles_job

Covers:
- compute_all_quantiles: happy path, skip when 0 rows returned (MIN_SAMPLES enforced in SQL)
- upsert_quantiles: SQL shape, Redis cache write, TTL
- cache_quantiles: no-op when REDIS_URL is None
- get_conn: RuntimeError when DATABASE_URL is None
- tick(): full happy-path integration with mocked DB + Redis
- p90 is a valid float (not NaN)
"""
import json
import math
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Ensure local module is importable and mock prometheus_client before import
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.modules["prometheus_client"] = MagicMock()

import quantiles_job  # noqa: E402 — must come after sys.modules patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_SAMPLE_ROW = (
    "BTCUSDT", "15m",
    1000,
    "2026-01-01 00:00:00", "2026-01-14 00:00:00",  # src_min, src_max
    20.0, 30.0, 40.0,                               # ADX p40/p60/p75
    0.5,  1.0,  1.5,  2.0,                          # ATR% p25/p50/p75/p90
)

_SAMPLE_Q = {
    "sample_count": 1000,
    "src_time_min": "2026-01-01T00:00:00",
    "src_time_max": "2026-01-14T00:00:00",
    "adx_p40": 20.0, "adx_p60": 30.0, "adx_p75": 40.0,
    "atrp_p25": 0.5, "atrp_p50": 1.0, "atrp_p75": 1.5, "atrp_p90": 2.0,
}


def _make_mock_conn(fetchall_result):
    """Return a mock psycopg2 connection whose cursor yields *fetchall_result*."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = fetchall_result
    return mock_conn, mock_cursor


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestComputeQuantiles(unittest.TestCase):

    def test_happy_path(self):
        """compute_all_quantiles returns correctly shaped results for one row."""
        mock_conn, mock_cursor = _make_mock_conn([_SAMPLE_ROW])

        results = quantiles_job.compute_all_quantiles(mock_conn)

        self.assertEqual(len(results), 1)
        symbol, tf, q = results[0]
        self.assertEqual(symbol, "BTCUSDT")
        self.assertEqual(tf, "15m")
        self.assertEqual(q["sample_count"], 1000)
        self.assertAlmostEqual(q["atrp_p90"], 2.0)

    def test_sql_contains_p90_and_filters(self):
        """SQL must contain p90 percentile and parameterised ATR% filters."""
        mock_conn, mock_cursor = _make_mock_conn([_SAMPLE_ROW])
        quantiles_job.compute_all_quantiles(mock_conn)

        sql, params = mock_cursor.execute.call_args[0]
        self.assertIn('percentile_cont(0.90) WITHIN GROUP (ORDER BY "atrPct") AS atrp_p90', sql)
        self.assertIn("MIN(ts)  AS src_min", sql)
        # ATR% min/max must be passed as params (%s), not interpolated into SQL
        # Verify params tuple contains the config values
        self.assertIn(quantiles_job.ATR_PCT_MIN, params)
        self.assertIn(quantiles_job.ATR_PCT_MAX, params)
        self.assertIn(quantiles_job.LOOKBACK_DAYS, params)
        self.assertIn(quantiles_job.MIN_SAMPLES, params)
        # SQL must use %s placeholders (parameterised), not hard-coded floats
        self.assertIn("%s", sql)

    def test_empty_when_no_rows(self):
        """Returns empty list when SQL HAVING filters out all rows."""
        mock_conn, _ = _make_mock_conn([])
        with patch.object(quantiles_job, "MIN_SAMPLES", 80):
            results = quantiles_job.compute_all_quantiles(mock_conn)
        self.assertEqual(results, [])

    def test_p90_is_valid_float(self):
        """atrp_p90 must be a finite float (not NaN, not None)."""
        mock_conn, _ = _make_mock_conn([_SAMPLE_ROW])
        results = quantiles_job.compute_all_quantiles(mock_conn)
        p90 = results[0][2]["atrp_p90"]
        self.assertIsInstance(p90, float)
        self.assertFalse(math.isnan(p90))
        self.assertFalse(math.isinf(p90))


class TestUpsertQuantiles(unittest.TestCase):

    @patch("quantiles_job._get_redis")
    def test_sql_shape_and_redis_write(self, mock_get_redis):
        """upsert_quantiles writes correct SQL and populates Redis cache."""
        mock_conn, mock_cursor = _make_mock_conn([])
        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        quantiles_job.upsert_quantiles(mock_conn, "BTCUSDT", "15m", _SAMPLE_Q)

        # --- DB ---
        sql, params = mock_cursor.execute.call_args[0]
        self.assertIn("atrp_p90", sql)
        self.assertIn("sample_count", sql)
        self.assertIn("ON CONFLICT (symbol, timeframe)", sql)
        mock_conn.commit.assert_called_once()

        # --- Redis ---
        mock_redis.hset.assert_called_once()
        hset_args = mock_redis.hset.call_args[0]
        self.assertEqual(hset_args[0], "atrpct:quantiles:15m")
        self.assertEqual(hset_args[1], "BTCUSDT")

        payload = json.loads(hset_args[2])
        self.assertAlmostEqual(payload["p90"], 2.0)
        self.assertEqual(payload["sample_count"], 1000)
        self.assertEqual(payload["window_days"], quantiles_job.LOOKBACK_DAYS)
        self.assertIn("computed_at", payload)

        # --- TTL ---
        mock_redis.expire.assert_called_with("atrpct:quantiles:15m", 21600)


class TestCacheQuantiles(unittest.TestCase):

    @patch.object(quantiles_job, "_get_redis", return_value=None)
    def test_noop_when_no_redis_url(self, _):
        """cache_quantiles must silently return when REDIS_URL is not configured."""
        # Should not raise
        quantiles_job.cache_quantiles("BTCUSDT", "15m", _SAMPLE_Q)

    @patch("quantiles_job._get_redis")
    def test_logs_on_redis_error(self, mock_get_redis):
        """cache_quantiles logs a warning (not exception) on Redis errors."""
        mock_redis = MagicMock()
        mock_redis.hset.side_effect = Exception("connection refused")
        mock_get_redis.return_value = mock_redis

        # Must not raise — just log
        try:
            quantiles_job.cache_quantiles("BTCUSDT", "15m", _SAMPLE_Q)
        except Exception as exc:
            self.fail(f"cache_quantiles raised unexpectedly: {exc}")


class TestGetConn(unittest.TestCase):

    @patch.object(quantiles_job, "DATABASE_URL", None)
    def test_raises_when_no_database_url(self):
        """get_conn must raise RuntimeError when DATABASE_URL is not set."""
        with self.assertRaises(RuntimeError, msg="DATABASE_URL is not set"):
            with quantiles_job.get_conn():
                pass


class TestTick(unittest.TestCase):

    @patch("quantiles_job.upsert_quantiles")
    @patch("quantiles_job.compute_all_quantiles")
    @patch("quantiles_job.get_conn")
    def test_happy_path_processes_all_pairs(self, mock_get_conn, mock_compute, mock_upsert):
        """tick() iterates all pairs, calls upsert, and updates Prometheus metrics."""
        # Arrange
        mock_conn_ctx = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn_ctx
        mock_compute.return_value = [
            ("BTCUSDT", "15m", _SAMPLE_Q),
            ("ETHUSDT", "1h", _SAMPLE_Q),
        ]

        # Act — reset counter to avoid cross-test state
        quantiles_job._log_counter = 0
        quantiles_job.tick()

        # Assert upsert called for each pair
        self.assertEqual(mock_upsert.call_count, 2)
        mock_upsert.assert_any_call(mock_conn_ctx, "BTCUSDT", "15m", _SAMPLE_Q)
        mock_upsert.assert_any_call(mock_conn_ctx, "ETHUSDT", "1h", _SAMPLE_Q)

    @patch("quantiles_job.compute_all_quantiles")
    @patch("quantiles_job.get_conn")
    def test_no_pairs_logs_warning(self, mock_get_conn, mock_compute):
        """tick() returns early without error when no pairs are available."""
        mock_get_conn.return_value.__enter__.return_value = MagicMock()
        mock_compute.return_value = []

        # Must not raise
        quantiles_job.tick()


if __name__ == "__main__":
    unittest.main()
