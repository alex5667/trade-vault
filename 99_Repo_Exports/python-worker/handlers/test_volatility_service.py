# test_volatility_service.py
"""
Tests for VolatilityService - normalization, bytes handling, cache functionality.
"""
from unittest.mock import Mock, patch
from handlers.volatility_service import VolatilityService


class TestVolatilityService:
    """Test suite for VolatilityService."""

    def test_normalize_timeframe(self):
        """Test timeframe normalization."""
        vs = VolatilityService(redis_client=None, symbol="BTCUSDT")

        # Test various formats
        assert vs._normalize_timeframe("1min") == "1m"
        assert vs._normalize_timeframe("60s") == "1m"
        assert vs._normalize_timeframe("60sec") == "1m"
        assert vs._normalize_timeframe("5MIN") == "5m"
        assert vs._normalize_timeframe("300s") == "5m"
        assert vs._normalize_timeframe("15minute") == "15m"
        assert vs._normalize_timeframe("1HOUR") == "1h"
        assert vs._normalize_timeframe("3600s") == "1h"
        assert vs._normalize_timeframe("4hour") == "4h"
        assert vs._normalize_timeframe("1day") == "1d"
        assert vs._normalize_timeframe("24h") == "1d"
        assert vs._normalize_timeframe("1440m") == "1d"

        # Test already normalized
        assert vs._normalize_timeframe("1m") == "1m"
        assert vs._normalize_timeframe("5m") == "5m"
        assert vs._normalize_timeframe("1h") == "1h"

    def test_to_float_bytes(self):
        """Test conversion from bytes to float."""
        vs = VolatilityService(redis_client=None, symbol="BTCUSDT")

        # Test bytes conversion
        assert vs._to_float(b"12.34") == 12.34
        assert vs._to_float("56.78") == 56.78
        assert vs._to_float(b"0.0001") == 0.0001

        # Test invalid values
        assert vs._to_float(b"") is None
        assert vs._to_float(None) is None
        assert vs._to_float(b"abc") is None
        assert vs._to_float("invalid") is None
        assert vs._to_float(b"0") is None  # Zero is invalid for ATR
        assert vs._to_float(b"-1.5") is None  # Negative is invalid

        # Test edge cases
        assert vs._to_float("1e-6") == 1e-6

    @patch('handlers.volatility_service.VolatilityService._now_ms')
    def test_cache_hit_no_redis_calls(self, mock_now_ms):
        """Test that cache hit prevents Redis calls."""
        redis = Mock()
        # Mock fresh hash data
        redis.hgetall.return_value = {b"v": b"1.23", b"ts": b"1700000000000"}
        redis.get.return_value = None

        vs = VolatilityService(redis_client=redis, symbol="BTCUSDT")

        # Fix time to ensure staleness check passes
        mock_now_ms.return_value = 1700000001000  # +1s from stored ts
        # Override max_stale_ms for test
        vs._max_stale_ms = lambda tf: 999999999

        # First call should load from Redis
        v1 = vs._load_tracker_atr_from_redis("1m", 0)
        assert v1 == 1.23
        assert redis.hgetall.call_count == 1
        assert redis.get.call_count == 0  # Hash worked, no fallback

        # Reset call counts
        redis.reset_mock()

        # Second call should use cache
        v2 = vs._load_tracker_atr_from_redis("1m", 0)
        assert v2 == 1.23
        assert redis.hgetall.call_count == 0  # No Redis calls
        assert redis.get.call_count == 0

    @patch('handlers.volatility_service.VolatilityService._now_ms')
    def test_stale_data_fallback(self, mock_now_ms):
        """Test fallback to legacy when hash data is stale."""
        redis = Mock()
        # Mock stale hash data
        redis.hgetall.return_value = {b"v": b"1.23", b"ts": b"1700000000000"}
        redis.get.return_value = b"2.34"  # Legacy fallback

        vs = VolatilityService(redis_client=redis, symbol="BTCUSDT")

        # Set current time to be very old (stale data)
        mock_now_ms.return_value = 1700000000000 + 24 * 3600 * 1000  # +1 day

        v = vs._load_tracker_atr_from_redis("1m", 0)
        assert v == 2.34  # Should use legacy fallback
        assert redis.hgetall.call_count == 1
        assert redis.get.call_count == 1  # Fallback called

    def test_cache_ttl_by_timeframe(self):
        """Test different cache TTLs for different timeframes."""
        vs = VolatilityService(redis_client=None, symbol="BTCUSDT")

        # Test cache TTLs
        assert vs._cache_ttl_ms("1m") == 1500    # 1.5s
        assert vs._cache_ttl_ms("5m") == 4000    # 4s
        assert vs._cache_ttl_ms("15m") == 8000   # 8s
        assert vs._cache_ttl_ms("1h") == 20000   # 20s
        assert vs._cache_ttl_ms("4h") == 30000   # 30s
        assert vs._cache_ttl_ms("1d") == 60000   # 60s
        assert vs._cache_ttl_ms("unknown") == 2000  # default

    def test_max_stale_ms_by_timeframe(self):
        """Test different staleness thresholds for different timeframes."""
        vs = VolatilityService(redis_client=None, symbol="BTCUSDT")

        # Test staleness thresholds (in milliseconds)
        assert vs._max_stale_ms("1m") == 5 * 60_000      # 5 min
        assert vs._max_stale_ms("5m") == 30 * 60_000     # 30 min
        assert vs._max_stale_ms("15m") == 90 * 60_000    # 90 min
        assert vs._max_stale_ms("1h") == 6 * 3600_000    # 6 hours
        assert vs._max_stale_ms("4h") == 24 * 3600_000   # 24 hours
        assert vs._max_stale_ms("1d") == 7 * 24 * 3600_000  # 7 days
        assert vs._max_stale_ms("unknown") == 30 * 60_000  # default

    @patch('handlers.volatility_service.VolatilityService._now_ms')
    def test_legacy_fallback(self, mock_now_ms):
        """Test fallback to legacy string keys when hash fails."""
        redis = Mock()
        redis.hgetall.return_value = None  # No hash data
        redis.get.return_value = b"3.45"

        vs = VolatilityService(redis_client=redis, symbol="BTCUSDT")
        mock_now_ms.return_value = 1700000000000

        v = vs._load_tracker_atr_from_redis("1m", 0)
        assert v == 3.45
        assert redis.hgetall.call_count == 1
        assert redis.get.call_count == 1

    def test_no_redis_fallback_to_estimate(self):
        """Test fallback to estimation when no Redis data available."""
        vs = VolatilityService(redis_client=None, symbol="BTCUSDT")

        v = vs._load_tracker_atr_from_redis("1m", 0)
        assert v is None  # Should return None when no Redis

        # Test estimation directly
        estimate = vs._estimate_atr(50000.0)
        assert estimate == 50000.0 * 0.0003  # Default ratio
