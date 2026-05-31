from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""Unit tests for nightly_meta_enforce_ramp_bundle.py.

Tests progressive share ramp-up with safety gates.
"""

from unittest.mock import patch

import fakeredis
import pytest


def now_ms() -> int:
    """Returns current timestamp in milliseconds."""
    return get_ny_time_millis()


@pytest.fixture
def mock_redis():
    """Create a fake Redis instance for testing."""
    return fakeredis.FakeRedis(decode_responses=True)


def test_ramp_streak_gate_fails(mock_redis, monkeypatch):
    """Test that ramp is skipped when streak < min_streak."""
    mock_redis.set("sre:regress:pass_streak", "2")
    mock_redis.set("sre:regress:last_status", "PASS")
    mock_redis.set("sre:regress:last_ts_ms", str(now_ms()))

    # Set current share to 0.10
    mock_redis.hset("config:orderflow:BTCUSDT", "meta_enforce_share", "0.10")

    monkeypatch.setenv("META_ENFORCE_MIN_STREAK", "3")
    monkeypatch.setenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30")
    monkeypatch.setenv("REGRESS_PASS_STREAK_KEY", "sre:regress:pass_streak")
    monkeypatch.setenv("REGRESS_LAST_STATUS_KEY", "sre:regress:last_status")
    monkeypatch.setenv("REGRESS_LAST_TS_KEY", "sre:regress:last_ts_ms")
    monkeypatch.setenv("META_ENFORCE_RAMP_NOTIFY_ON_SKIP", "0")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("CANARY_SYMBOLS", "BTCUSDT")
    monkeypatch.setenv("CFG_HASH_PREFIX", "config:orderflow:")

    with patch("tools.nightly_meta_enforce_ramp_bundle.redis.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = mock_redis

        from tools.nightly_meta_enforce_ramp_bundle import main

        # Should return early without creating bundle
        main()

        bundle_keys = [k for k in mock_redis.keys() if k.startswith("recs:bundle:")]
        assert len(bundle_keys) == 0, "Ramp should be skipped when streak < min_streak"


def test_ramp_recent_emergency_fails(mock_redis, monkeypatch):
    """Test that ramp is skipped when recent emergency occurred."""
    # Setup: streak passes
    mock_redis.set("sre:regress:pass_streak", "3")
    mock_redis.set("sre:regress:last_status", "PASS")
    mock_redis.set("sre:regress:last_ts_ms", str(now_ms()))

    # Recent emergency (12 hours ago, less than 24h threshold)
    recent_emerg_ms = now_ms() - int(12 * 3600 * 1000)
    mock_redis.set("sre:of_gate:emergency:last_ms", str(recent_emerg_ms))

    # Set current share to 0.10
    mock_redis.hset("config:orderflow:BTCUSDT", "meta_enforce_share", "0.10")

    monkeypatch.setenv("META_ENFORCE_MIN_STREAK", "3")
    monkeypatch.setenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30")
    monkeypatch.setenv("META_ENFORCE_RAMP_MIN_HOURS_SINCE_LAST_EMERG", "24")
    monkeypatch.setenv("EMERG_COOLDOWN_KEY", "sre:of_gate:emergency:last_ms")
    monkeypatch.setenv("META_ENFORCE_RAMP_NOTIFY_ON_SKIP", "0")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("CANARY_SYMBOLS", "BTCUSDT")
    monkeypatch.setenv("CFG_HASH_PREFIX", "config:orderflow:")

    with patch("tools.nightly_meta_enforce_ramp_bundle.redis.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = mock_redis

        from tools.nightly_meta_enforce_ramp_bundle import main

        # Should return early without creating bundle
        main()

        bundle_keys = [k for k in mock_redis.keys() if k.startswith("recs:bundle:")]
        assert len(bundle_keys) == 0, "Ramp should be skipped when recent emergency occurred"


def test_ramp_proposes_next_share(mock_redis, monkeypatch):
    """Test that ramp proposes next share in schedule."""
    # Setup: gates pass
    mock_redis.set("sre:regress:pass_streak", "3")
    mock_redis.set("sre:regress:last_status", "PASS")
    mock_redis.set("sre:regress:last_ts_ms", str(now_ms()))

    # No recent emergency
    old_emerg_ms = now_ms() - int(48 * 3600 * 1000)  # 48 hours ago
    mock_redis.set("sre:of_gate:emergency:last_ms", str(old_emerg_ms))

    # Set current share to 0.10
    mock_redis.hset("config:orderflow:BTCUSDT", "meta_enforce_share", "0.10")

    monkeypatch.setenv("META_ENFORCE_MIN_STREAK", "3")
    monkeypatch.setenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30")
    monkeypatch.setenv("META_ENFORCE_RAMP_MIN_HOURS_SINCE_LAST_EMERG", "24")
    monkeypatch.setenv("EMERG_COOLDOWN_KEY", "sre:of_gate:emergency:last_ms")
    monkeypatch.setenv("META_ENFORCE_SHARE_SCHEDULE", "0.10,0.25,0.50,1.00")
    monkeypatch.setenv("META_ENFORCE_RAMP_NOTIFY_ON_SKIP", "0")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("CANARY_SYMBOLS", "BTCUSDT")
    monkeypatch.setenv("CFG_HASH_PREFIX", "config:orderflow:")
    monkeypatch.setenv("RECS_HMAC_SECRET", "test_secret")
    monkeypatch.setenv("RECS_TTL_SEC", "86400")
    monkeypatch.setenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
    monkeypatch.setenv("META_ENFORCE_SALT", "enf_v1")

    with patch("tools.nightly_meta_enforce_ramp_bundle.redis.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = mock_redis

        from tools.nightly_meta_enforce_ramp_bundle import main

        # Should create bundle with share=0.25
        main()

        bundle_keys = [k for k in mock_redis.keys() if k.startswith("recs:bundle:")]
        assert len(bundle_keys) == 1, "Ramp should create bundle when gates pass"

        # Check bundle content
        import json
        bundle_raw = mock_redis.get(bundle_keys[0])
        bundle = json.loads(bundle_raw)

        assert bundle["meta"]["from_share"] == 0.10
        assert bundle["meta"]["to_share"] == 0.25
        assert "meta_enforce_share" in str(bundle["ops"])


def test_ramp_already_at_max(mock_redis, monkeypatch):
    """Test that ramp does nothing when already at max share."""
    # Setup: gates pass
    mock_redis.set("sre:regress:pass_streak", "3")
    mock_redis.set("sre:regress:last_status", "PASS")
    mock_redis.set("sre:regress:last_ts_ms", str(now_ms()))

    # No recent emergency
    old_emerg_ms = now_ms() - int(48 * 3600 * 1000)
    mock_redis.set("sre:of_gate:emergency:last_ms", str(old_emerg_ms))

    # Set current share to 1.00 (max)
    mock_redis.hset("config:orderflow:BTCUSDT", "meta_enforce_share", "1.00")

    monkeypatch.setenv("META_ENFORCE_MIN_STREAK", "3")
    monkeypatch.setenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30")
    monkeypatch.setenv("META_ENFORCE_RAMP_MIN_HOURS_SINCE_LAST_EMERG", "24")
    monkeypatch.setenv("EMERG_COOLDOWN_KEY", "sre:of_gate:emergency:last_ms")
    monkeypatch.setenv("META_ENFORCE_SHARE_SCHEDULE", "0.10,0.25,0.50,1.00")
    monkeypatch.setenv("META_ENFORCE_RAMP_NOTIFY_ON_SKIP", "0")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("CANARY_SYMBOLS", "BTCUSDT")
    monkeypatch.setenv("CFG_HASH_PREFIX", "config:orderflow:")

    with patch("tools.nightly_meta_enforce_ramp_bundle.redis.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = mock_redis

        from tools.nightly_meta_enforce_ramp_bundle import main

        # Should return early without creating bundle
        main()

        bundle_keys = [k for k in mock_redis.keys() if k.startswith("recs:bundle:")]
        assert len(bundle_keys) == 0, "Ramp should not create bundle when already at max share"

