from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""Unit tests for of_gate_hardstop_cap_clamp.py

Tests emergency cap-clamp functionality:
- Hard-stop detection
- Streak tracking
- Bundle creation and audit log
- Rollback compatibility
"""


import json
import os

# Import module functions
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from of_gate_hardstop_cap_clamp import (
    _f,
    _i,
    apply_emergency_bundle,
    hard_stop,
    main,
    now_ms,
    pctl,
    read_metrics_window,
    sign,
    summarize_health,
)


class TestPercentile:
    """Test percentile calculation."""

    def test_pctl_empty(self):
        assert pctl([], 0.5) == 0.0

    def test_pctl_single(self):
        assert pctl([1.0], 0.5) == 1.0

    def test_pctl_basic(self):
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert pctl(xs, 0.0) == 1.0
        assert pctl(xs, 0.5) == 3.0
        assert pctl(xs, 1.0) == 5.0
        assert pctl(xs, 0.99) == 5.0


class TestTypeConversions:
    """Test type conversion helpers."""

    def test_f(self):
        assert _f("1.5") == 1.5
        assert _f(1.5) == 1.5
        assert _f(None, 0.0) == 0.0
        assert _f("invalid", 2.0) == 2.0

    def test_i(self):
        assert _i("42") == 42
        assert _i(42) == 42
        assert _i(None, 0) == 0
        assert _i("invalid", 5) == 5


class TestSign:
    """Test HMAC signature."""

    def test_sign(self):
        bundle_id = "abc123"
        secret = "test_secret"
        sig = sign(bundle_id, secret)
        assert len(sig) == 8
        assert isinstance(sig, str)
        # Same input should produce same signature
        assert sign(bundle_id, secret) == sig
        # Different secret should produce different signature
        assert sign(bundle_id, "different") != sig


class TestReadMetricsWindow:
    """Test reading metrics from Redis stream."""

    def test_read_metrics_empty(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        r.xrevrange = MagicMock(return_value=[])
        rows = read_metrics_window(r, "metrics:of_gate", now_ms() - 3600000, max_scan=1000)
        assert rows == []

    def test_read_metrics_with_data(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        ts = now_ms()
        # Create mock stream data
        mock_data = [
            (f"{ts}-0", {"ts_ms": str(ts - 1000), "ok": "1", "latency_us": "5000"}),
            (f"{ts}-1", {"ts_ms": str(ts - 2000), "ok": "0", "latency_us": "8000"}),
        ]
        r.xrevrange = MagicMock(side_effect=[mock_data, []])
        rows = read_metrics_window(r, "metrics:of_gate", ts - 5000, max_scan=1000)
        assert len(rows) == 2
        assert rows[0]["_ts_ms"] == ts - 2000
        assert rows[1]["_ts_ms"] == ts - 1000


class TestSummarizeHealth:
    """Test health metrics summarization."""

    def test_summarize_health_empty(self):
        result = summarize_health([])
        assert result["n"] == 0.0

    def test_summarize_health_basic(self):
        rows = [
            {"ok": "1", "ok_soft": "0", "latency_us": "1000", "exec_risk_norm": "0.5"},
            {"ok": "1", "ok_soft": "1", "latency_us": "2000", "exec_risk_norm": "0.6"},
            {"ok": "0", "ok_soft": "0", "latency_us": "3000", "exec_risk_norm": "0.7"},
        ]
        result = summarize_health(rows)
        assert result["n"] == 3.0
        assert result["ok_rate"] == pytest.approx(2.0 / 3.0, abs=0.01)
        assert result["soft_rate"] == pytest.approx(1.0 / 3.0, abs=0.01)
        assert result["lat_p99_us"] > 0
        assert result["exec_p90"] > 0


class TestHardStop:
    """Test hard-stop detection."""

    def test_hard_stop_no_data(self):
        health = {"n": 0.0}
        is_hs, reasons = hard_stop(health)
        assert is_hs is True
        assert "low_n" in str(reasons)

    def test_hard_stop_low_n(self):
        with patch.dict(os.environ, {"META_HARDSTOP_MIN_N": "200"}):
            health = {"n": 100.0, "lat_p99_us": 5000.0, "exec_p90": 0.5, "soft_rate": 0.3, "ok_rate": 0.5}
            is_hs, reasons = hard_stop(health)
            assert is_hs is True
            assert any("low_n" in r for r in reasons)

    def test_hard_stop_high_latency(self):
        with patch.dict(os.environ, {"META_HARDSTOP_LAT_P99_US": "12000"}):
            health = {"n": 300.0, "lat_p99_us": 15000.0, "exec_p90": 0.5, "soft_rate": 0.3, "ok_rate": 0.5}
            is_hs, reasons = hard_stop(health)
            assert is_hs is True
            assert any("lat_p99" in r for r in reasons)

    def test_hard_stop_high_exec(self):
        with patch.dict(os.environ, {"META_HARDSTOP_EXEC_P90": "0.92"}):
            health = {"n": 300.0, "lat_p99_us": 5000.0, "exec_p90": 0.95, "soft_rate": 0.3, "ok_rate": 0.5}
            is_hs, reasons = hard_stop(health)
            assert is_hs is True
            assert any("exec_p90" in r for r in reasons)

    def test_hard_stop_ok(self):
        health = {"n": 300.0, "lat_p99_us": 5000.0, "exec_p90": 0.5, "soft_rate": 0.3, "ok_rate": 0.5}
        is_hs, reasons = hard_stop(health)
        assert is_hs is False
        assert len(reasons) == 0


class TestApplyEmergencyBundle:
    """Test emergency bundle creation and application."""

    def test_apply_bundle_basic(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        # Set initial values
        cfg_key = "config:orderflow:BTCUSDT"
        r.hset(cfg_key, "meta_enforce_share_trend", "0.50")
        r.hset(cfg_key, "meta_enforce_share_range", "0.30")

        symbols = ["BTCUSDT"]
        caps = {"trend": 0.10, "range": 0.05, "news": 0.00, "other": 0.00}

        bundle_id, sig = apply_emergency_bundle(
            r,
            symbols=symbols,
            cfg_prefix="config:orderflow:",
            secret="test_secret",
            ttl_sec=86400,
            caps=caps,
        )

        assert bundle_id is not None
        assert len(bundle_id) == 12  # 6 bytes hex
        assert len(sig) == 8

        # Check bundle was stored
        bundle_json = r.get(f"recs:bundle:{bundle_id}")
        assert bundle_json is not None
        bundle = json.loads(bundle_json)
        assert bundle["id"] == bundle_id
        assert bundle["meta"]["kind"] == "meta_hardstop_cap_clamp"

        # Check status
        status = r.get(f"recs:status:{bundle_id}")
        assert status == "APPLIED"

        # Check audit log
        audit_entries = r.lrange(f"recs:audit:{bundle_id}", 0, -1)
        assert len(audit_entries) > 0

        # Check values were clamped
        assert float(r.hget(cfg_key, "meta_enforce_share_trend")) == 0.10  # clamped from 0.50
        assert float(r.hget(cfg_key, "meta_enforce_share_range")) == 0.05  # clamped from 0.30
        assert r.hget(cfg_key, "meta_enforce_per_regime") == "1"

    def test_apply_bundle_clamp_never_increases(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        cfg_key = "config:orderflow:ETHUSDT"
        # Set value lower than cap
        r.hset(cfg_key, "meta_enforce_share_trend", "0.05")

        symbols = ["ETHUSDT"]
        caps = {"trend": 0.10, "range": 0.05, "news": 0.00, "other": 0.00}

        bundle_id, _ = apply_emergency_bundle(
            r,
            symbols=symbols,
            cfg_prefix="config:orderflow:",
            secret="test_secret",
            ttl_sec=86400,
            caps=caps,
        )

        # Should keep original value (0.05 < 0.10 cap)
        assert float(r.hget(cfg_key, "meta_enforce_share_trend")) == 0.05

    def test_apply_bundle_missing_field(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        cfg_key = "config:orderflow:BTCUSDT"
        # No existing values

        symbols = ["BTCUSDT"]
        caps = {"trend": 0.10, "range": 0.05, "news": 0.00, "other": 0.00}

        bundle_id, _ = apply_emergency_bundle(
            r,
            symbols=symbols,
            cfg_prefix="config:orderflow:",
            secret="test_secret",
            ttl_sec=86400,
            caps=caps,
        )

        # Should set to cap (fail-closed)
        assert float(r.hget(cfg_key, "meta_enforce_share_trend")) == 0.10
        assert float(r.hget(cfg_key, "meta_enforce_share_range")) == 0.05

    def test_apply_bundle_audit_format(self):
        """Test that audit log format is compatible with recs_callback_worker rollback."""
        r = fakeredis.FakeRedis(decode_responses=True)

        cfg_key = "config:orderflow:BTCUSDT"
        r.hset(cfg_key, "meta_enforce_share_trend", "0.50")

        symbols = ["BTCUSDT"]
        caps = {"trend": 0.10, "range": 0.05, "news": 0.00, "other": 0.00}

        bundle_id, _ = apply_emergency_bundle(
            r,
            symbols=symbols,
            cfg_prefix="config:orderflow:",
            secret="test_secret",
            ttl_sec=86400,
            caps=caps,
        )

        # Check audit format matches recs_callback_worker expectations
        audit_entries = r.lrange(f"recs:audit:{bundle_id}", 0, -1)
        assert len(audit_entries) > 0

        for entry_json in audit_entries:
            entry = json.loads(entry_json)
            assert "op" in entry
            assert entry["op"] == "HSET"
            assert "key" in entry
            assert "field" in entry
            assert "old" in entry
            assert "old_null" in entry
            assert "new" in entry
            assert "ts_ms" in entry
            assert "who" in entry


class TestMain:
    """Test main function integration."""

    @patch.dict(os.environ, {
        "REDIS_URL": "redis://localhost:6379/0",
        "OF_GATE_METRICS_STREAM": "metrics:of_gate",
        "META_HARDSTOP_WINDOW_MIN": "30",
        "META_HARDSTOP_STREAK_N": "3",
        "META_CLAMP_CAP_TREND": "0.10",
        "META_CLAMP_CAP_RANGE": "0.05",
        "CANARY_SYMBOLS": "BTCUSDT,ETHUSDT",
        "CFG_HASH_PREFIX": "config:orderflow:",
        "RECS_HMAC_SECRET": "test_secret",
        "RECS_TTL_SEC": "86400",
        "NOTIFY_TELEGRAM_STREAM": RS.NOTIFY_TELEGRAM,
    })
    def test_main_no_symbols(self):
        """Test main with no symbols configured."""
        with patch.dict(os.environ, {"CANARY_SYMBOLS": "", "META_CLAMP_SYMBOLS": ""}):
            r = fakeredis.FakeRedis(decode_responses=True)
            with patch("of_gate_hardstop_cap_clamp.get_redis", return_value=r), \
                 patch("of_gate_hardstop_cap_clamp.wait_for_redis", return_value=True):
                # Should return early without error
                main()

    @patch.dict(os.environ, {
        "REDIS_URL": "redis://localhost:6379/0",
        "OF_GATE_METRICS_STREAM": "metrics:of_gate",
        "META_HARDSTOP_WINDOW_MIN": "30",
        "META_HARDSTOP_STREAK_N": "3",
        "META_CLAMP_CAP_TREND": "0.10",
        "META_CLAMP_CAP_RANGE": "0.05",
        "CANARY_SYMBOLS": "BTCUSDT",
        "CFG_HASH_PREFIX": "config:orderflow:",
        "RECS_HMAC_SECRET": "test_secret",
        "RECS_TTL_SEC": "86400",
        "NOTIFY_TELEGRAM_STREAM": RS.NOTIFY_TELEGRAM,
    })
    def test_main_no_hard_stop(self):
        """Test main when no hard-stop detected."""
        r = fakeredis.FakeRedis(decode_responses=True)
        r.xrevrange = MagicMock(side_effect=[[], []])

        with patch("of_gate_hardstop_cap_clamp.get_redis", return_value=r), \
             patch("of_gate_hardstop_cap_clamp.wait_for_redis", return_value=True):
            main()
            # Should not create bundle
            keys = list(r.scan_iter("recs:bundle:*"))
            assert len(keys) == 0

    @patch.dict(os.environ, {
        "REDIS_URL": "redis://localhost:6379/0",
        "OF_GATE_METRICS_STREAM": "metrics:of_gate",
        "META_HARDSTOP_WINDOW_MIN": "30",
        "META_HARDSTOP_STREAK_N": "2",  # Lower threshold for testing
        "META_HARDSTOP_MIN_N": "200",
        "META_CLAMP_CAP_TREND": "0.10",
        "META_CLAMP_CAP_RANGE": "0.05",
        "CANARY_SYMBOLS": "BTCUSDT",
        "CFG_HASH_PREFIX": "config:orderflow:",
        "RECS_HMAC_SECRET": "test_secret",
        "RECS_TTL_SEC": "86400",
        "NOTIFY_TELEGRAM_STREAM": RS.NOTIFY_TELEGRAM,
        "META_CLAMP_ACTIVE_KEY": "meta:hardstop:clamp:active",
    })
    def test_main_apply_clamp(self):
        """Test main when hard-stop detected and streak reached."""
        r = fakeredis.FakeRedis(decode_responses=True)

        # Set streak to required value
        r.set("meta:hardstop:streak", "2")

        # Create mock metrics with hard-stop conditions
        ts = now_ms()
        mock_metrics = [
            (f"{ts}-0", {
                "ts_ms": str(ts - 1000),
                "ok": "0",  # Low ok rate
                "ok_soft": "1",
                "latency_us": "15000",  # High latency
                "exec_risk_norm": "0.95",  # High exec risk
            }),
        ] * 250  # Enough samples

        r.xrevrange = MagicMock(side_effect=[mock_metrics, []])
        r.xadd = MagicMock(return_value="mock_msg_id")

        with patch("of_gate_hardstop_cap_clamp.get_redis", return_value=r), \
             patch("of_gate_hardstop_cap_clamp.wait_for_redis", return_value=True):
            main()

            # Should create bundle
            keys = list(r.scan_iter("recs:bundle:*"))
            assert len(keys) == 1

            # Should set active key
            active = r.get("meta:hardstop:clamp:active")
            assert active is not None

            # Should send notification
            assert r.xadd.called


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

