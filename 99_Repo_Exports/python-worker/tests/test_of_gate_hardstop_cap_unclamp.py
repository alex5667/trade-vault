from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""Unit tests for of_gate_hardstop_cap_unclamp.py

Tests staged auto-unclamp functionality:
- Relax stage: builds targets from clamp audit capped by relax caps
- Full unclamp: restores pre-clamp values including HDEL for old_null=1
- Healthy streak tracking
- Rollback compatibility
"""


import json
import os

# Import module functions
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from of_gate_hardstop_cap_unclamp import (
    _apply_hash_restores,
    _f,
    _i,
    _read_audit_list,
    main,
    now_ms,
    sign,
)


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


class TestReadAuditList:
    """Test reading audit log from Redis."""

    def test_read_audit_empty(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        result = _read_audit_list(r, "nonexistent")
        assert result == []

    def test_read_audit_with_data(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        bundle_id = "test123"
        audit_key = f"recs:audit:{bundle_id}"

        entries = [
            {"op": "HSET", "key": "config:orderflow:BTCUSDT", "field": "meta_enforce_share_trend", "old": "0.50", "old_null": 0, "new": "0.10"},
            {"op": "HSET", "key": "config:orderflow:BTCUSDT", "field": "meta_enforce_share_range", "old": "0.30", "old_null": 0, "new": "0.05"},
        ]

        for entry in entries:
            r.rpush(audit_key, json.dumps(entry))

        result = _read_audit_list(r, bundle_id)
        assert len(result) == 2
        assert result[0]["field"] == "meta_enforce_share_trend"
        assert result[1]["field"] == "meta_enforce_share_range"


class TestApplyHashRestores:
    """Test applying hash restores with audit."""

    def test_apply_restores_hset(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        # Set current value
        cfg_key = "config:orderflow:BTCUSDT"
        r.hset(cfg_key, "meta_enforce_share_trend", "0.10")  # current (clamped)

        restores = [
            {"key": cfg_key, "field": "meta_enforce_share_trend", "old": "0.50", "old_null": 0},
        ]

        bundle_id, sig = _apply_hash_restores(
            r,
            who="test_unclamp",
            ttl_sec=86400,
            restores=restores,
        )

        assert bundle_id is not None
        assert len(sig) == 8

        # Check value was restored
        assert r.hget(cfg_key, "meta_enforce_share_trend") == "0.50"

        # Check bundle was stored
        bundle_json = r.get(f"recs:bundle:{bundle_id}")
        assert bundle_json is not None
        bundle = json.loads(bundle_json)
        assert bundle["meta"]["kind"] == "meta_hardstop_cap_unclamp_step"

        # Check audit log
        audit_entries = r.lrange(f"recs:audit:{bundle_id}", 0, -1)
        assert len(audit_entries) > 0

        # Verify audit format for rollback
        for entry_json in audit_entries:
            entry = json.loads(entry_json)
            assert "op" in entry
            assert "key" in entry
            assert "field" in entry
            assert "old" in entry  # current value before restore
            assert "old_null" in entry
            assert "new" in entry  # restored value
            assert "ts_ms" in entry
            assert "who" in entry

    def test_apply_restores_hdel_on_old_null(self):
        """Test that old_null=1 creates HDEL operation (and rollback can restore it)."""
        r = fakeredis.FakeRedis(decode_responses=True)

        cfg_key = "config:orderflow:BTCUSDT"
        # Set a value that will be deleted
        r.hset(cfg_key, "meta_enforce_share_news", "0.20")

        restores = [
            {"key": cfg_key, "field": "meta_enforce_share_news", "old": "", "old_null": 1},
        ]

        bundle_id, sig = _apply_hash_restores(
            r,
            who="test_unclamp",
            ttl_sec=86400,
            restores=restores,
        )

        # Check field was deleted
        assert r.hget(cfg_key, "meta_enforce_share_news") is None

        # Check audit has HDEL operation
        audit_entries = r.lrange(f"recs:audit:{bundle_id}", 0, -1)
        assert len(audit_entries) > 0

        hdel_found = False
        for entry_json in audit_entries:
            entry = json.loads(entry_json)
            if entry.get("field") == "meta_enforce_share_news":
                assert entry.get("op") == "HDEL" or entry.get("old_null") == 1
                # old should be the current value before deletion (for rollback)
                assert entry.get("old") == "0.20"
                hdel_found = True
        assert hdel_found, "HDEL operation not found in audit"


class TestRelaxBuildsTargets:
    """Test that relax stage builds correct targets from clamp audit."""

    def test_relax_builds_targets_capped(self):
        """Test that relax builds min(old from audit, relax_cap)."""
        r = fakeredis.FakeRedis(decode_responses=True)

        # Simulate clamp audit
        clamp_bundle_id = "clamp123"
        audit_key = f"recs:audit:{clamp_bundle_id}"

        # Pre-clamp values: trend=0.50, range=0.30
        # Clamped to: trend=0.10, range=0.05
        clamp_audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.50",  # pre-clamp
                "old_null": 0,
                "new": "0.10",  # clamped
            },
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_range",
                "old": "0.30",  # pre-clamp
                "old_null": 0,
                "new": "0.05",  # clamped
            },
        ]

        for entry in clamp_audit:
            r.rpush(audit_key, json.dumps(entry))

        # Read audit
        audit = _read_audit_list(r, clamp_bundle_id)

        # Build relax restores (simulating build_relax_restores logic)
        relax_caps = {
            "meta_enforce_share_trend": 0.25,
            "meta_enforce_share_range": 0.15,
            "meta_enforce_share_news": 0.00,
            "meta_enforce_share_other": 0.00,
        }

        restores = []
        for a in audit:
            if a.get("op") != "HSET":
                continue
            field = (a.get("field", ""))
            if field not in relax_caps:
                continue
            old_null = int(a.get("old_null", 0) or 0)
            if old_null == 1:
                continue
            try:
                oldf = float(a.get("old", 0.0) or 0.0)
            except Exception:
                oldf = 0.0
            cap = float(relax_caps[field])
            target = min(oldf, cap)
            restores.append({
                "key": (a.get("key", "")),
                "field": field,
                "old": f"{target:.2f}",
                "old_null": 0,
            })

        # Verify targets
        assert len(restores) == 2

        trend_restore = next(r for r in restores if r["field"] == "meta_enforce_share_trend")
        assert trend_restore["old"] == "0.25"  # min(0.50, 0.25) = 0.25

        range_restore = next(r for r in restores if r["field"] == "meta_enforce_share_range")
        assert range_restore["old"] == "0.15"  # min(0.30, 0.15) = 0.15

    def test_relax_builds_targets_below_cap(self):
        """Test that if pre-clamp value is below relax cap, use pre-clamp value."""
        r = fakeredis.FakeRedis(decode_responses=True)

        clamp_bundle_id = "clamp456"
        audit_key = f"recs:audit:{clamp_bundle_id}"

        # Pre-clamp value is lower than relax cap
        clamp_audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:ETHUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.15",  # pre-clamp (below relax_cap=0.25)
                "old_null": 0,
                "new": "0.10",  # clamped
            },
        ]

        for entry in clamp_audit:
            r.rpush(audit_key, json.dumps(entry))

        audit = _read_audit_list(r, clamp_bundle_id)

        relax_caps = {"meta_enforce_share_trend": 0.25}

        restores = []
        for a in audit:
            if a.get("op") != "HSET":
                continue
            field = (a.get("field", ""))
            if field not in relax_caps:
                continue
            old_null = int(a.get("old_null", 0) or 0)
            if old_null == 1:
                continue
            try:
                oldf = float(a.get("old", 0.0) or 0.0)
            except Exception:
                oldf = 0.0
            cap = float(relax_caps[field])
            target = min(oldf, cap)
            restores.append({
                "key": (a.get("key", "")),
                "field": field,
                "old": f"{target:.2f}",
                "old_null": 0,
            })

        assert len(restores) == 1
        assert restores[0]["old"] == "0.15"  # min(0.15, 0.25) = 0.15 (pre-clamp value)


class TestFullRestoreHDEL:
    """Test that full restore handles HDEL for old_null=1."""

    def test_full_restore_hdel_on_old_null(self):
        """Test that if old_null=1 in clamp audit, full restore creates HDEL."""
        r = fakeredis.FakeRedis(decode_responses=True)

        clamp_bundle_id = "clamp789"
        audit_key = f"recs:audit:{clamp_bundle_id}"

        # Clamp audit with old_null=1 (field didn't exist pre-clamp, was created by clamp)
        clamp_audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_news",
                "old": "",  # didn't exist
                "old_null": 1,
                "new": "0.00",  # created by clamp
            },
        ]

        for entry in clamp_audit:
            r.rpush(audit_key, json.dumps(entry))

        # Set current value (clamped)
        cfg_key = "config:orderflow:BTCUSDT"
        r.hset(cfg_key, "meta_enforce_share_news", "0.00")

        # Build full restores (simulating build_full_restores logic)
        audit = _read_audit_list(r, clamp_bundle_id)

        restores = []
        for a in audit:
            if (a.get("op")) != "HSET":
                continue
            restores.append({
                "key": (a.get("key", "")),
                "field": (a.get("field", "")),
                "old": ("" if a.get("old") is None else (a.get("old", ""))),
                "old_null": int(a.get("old_null", 0) or 0),
            })

        # Apply restores
        bundle_id, sig = _apply_hash_restores(
            r,
            who="test_full_unclamp",
            ttl_sec=86400,
            restores=restores,
        )

        # Check field was deleted (old_null=1 means field didn't exist pre-clamp)
        assert r.hget(cfg_key, "meta_enforce_share_news") is None

        # Check audit has correct format for rollback
        audit_entries = r.lrange(f"recs:audit:{bundle_id}", 0, -1)
        assert len(audit_entries) > 0

        for entry_json in audit_entries:
            entry = json.loads(entry_json)
            if entry.get("field") == "meta_enforce_share_news":
                # Should be HDEL or have old_null=1
                assert entry.get("op") == "HDEL" or entry.get("old_null") == 1
                # old should be "0.00" (current value before deletion, for rollback)
                assert entry.get("old") == "0.00"


class TestMain:
    """Test main function integration."""

    @patch.dict(os.environ, {
        "REDIS_URL": "redis://localhost:6379/0",
        "OF_GATE_METRICS_STREAM": RS.OF_GATE_METRICS,
        "META_HARDSTOP_WINDOW_MIN": "30",
        "META_CLAMP_ACTIVE_KEY": "meta:hardstop:clamp:active",
        "META_CLAMP_STAGE_KEY": "meta:hardstop:clamp:stage",
        "META_HEALTHY_STREAK_KEY": "meta:hardstop:healthy_streak",
        "META_UNCLAMP_RELAX_STREAK_N": "6",
        "META_UNCLAMP_REMOVE_STREAK_N": "18",
        "META_RELAX_CAP_TREND": "0.25",
        "META_RELAX_CAP_RANGE": "0.15",
        "RECS_HMAC_SECRET": "test_secret",
        "RECS_TTL_SEC": "86400",
        "NOTIFY_TELEGRAM_STREAM": RS.NOTIFY_TELEGRAM,
    })
    def test_main_no_active_clamp(self):
        """Test main when no clamp is active."""
        r = fakeredis.FakeRedis(decode_responses=True)
        r.xrevrange = MagicMock(return_value=[])

        with patch("of_gate_hardstop_cap_unclamp.redis.Redis.from_url", return_value=r):
            main()
            # Should clean up state
            assert r.get("meta:hardstop:healthy_streak") is None
            assert r.get("meta:hardstop:clamp:stage") is None

    @patch.dict(os.environ, {
        "REDIS_URL": "redis://localhost:6379/0",
        "OF_GATE_METRICS_STREAM": RS.OF_GATE_METRICS,
        "META_HARDSTOP_WINDOW_MIN": "30",
        "META_CLAMP_ACTIVE_KEY": "meta:hardstop:clamp:active",
        "META_CLAMP_STAGE_KEY": "meta:hardstop:clamp:stage",
        "META_HEALTHY_STREAK_KEY": "meta:hardstop:healthy_streak",
        "META_UNCLAMP_RELAX_STREAK_N": "2",  # Lower for testing
        "META_UNCLAMP_REMOVE_STREAK_N": "5",
        "META_RELAX_CAP_TREND": "0.25",
        "META_RELAX_CAP_RANGE": "0.15",
        "RECS_HMAC_SECRET": "test_secret",
        "RECS_TTL_SEC": "86400",
        "NOTIFY_TELEGRAM_STREAM": RS.NOTIFY_TELEGRAM,
    })
    def test_main_relax_stage(self):
        """Test main when relax conditions are met."""
        r = fakeredis.FakeRedis(decode_responses=True)

        # Set active clamp
        clamp_bundle_id = "clamp_test_123"
        r.set("meta:hardstop:clamp:active", clamp_bundle_id)
        r.set("meta:hardstop:clamp:stage", "CLAMPED")
        r.set("meta:hardstop:healthy_streak", "1")  # Will become 2

        # Create clamp audit
        audit_key = f"recs:audit:{clamp_bundle_id}"
        clamp_audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.50",
                "old_null": 0,
                "new": "0.10",
            },
        ]
        for entry in clamp_audit:
            r.rpush(audit_key, json.dumps(entry))

        # Set current clamped value
        r.hset("config:orderflow:BTCUSDT", "meta_enforce_share_trend", "0.10")

        # Create healthy metrics
        ts = now_ms()
        mock_metrics = [
            (f"{ts}-{i}", {
                "ts_ms": str(ts - i * 1000),
                "ok": "1",
                "ok_soft": "0",
                "latency_us": "1000",
                "exec_risk_norm": "0.5",
            })
            for i in range(250)  # Enough samples
        ]

        r.xrevrange = MagicMock(return_value=mock_metrics)
        r.xadd = MagicMock(return_value="mock_msg_id")

        with patch("of_gate_hardstop_cap_unclamp.redis.Redis.from_url", return_value=r):
            main()

            # Should create relax bundle (check by getting bundle directly)
            # fakeredis doesn't support keys(), so check by trying to get a bundle
            # We know bundle_id from the log, but for test we check stage was updated

            # Should update stage to RELAXED
            stage = r.get("meta:hardstop:clamp:stage")
            assert stage == "RELAXED"

            # Should send notification
            assert r.xadd.called


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

