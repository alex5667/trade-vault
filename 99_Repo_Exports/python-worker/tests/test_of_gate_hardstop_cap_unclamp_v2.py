from __future__ import annotations

"""Unit tests for of_gate_hardstop_cap_unclamp_v2.py

Tests staged auto-unclamp v2 functionality:
- Dual-window health check (30min + 2h)
- AUTO mode: auto-applies actions
- PROPOSE mode: creates bundle, waits for callback worker
- Side-effects (stage/clear active) executed after bundle APPLIED
- Pending proposal lifecycle
"""


import json
import os

# Import module functions
from unittest.mock import MagicMock, patch

import fakeredis

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from of_gate_hardstop_cap_unclamp_v2 import (
    _apply_restores_direct,
    _create_proposal_bundle,
    _f,
    _i,
    _mode,
    _read_audit_list,
    build_full_restore_ops_from_clamp_audit,
    build_relax_ops_from_clamp_audit,
    is_unhealthy,
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


class TestIsUnhealthy:
    """Test dual-window unhealthy detection."""

    def test_is_unhealthy_low_n(self):
        with patch.dict(os.environ, {"META_HARDSTOP_MIN_N": "200"}):
            health = {"n": 100.0, "lat_p99_us": 5000.0, "exec_p90": 0.5, "soft_rate": 0.3, "ok_rate": 0.5}
            is_bad, reasons = is_unhealthy(health, prefix="w30")
            assert is_bad is True
            assert any("w30:low_n" in r for r in reasons)

    def test_is_unhealthy_high_latency(self):
        with patch.dict(os.environ, {"META_HARDSTOP_LAT_P99_US": "12000"}):
            health = {"n": 300.0, "lat_p99_us": 15000.0, "exec_p90": 0.5, "soft_rate": 0.3, "ok_rate": 0.5}
            is_bad, reasons = is_unhealthy(health, prefix="w30")
            assert is_bad is True
            assert any("w30:lat_p99" in r for r in reasons)

    def test_is_unhealthy_healthy(self):
        with patch.dict(os.environ, {
            "META_HARDSTOP_MIN_N": "200",
            "META_HARDSTOP_LAT_P99_US": "12000",
            "META_HARDSTOP_EXEC_P90": "0.92",
            "META_HARDSTOP_SOFT_RATE": "0.60",
            "META_HARDSTOP_OK_RATE_MIN": "0.10",
        }):
            health = {"n": 300.0, "lat_p99_us": 5000.0, "exec_p90": 0.5, "soft_rate": 0.3, "ok_rate": 0.5}
            is_bad, reasons = is_unhealthy(health, prefix="w30")
            assert is_bad is False
            assert len(reasons) == 0


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


class TestApplyRestoresDirect:
    """Test AUTO mode: direct apply with audit."""

    def test_apply_restores_direct_hset(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        # Set current value
        cfg_key = "config:orderflow:BTCUSDT"
        r.hset(cfg_key, "meta_enforce_share_trend", "0.10")  # current (clamped)

        restores = [
            {"op": "HSET", "key": cfg_key, "field": "meta_enforce_share_trend", "value": "0.50"},
        ]

        with patch.dict(os.environ, {"RECS_HMAC_SECRET": "test_secret"}):
            bundle_id, sig = _apply_restores_direct(
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

        # Check status is APPLIED
        assert r.get(f"recs:status:{bundle_id}") == "APPLIED"

        # Check audit log
        audit_entries = r.lrange(f"recs:audit:{bundle_id}", 0, -1)
        assert len(audit_entries) > 0


class TestCreateProposalBundle:
    """Test PROPOSE mode: create bundle with PENDING status."""

    def test_create_proposal_bundle(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        ops = [
            {"op": "HSET", "key": "config:orderflow:BTCUSDT", "field": "meta_enforce_share_trend", "value": "0.50"},
        ]
        meta = {"kind": "meta_hardstop_cap_unclamp_relax", "clamp_id": "clamp123"}

        with patch.dict(os.environ, {"RECS_HMAC_SECRET": "test_secret"}):
            bundle_id, sig = _create_proposal_bundle(
                r,
                who="test_unclamp",
                ttl_sec=86400,
                ops=ops,
                meta=meta,
            )

        assert bundle_id is not None
        assert len(sig) == 8

        # Check bundle was stored
        bundle_json = r.get(f"recs:bundle:{bundle_id}")
        assert bundle_json is not None
        bundle = json.loads(bundle_json)
        assert bundle["meta"]["kind"] == "meta_hardstop_cap_unclamp_relax"

        # Check status is PENDING (not APPLIED)
        assert r.get(f"recs:status:{bundle_id}") == "PENDING"


class TestMode:
    """Test mode detection (AUTO vs PROPOSE)."""

    def test_mode_default_auto(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch.dict(os.environ, {"META_UNCLAMP_MODE": "AUTO"}):
            assert _mode(r) == "AUTO"

    def test_mode_env_propose(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch.dict(os.environ, {"META_UNCLAMP_MODE": "PROPOSE"}):
            assert _mode(r) == "PROPOSE"

    def test_mode_redis_override(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        r.set("cfg:meta_unclamp:mode", "PROPOSE")
        with patch.dict(os.environ, {"META_UNCLAMP_MODE": "AUTO", "META_UNCLAMP_MODE_KEY": "cfg:meta_unclamp:mode"}):
            assert _mode(r) == "PROPOSE"


class TestBuildRelaxOps:
    """Test building relax operations from clamp audit."""

    def test_build_relax_ops_capped(self):
        clamp_audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.50",
                "old_null": 0,
            },
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_range",
                "old": "0.30",
                "old_null": 0,
            },
        ]

        relax_caps = {
            "meta_enforce_share_trend": 0.25,
            "meta_enforce_share_range": 0.15,
        }

        ops = build_relax_ops_from_clamp_audit(clamp_audit, relax_caps)

        assert len(ops) == 2
        # trend: min(0.50, 0.25) = 0.25
        trend_op = next(op for op in ops if op["field"] == "meta_enforce_share_trend")
        assert trend_op["value"] == "0.25"
        # range: min(0.30, 0.15) = 0.15
        range_op = next(op for op in ops if op["field"] == "meta_enforce_share_range")
        assert range_op["value"] == "0.15"

    def test_build_relax_ops_skips_old_null(self):
        clamp_audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.50",
                "old_null": 0,
            },
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_news",
                "old": "",
                "old_null": 1,  # didn't exist pre-clamp
            },
        ]

        relax_caps = {
            "meta_enforce_share_trend": 0.25,
            "meta_enforce_share_news": 0.00,
        }

        ops = build_relax_ops_from_clamp_audit(clamp_audit, relax_caps)

        # Should skip old_null=1
        assert len(ops) == 1
        assert ops[0]["field"] == "meta_enforce_share_trend"


class TestBuildFullRestoreOps:
    """Test building full restore operations from clamp audit."""

    def test_build_full_restore_ops(self):
        clamp_audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.50",
                "old_null": 0,
            },
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_news",
                "old": "",
                "old_null": 1,  # should become HDEL
            },
        ]

        ops = build_full_restore_ops_from_clamp_audit(clamp_audit)

        assert len(ops) == 2
        # trend: HSET with old value
        trend_op = next(op for op in ops if op["field"] == "meta_enforce_share_trend")
        assert trend_op["op"] == "HSET"
        assert trend_op["value"] == "0.50"
        # news: HDEL (old_null=1)
        news_op = next(op for op in ops if op["field"] == "meta_enforce_share_news")
        assert news_op["op"] == "HDEL"


class TestMainDualWindow:
    """Test main logic with dual-window health check."""

    def test_main_no_clamp_active(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        # No clamp active -> should clean up state
        main()
        # Should not crash

    def test_main_dual_window_both_unhealthy(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        # Set up clamp active
        clamp_bundle_id = "clamp123"
        r.set("meta:hardstop:clamp:active", clamp_bundle_id)

        # Create clamp audit
        audit_key = f"recs:audit:{clamp_bundle_id}"
        r.rpush(audit_key, json.dumps({
            "op": "HSET",
            "key": "config:orderflow:BTCUSDT",
            "field": "meta_enforce_share_trend",
            "old": "0.50",
            "old_null": 0,
            "new": "0.10",
        }))

        # Mock metrics stream (both windows unhealthy)
        def mock_xrevrange(stream, max=None, min=None, count=None):
            ts = now_ms()
            # Return unhealthy metrics (high latency)
            return [
                (f"{ts}-0", {"ts_ms": str(ts - 1000), "ok": "0", "latency_us": "15000", "exec_risk_norm": "0.95", "ok_soft": "1"}),
            ]

        r.xrevrange = MagicMock(side_effect=mock_xrevrange)

        with patch.dict(os.environ, {
            "REDIS_URL": "redis://localhost:6379/0",
            "META_UNCLAMP_SHORT_WINDOW_MIN": "30",
            "META_UNCLAMP_LONG_WINDOW_MIN": "120",
            "META_HARDSTOP_MIN_N": "200",
            "META_HARDSTOP_LAT_P99_US": "12000",
            "RECS_HMAC_SECRET": "test_secret",
        }):
            main()

        # Streak should be reset to 0
        streak = r.get("meta:hardstop:healthy_streak")
        assert streak == "0" or streak is None

    def test_main_dual_window_both_healthy_streak_insufficient(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        # Set up clamp active
        clamp_bundle_id = "clamp123"
        r.set("meta:hardstop:clamp:active", clamp_bundle_id)
        r.set("meta:hardstop:clamp:stage", "CLAMPED")
        r.set("meta:hardstop:healthy_streak", "3")  # below relax_n=6

        # Create clamp audit
        audit_key = f"recs:audit:{clamp_bundle_id}"
        r.rpush(audit_key, json.dumps({
            "op": "HSET",
            "key": "config:orderflow:BTCUSDT",
            "field": "meta_enforce_share_trend",
            "old": "0.50",
            "old_null": 0,
            "new": "0.10",
        }))

        # Mock metrics stream (both windows healthy)
        def mock_xrevrange(stream, max=None, min=None, count=None):
            ts = now_ms()
            # Return healthy metrics
            return [
                (f"{ts}-0", {"ts_ms": str(ts - 1000), "ok": "1", "latency_us": "5000", "exec_risk_norm": "0.5", "ok_soft": "0"}),
            ] * 300  # enough for min_n

        r.xrevrange = MagicMock(side_effect=mock_xrevrange)

        with patch.dict(os.environ, {
            "REDIS_URL": "redis://localhost:6379/0",
            "META_UNCLAMP_SHORT_WINDOW_MIN": "30",
            "META_UNCLAMP_LONG_WINDOW_MIN": "120",
            "META_HARDSTOP_MIN_N": "200",
            "META_HARDSTOP_LAT_P99_US": "12000",
            "META_UNCLAMP_RELAX_STREAK_N": "6",
            "RECS_HMAC_SECRET": "test_secret",
        }):
            main()

        # Streak should be incremented but no action yet
        streak = r.get("meta:hardstop:healthy_streak")
        assert streak == "4"  # 3 + 1


class TestMainAutoMode:
    """Test AUTO mode behavior."""

    def test_main_auto_mode_relax(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        # Set up clamp active
        clamp_bundle_id = "clamp123"
        r.set("meta:hardstop:clamp:active", clamp_bundle_id)
        r.set("meta:hardstop:clamp:stage", "CLAMPED")
        r.set("meta:hardstop:healthy_streak", "5")  # will become 6

        # Create clamp audit
        audit_key = f"recs:audit:{clamp_bundle_id}"
        r.rpush(audit_key, json.dumps({
            "op": "HSET",
            "key": "config:orderflow:BTCUSDT",
            "field": "meta_enforce_share_trend",
            "old": "0.50",
            "old_null": 0,
            "new": "0.10",
        }))

        # Set current clamped value
        r.hset("config:orderflow:BTCUSDT", "meta_enforce_share_trend", "0.10")

        # Mock metrics stream (both windows healthy)
        def mock_xrevrange(stream, max=None, min=None, count=None):
            ts = now_ms()
            return [
                (f"{ts}-0", {"ts_ms": str(ts - 1000), "ok": "1", "latency_us": "5000", "exec_risk_norm": "0.5", "ok_soft": "0"}),
            ] * 300

        r.xrevrange = MagicMock(side_effect=mock_xrevrange)

        with patch.dict(os.environ, {
            "REDIS_URL": "redis://localhost:6379/0",
            "META_UNCLAMP_MODE": "AUTO",
            "META_UNCLAMP_SHORT_WINDOW_MIN": "30",
            "META_UNCLAMP_LONG_WINDOW_MIN": "120",
            "META_HARDSTOP_MIN_N": "200",
            "META_HARDSTOP_LAT_P99_US": "12000",
            "META_UNCLAMP_RELAX_STREAK_N": "6",
            "META_RELAX_CAP_TREND": "0.25",
            "RECS_HMAC_SECRET": "test_secret",
        }):
            main()

        # Check stage was updated
        assert r.get("meta:hardstop:clamp:stage") == "RELAXED"

        # Check value was relaxed (capped at 0.25, not full 0.50)
        assert r.hget("config:orderflow:BTCUSDT", "meta_enforce_share_trend") == "0.25"


class TestMainProposeMode:
    """Test PROPOSE mode behavior."""

    def test_main_propose_mode_creates_pending(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        # Set up clamp active
        clamp_bundle_id = "clamp123"
        r.set("meta:hardstop:clamp:active", clamp_bundle_id)
        r.set("meta:hardstop:clamp:stage", "CLAMPED")
        r.set("meta:hardstop:healthy_streak", "5")  # will become 6

        # Create clamp audit
        audit_key = f"recs:audit:{clamp_bundle_id}"
        r.rpush(audit_key, json.dumps({
            "op": "HSET",
            "key": "config:orderflow:BTCUSDT",
            "field": "meta_enforce_share_trend",
            "old": "0.50",
            "old_null": 0,
            "new": "0.10",
        }))

        # Mock metrics stream (both windows healthy)
        def mock_xrevrange(stream, max=None, min=None, count=None):
            ts = now_ms()
            return [
                (f"{ts}-0", {"ts_ms": str(ts - 1000), "ok": "1", "latency_us": "5000", "exec_risk_norm": "0.5", "ok_soft": "0"}),
            ] * 300

        r.xrevrange = MagicMock(side_effect=mock_xrevrange)

        with patch.dict(os.environ, {
            "REDIS_URL": "redis://localhost:6379/0",
            "META_UNCLAMP_MODE": "PROPOSE",
            "META_UNCLAMP_SHORT_WINDOW_MIN": "30",
            "META_UNCLAMP_LONG_WINDOW_MIN": "120",
            "META_HARDSTOP_MIN_N": "200",
            "META_HARDSTOP_LAT_P99_US": "12000",
            "META_UNCLAMP_RELAX_STREAK_N": "6",
            "META_RELAX_CAP_TREND": "0.25",
            "RECS_HMAC_SECRET": "test_secret",
        }):
            main()

        # Check pending was created
        pending_json = r.get("meta:hardstop:unclamp:pending")
        assert pending_json is not None
        pending = json.loads(pending_json)
        assert pending["action"] == "RELAX"
        assert "bundle_id" in pending

        # Check bundle status is PENDING (not APPLIED)
        bundle_id = pending["bundle_id"]
        assert r.get(f"recs:status:{bundle_id}") == "PENDING"

        # Stage should NOT be updated yet (waiting for callback worker)
        assert r.get("meta:hardstop:clamp:stage") == "CLAMPED"

    def test_main_propose_mode_side_effects_after_applied(self):
        r = fakeredis.FakeRedis(decode_responses=True)

        # Set up clamp active
        clamp_bundle_id = "clamp123"
        r.set("meta:hardstop:clamp:active", clamp_bundle_id)
        r.set("meta:hardstop:clamp:stage", "CLAMPED")

        # Create pending proposal
        proposal_bundle_id = "proposal123"
        r.set("meta:hardstop:unclamp:pending", json.dumps({
            "bundle_id": proposal_bundle_id,
            "action": "RELAX",
            "stage_after": "RELAXED",
            "created_ms": now_ms(),
        }))

        # Set bundle status to APPLIED (simulating callback worker)
        r.set(f"recs:status:{proposal_bundle_id}", "APPLIED")

        # Mock metrics stream
        def mock_xrevrange(stream, max=None, min=None, count=None):
            return []

        r.xrevrange = MagicMock(side_effect=mock_xrevrange)

        with patch.dict(os.environ, {
            "REDIS_URL": "redis://localhost:6379/0",
            "META_UNCLAMP_MODE": "PROPOSE",
            "RECS_HMAC_SECRET": "test_secret",
        }):
            main()

        # Check side-effects were applied
        assert r.get("meta:hardstop:clamp:stage") == "RELAXED"
        # Pending should be cleared
        assert r.get("meta:hardstop:unclamp:pending") is None

