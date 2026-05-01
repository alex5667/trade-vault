from __future__ import annotations
"""Unit tests for of_gate_hardstop_cap_unclamp_v4.py

Tests staged auto-unclamp v4 functionality:
- Selective per-symbol RELAX/REMOVE based on per-symbol outcome stats
- Dual-window outcome gate: 2h (RELAX) + 24h (REMOVE)
- Per-symbol state tracking: CLAMPED/RELAXED/RESTORED with remaining set
- AUTO mode: auto-applies actions
- PROPOSE mode: creates bundle, waits for callback worker
- allow_remove flag: can disable REMOVE (only RELAX allowed)
- State transitions: remaining set cleared when empty
"""


import json
import os
import time
from unittest.mock import patch, MagicMock
from typing import Dict, Any, List

import pytest
import fakeredis

# Import module functions
import sys
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from of_gate_hardstop_cap_unclamp_v4 import (
    now_ms,
    pctl,
    _f,
    _i,
    sign,
    read_metrics_window,
    summarize_health,
    is_unhealthy,
    _read_audit_list,
    _apply_restores_direct,
    _create_proposal_bundle,
    _mode,
    _allow_remove,
    build_relax_ops_selective,
    build_remove_ops_selective,
    _is_closed,
    _get_symbol,
    _get_r_mult,
    read_outcome_stats_per_symbol,
    outcome_ok,
    _extract_symbols_from_audit,
    _init_remaining_if_needed,
    main,
)


class TestOutcomeOkShortLongThresholds:
    """Test outcome_ok with short and long thresholds."""
    
    def test_outcome_ok_short_allows_relax(self):
        """Short window (2h) should allow RELAX if thresholds pass."""
        stats = {"n": 25.0, "meanR": -0.02, "tail_rate": 0.30}
        ok, reasons = outcome_ok(stats, min_n=20, mean_min=-0.03, tail_max=0.35)
        assert ok is True
        assert len(reasons) == 0
    
    def test_outcome_ok_long_blocks_remove(self):
        """Long window (24h) should block REMOVE if thresholds fail."""
        stats = {"n": 70.0, "meanR": -0.05, "tail_rate": 0.35}
        ok, reasons = outcome_ok(stats, min_n=80, mean_min=-0.02, tail_max=0.30)
        assert ok is False
        assert any("low_n" in r or "mean" in r or "tail" in r for r in reasons)
    
    def test_outcome_ok_both_pass_allows_remove(self):
        """Both short and long passing should allow REMOVE."""
        stats_short = {"n": 25.0, "meanR": -0.02, "tail_rate": 0.30}
        stats_long = {"n": 100.0, "meanR": -0.01, "tail_rate": 0.25}
        
        ok_s, _ = outcome_ok(stats_short, min_n=20, mean_min=-0.03, tail_max=0.35)
        ok_l, _ = outcome_ok(stats_long, min_n=80, mean_min=-0.02, tail_max=0.30)
        
        assert ok_s is True
        assert ok_l is True


class TestSelectiveOpsOnlyForEligibleSymbols:
    """Test that ops are built only for eligible symbols."""
    
    def test_build_relax_ops_selective_only_eligible(self):
        """build_relax_ops_selective should only include eligible symbols."""
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
                "key": "config:orderflow:ETHUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.40",
                "old_null": 0,
            },
            {
                "op": "HSET",
                "key": "config:orderflow:BNBUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.30",
                "old_null": 0,
            },
        ]
        
        relax_caps = {"meta_enforce_share_trend": 0.25}
        eligible_syms = ["BTCUSDT", "ETHUSDT"]  # BNBUSDT not eligible
        
        ops = build_relax_ops_selective(
            clamp_audit,
            relax_caps=relax_caps,
            cfg_prefix="config:orderflow:",
            eligible_syms=eligible_syms,
        )
        
        assert len(ops) == 2
        syms_in_ops = {op["key"].split(":")[-1] for op in ops}
        assert "BTCUSDT" in syms_in_ops
        assert "ETHUSDT" in syms_in_ops
        assert "BNBUSDT" not in syms_in_ops
    
    def test_build_remove_ops_selective_only_eligible(self):
        """build_remove_ops_selective should only include eligible symbols."""
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
                "key": "config:orderflow:ETHUSDT",
                "field": "meta_enforce_share_range",
                "old": "0.30",
                "old_null": 0,
            },
        ]
        
        eligible_syms = ["BTCUSDT"]  # ETHUSDT not eligible
        
        ops = build_remove_ops_selective(
            clamp_audit,
            cfg_prefix="config:orderflow:",
            eligible_syms=eligible_syms,
        )
        
        assert len(ops) == 1
        assert "BTCUSDT" in ops[0]["key"]
        assert "ETHUSDT" not in ops[0]["key"]


class TestRemainingSetInitFromAudit:
    """Test remaining set initialization from clamp audit."""
    
    def test_init_remaining_if_needed_creates_set(self):
        """_init_remaining_if_needed should create remaining set from audit."""
        r = fakeredis.FakeRedis(decode_responses=True)
        
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
                "key": "config:orderflow:ETHUSDT",
                "field": "meta_enforce_share_range",
                "old": "0.30",
                "old_null": 0,
            },
        ]
        
        remaining_key = "meta:hardstop:clamp:remaining_syms"
        sym_state_key = "meta:hardstop:clamp:sym_state"
        
        result = _init_remaining_if_needed(
            r,
            remaining_key=remaining_key,
            sym_state_key=sym_state_key,
            clamp_audit=clamp_audit,
            cfg_prefix="config:orderflow:",
        )
        
        assert len(result) == 2
        assert "BTCUSDT" in result
        assert "ETHUSDT" in result
        
        # Check Redis state
        remaining_set = set(r.smembers(remaining_key))
        assert "BTCUSDT" in remaining_set
        assert "ETHUSDT" in remaining_set
        
        assert r.hget(sym_state_key, "BTCUSDT") == "CLAMPED"
        assert r.hget(sym_state_key, "ETHUSDT") == "CLAMPED"
    
    def test_init_remaining_if_needed_reuses_existing(self):
        """_init_remaining_if_needed should reuse existing set if present."""
        r = fakeredis.FakeRedis(decode_responses=True)
        
        remaining_key = "meta:hardstop:clamp:remaining_syms"
        sym_state_key = "meta:hardstop:clamp:sym_state"
        
        # Pre-populate
        r.sadd(remaining_key, "BTCUSDT")
        r.sadd(remaining_key, "ETHUSDT")
        r.hset(sym_state_key, "BTCUSDT", "CLAMPED")
        r.hset(sym_state_key, "ETHUSDT", "RELAXED")
        
        clamp_audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:BNBUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.30",
                "old_null": 0,
            },
        ]
        
        result = _init_remaining_if_needed(
            r,
            remaining_key=remaining_key,
            sym_state_key=sym_state_key,
            clamp_audit=clamp_audit,
            cfg_prefix="config:orderflow:",
        )
        
        # Should return existing set, not create new
        assert len(result) == 2
        assert "BTCUSDT" in result
        assert "ETHUSDT" in result
        assert "BNBUSDT" not in result


class TestStateTransitionsRemoveClearsActiveWhenEmpty:
    """Test that removing all symbols clears clamp active when remaining is empty."""
    
    def test_remove_clears_active_when_remaining_empty(self):
        """When remaining set becomes empty after REMOVE, clamp active should be cleared."""
        r = fakeredis.FakeRedis(decode_responses=True)
        
        # Set up clamp state
        clamp_bundle_id = "clamp123"
        r.set("meta:hardstop:clamp:active", clamp_bundle_id)
        r.set("meta:hardstop:clamp:stage", "RELAXED")
        r.set("meta:hardstop:healthy_streak", "20")
        
        remaining_key = "meta:hardstop:clamp:remaining_syms"
        sym_state_key = "meta:hardstop:clamp:sym_state"
        
        # Only one symbol remaining
        r.sadd(remaining_key, "BTCUSDT")
        r.hset(sym_state_key, "BTCUSDT", "RELAXED")
        
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
        
        # Set current value
        r.hset("config:orderflow:BTCUSDT", "meta_enforce_share_trend", "0.25")
        
        # Mock metrics stream (all windows healthy)
        def mock_xrevrange(stream, max=None, min=None, count=None):
            ts = now_ms()
            return [
                (f"{ts}-0", {"ts_ms": str(ts - 1000), "ok": "1", "latency_us": "5000", "exec_risk_norm": "0.5", "ok_soft": "0"}),
            ] * 300
        
        r.xrevrange = MagicMock(side_effect=mock_xrevrange)
        
        # Mock trades stream (good outcomes for BTCUSDT)
        def mock_trades_xrevrange(stream, max=None, min=None, count=None):
            ts = now_ms()
            return [
                (f"{ts}-0", {"ts_ms": str(ts - 1000), "event_type": "POSITION_CLOSED", "symbol": "BTCUSDT", "r_mult": "0.5"}),
            ] * 100  # enough for long window
        
        original_xrevrange = r.xrevrange
        call_count = [0]
        
        def side_effect(stream, max=None, min=None, count=None):
            call_count[0] += 1
            if stream == "events:trades":
                return mock_trades_xrevrange(stream, max, min, count)
            else:
                return original_xrevrange(stream, max, min, count)
        
        r.xrevrange = MagicMock(side_effect=side_effect)
        
        with patch.dict(os.environ, {
            "REDIS_URL": "redis://localhost:6379/0",
            "META_UNCLAMP_MODE": "AUTO",
            "META_UNCLAMP_ALLOW_REMOVE": "1",
            "META_UNCLAMP_SHORT_WINDOW_MIN": "30",
            "META_UNCLAMP_LONG_WINDOW_MIN": "120",
            "META_UNCLAMP_BASELINE_WINDOW_MIN": "720",
            "META_HARDSTOP_MIN_N": "200",
            "META_HARDSTOP_LAT_P99_US": "12000",
            "META_UNCLAMP_REMOVE_STREAK_N": "18",
            "META_UNCLAMP_OUTCOME_SHORT_HOURS": "2",
            "META_UNCLAMP_OUTCOME_LONG_HOURS": "24",
            "META_UNCLAMP_OUTCOME_SHORT_MIN_N": "20",
            "META_UNCLAMP_OUTCOME_SHORT_MEAN_MIN": "-0.03",
            "META_UNCLAMP_OUTCOME_SHORT_TAIL_MAX": "0.35",
            "META_UNCLAMP_OUTCOME_LONG_MIN_N": "80",
            "META_UNCLAMP_OUTCOME_LONG_MEAN_MIN": "-0.02",
            "META_UNCLAMP_OUTCOME_LONG_TAIL_MAX": "0.30",
            "TRADE_EVENTS_STREAM": "events:trades",
            "RECS_HMAC_SECRET": "test_secret",
            "CFG_HASH_PREFIX": "config:orderflow:",
        }):
            with patch("of_gate_hardstop_cap_unclamp_v4.redis.Redis.from_url", return_value=r):
                main()
        
        # After REMOVE of last symbol, clamp active should be cleared
        assert r.get("meta:hardstop:clamp:active") is None
        assert r.get("meta:hardstop:clamp:stage") is None
        assert r.scard(remaining_key) == 0


class TestReadOutcomeStatsPerSymbol:
    """Test per-symbol outcome stats reading."""
    
    def test_read_outcome_stats_per_symbol_filters_by_symbol(self):
        """read_outcome_stats_per_symbol should only count trades for specified symbols."""
        r = fakeredis.FakeRedis(decode_responses=True)
        ts = now_ms()
        
        def mock_xrevrange(stream, max=None, min=None, count=None):
            return [
                (f"{ts}-0", {"ts_ms": str(ts - 1000), "event_type": "POSITION_CLOSED", "symbol": "BTCUSDT", "r_mult": "0.5"}),
                (f"{ts}-1", {"ts_ms": str(ts - 2000), "event_type": "POSITION_CLOSED", "symbol": "ETHUSDT", "r_mult": "0.3"}),
                (f"{ts}-2", {"ts_ms": str(ts - 3000), "event_type": "POSITION_CLOSED", "symbol": "BNBUSDT", "r_mult": "-1.5"}),
            ]
        
        r.xrevrange = MagicMock(side_effect=mock_xrevrange)
        
        stats = read_outcome_stats_per_symbol(
            r,
            stream="events:trades",
            since_ms=ts - 10000,
            symbols=["BTCUSDT", "ETHUSDT"],  # BNBUSDT not included
            max_scan=1000,
        )
        
        assert "BTCUSDT" in stats
        assert "ETHUSDT" in stats
        assert "BNBUSDT" not in stats
        assert stats["BTCUSDT"]["n"] == 1.0
        assert stats["ETHUSDT"]["n"] == 1.0


class TestAllowRemove:
    """Test allow_remove flag."""
    
    def test_allow_remove_default_true(self):
        """Default should allow REMOVE."""
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch.dict(os.environ, {}):
            assert _allow_remove(r) is True
    
    def test_allow_remove_env_false(self):
        """ENV can disable REMOVE."""
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch.dict(os.environ, {"META_UNCLAMP_ALLOW_REMOVE": "0"}):
            assert _allow_remove(r) is False
    
    def test_allow_remove_redis_override(self):
        """Redis key can override ENV."""
        r = fakeredis.FakeRedis(decode_responses=True)
        r.set("cfg:meta_unclamp:allow_remove", "0")
        with patch.dict(os.environ, {"META_UNCLAMP_ALLOW_REMOVE": "1", "META_UNCLAMP_ALLOW_REMOVE_KEY": "cfg:meta_unclamp:allow_remove"}):
            assert _allow_remove(r) is False


class TestExtractSymbolsFromAudit:
    """Test symbol extraction from audit."""
    
    def test_extract_symbols_from_audit(self):
        """_extract_symbols_from_audit should extract unique symbols."""
        audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.50",
            },
            {
                "op": "HSET",
                "key": "config:orderflow:ETHUSDT",
                "field": "meta_enforce_share_range",
                "old": "0.30",
            },
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",  # duplicate
                "field": "meta_enforce_share_range",
                "old": "0.20",
            },
        ]
        
        syms = _extract_symbols_from_audit(audit, "config:orderflow:")
        assert len(syms) == 2
        assert "BTCUSDT" in syms
        assert "ETHUSDT" in syms

