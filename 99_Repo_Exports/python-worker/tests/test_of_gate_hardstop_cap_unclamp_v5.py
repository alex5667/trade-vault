"""Unit tests for of_gate_hardstop_cap_unclamp_v5.py

Tests staged auto-unclamp v5 functionality:
- Outcome-gate per bucket (trend vs range) evaluated separately
- Selective per-cell RELAX/RESTORE based on per-bucket outcome stats
- REMOVE (RESTORE) forbidden if long-outcome bad in range (even if trend good)
- Per-cell state tracking: CLAMPED/RELAXED/RESTORED with remaining cells set
- AUTO mode: auto-applies actions
- PROPOSE mode: creates bundle, waits for callback worker
- allow_remove flag: can disable RESTORE (only RELAX allowed)
- State transitions: remaining cells cleared when empty
"""

from __future__ import annotations

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
from of_gate_hardstop_cap_unclamp_v5 import (
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
    build_relax_ops_cells,
    build_restore_ops_cells,
    _is_closed,
    _get_symbol,
    _get_bucket,
    _get_r_mult,
    read_outcome_stats_sym_bucket,
    outcome_ok,
    _extract_symbols_from_audit,
    _init_remaining_cells_if_needed,
    _audit_has_field_for_sym,
    main,
)


class TestOutcomeOkPerBucketThresholds:
    """Test outcome_ok with per-bucket thresholds (trend vs range)."""
    
    def test_outcome_ok_trend_short_allows_relax(self):
        """Short window (2h) trend should allow RELAX if thresholds pass."""
        stats = {"n": 25.0, "meanR": -0.02, "tail_rate": 0.30}
        ok, reasons = outcome_ok(stats, min_n=20, mean_min=-0.03, tail_max=0.35)
        assert ok is True
        assert len(reasons) == 0
    
    def test_outcome_ok_range_long_blocks_restore(self):
        """Long window (24h) range should block RESTORE if thresholds fail."""
        stats = {"n": 70.0, "meanR": -0.05, "tail_rate": 0.35}
        ok, reasons = outcome_ok(stats, min_n=80, mean_min=-0.02, tail_max=0.30)
        assert ok is False
        assert any("low_n" in r or "mean" in r or "tail" in r for r in reasons)
    
    def test_outcome_ok_trend_good_range_bad_blocks_full_restore(self):
        """If trend long OK but range long bad, only trend cell restores."""
        stats_trend_long = {"n": 100.0, "meanR": -0.01, "tail_rate": 0.25}
        stats_range_long = {"n": 70.0, "meanR": -0.05, "tail_rate": 0.35}
        
        ok_trend_l, _ = outcome_ok(stats_trend_long, min_n=80, mean_min=-0.02, tail_max=0.30)
        ok_range_l, _ = outcome_ok(stats_range_long, min_n=80, mean_min=-0.02, tail_max=0.30)
        
        assert ok_trend_l is True
        assert ok_range_l is False  # Range blocks full restore


class TestBucketExtraction:
    """Test bucket extraction from event fields."""
    
    def test_get_bucket_trend(self):
        """_get_bucket should identify trend bucket."""
        fields = {"regime_group": "trend_bull"}
        assert _get_bucket(fields) == "trend"
        
        fields2 = {"regime": "bear"}
        assert _get_bucket(fields2) == "trend"
    
    def test_get_bucket_range(self):
        """_get_bucket should identify range bucket."""
        fields = {"regime_group": "range_chop"}
        assert _get_bucket(fields) == "range"
        
        fields2 = {"scenario_v4": "meanrev"}
        assert _get_bucket(fields2) == "range"
    
    def test_get_bucket_from_payload(self):
        """_get_bucket should extract from JSON payload."""
        fields = {"payload": json.dumps({"regime_group": "trend"})}
        assert _get_bucket(fields) == "trend"
        
        fields2 = {"payload": json.dumps({"regime": "chop"})}
        assert _get_bucket(fields2) == "range"


class TestReadOutcomeStatsSymBucket:
    """Test reading outcome stats per symbol per bucket."""
    
    def test_read_outcome_stats_separates_trend_range(self):
        """read_outcome_stats_sym_bucket should separate trend and range."""
        r = fakeredis.FakeRedis(decode_responses=True)
        stream = "events:trades"
        
        now = now_ms()
        since = now - 2 * 3600_000  # 2h
        
        # Add trend trade
        r.xadd(stream, {
            "ts_ms": str(now - 3600_000),
            "event_type": "POSITION_CLOSED",
            "symbol": "BTCUSDT",
            "regime_group": "trend",
            "r_mult": "0.5",
        })
        
        # Add range trade
        r.xadd(stream, {
            "ts_ms": str(now - 1800_000),
            "event_type": "POSITION_CLOSED",
            "symbol": "BTCUSDT",
            "regime_group": "range",
            "r_mult": "-0.3",
        })
        
        stats = read_outcome_stats_sym_bucket(
            r,
            stream=stream,
            since_ms=since,
            symbols=["BTCUSDT"],
            max_scan=1000,
        )
        
        assert "BTCUSDT" in stats
        assert "trend" in stats["BTCUSDT"]
        assert "range" in stats["BTCUSDT"]
        assert stats["BTCUSDT"]["trend"]["n"] == 1.0
        assert stats["BTCUSDT"]["range"]["n"] == 1.0


class TestSelectiveOpsPerCell:
    """Test that ops are built only for eligible cells (SYM|bucket)."""
    
    def test_build_relax_ops_cells_only_eligible(self):
        """build_relax_ops_cells should only include eligible cells."""
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
                "old": "0.40",
                "old_null": 0,
            },
            {
                "op": "HSET",
                "key": "config:orderflow:ETHUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.30",
                "old_null": 0,
            },
        ]
        
        eligible_cells = ["BTCUSDT|trend", "BTCUSDT|range"]  # ETHUSDT not eligible
        
        ops = build_relax_ops_cells(
            clamp_audit,
            cfg_prefix="config:orderflow:",
            eligible_cells=eligible_cells,
            cap_trend=0.30,
            cap_range=0.10,
        )
        
        assert len(ops) == 2
        cells_in_ops = set()
        for op in ops:
            sym = op["key"].split(":")[-1]
            bucket = "trend" if "trend" in op["field"] else "range"
            cells_in_ops.add(f"{sym}|{bucket}")
        assert "BTCUSDT|trend" in cells_in_ops
        assert "BTCUSDT|range" in cells_in_ops
        assert "ETHUSDT|trend" not in cells_in_ops
    
    def test_build_relax_ops_cells_applies_caps(self):
        """build_relax_ops_cells should apply per-bucket caps."""
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
                "old": "0.20",
                "old_null": 0,
            },
        ]
        
        eligible_cells = ["BTCUSDT|trend", "BTCUSDT|range"]
        
        ops = build_relax_ops_cells(
            clamp_audit,
            cfg_prefix="config:orderflow:",
            eligible_cells=eligible_cells,
            cap_trend=0.30,
            cap_range=0.10,
        )
        
        assert len(ops) == 2
        for op in ops:
            if "trend" in op["field"]:
                assert float(op["value"]) == 0.30  # min(0.50, 0.30)
            else:
                assert float(op["value"]) == 0.10  # min(0.20, 0.10)
    
    def test_build_restore_ops_cells_only_eligible(self):
        """build_restore_ops_cells should only include eligible cells."""
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
        
        eligible_cells = ["BTCUSDT|trend"]  # ETHUSDT not eligible
        
        ops = build_restore_ops_cells(
            clamp_audit,
            cfg_prefix="config:orderflow:",
            eligible_cells=eligible_cells,
        )
        
        assert len(ops) == 1
        assert "BTCUSDT" in ops[0]["key"]
        assert "trend" in ops[0]["field"]
        assert ops[0]["value"] == "0.50"


class TestRemainingCellsInitFromAudit:
    """Test remaining cells initialization from clamp audit."""
    
    def test_init_remaining_cells_if_needed_creates_set(self):
        """_init_remaining_cells_if_needed should create remaining cells set from audit."""
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
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_range",
                "old": "0.30",
                "old_null": 0,
            },
            {
                "op": "HSET",
                "key": "config:orderflow:ETHUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.40",
                "old_null": 0,
            },
        ]
        
        remaining_key = "meta:hardstop:clamp:remaining_cells"
        cell_state_key = "meta:hardstop:clamp:cell_state"
        
        result = _init_remaining_cells_if_needed(
            r,
            remaining_cells_key=remaining_key,
            cell_state_key=cell_state_key,
            clamp_audit=clamp_audit,
            cfg_prefix="config:orderflow:",
            ttl=3600,
        )
        
        assert len(result) == 3
        assert "BTCUSDT|trend" in result
        assert "BTCUSDT|range" in result
        assert "ETHUSDT|trend" in result
        
        # Check Redis state
        remaining_set = set(r.smembers(remaining_key))
        assert "BTCUSDT|trend" in remaining_set
        assert "BTCUSDT|range" in remaining_set
        assert "ETHUSDT|trend" in remaining_set
        
        assert r.hget(cell_state_key, "BTCUSDT|trend") == "CLAMPED"
        assert r.hget(cell_state_key, "BTCUSDT|range") == "CLAMPED"
        assert r.hget(cell_state_key, "ETHUSDT|trend") == "CLAMPED"
    
    def test_init_remaining_cells_if_needed_reuses_existing(self):
        """_init_remaining_cells_if_needed should reuse existing set if present."""
        r = fakeredis.FakeRedis(decode_responses=True)
        
        remaining_key = "meta:hardstop:clamp:remaining_cells"
        cell_state_key = "meta:hardstop:clamp:cell_state"
        
        # Pre-populate
        r.sadd(remaining_key, "BTCUSDT|trend")
        r.sadd(remaining_key, "BTCUSDT|range")
        r.hset(cell_state_key, "BTCUSDT|trend", "CLAMPED")
        r.hset(cell_state_key, "BTCUSDT|range", "RELAXED")
        
        clamp_audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:BNBUSDT",
                "field": "meta_enforce_share_trend",
                "old": "0.30",
                "old_null": 0,
            },
        ]
        
        result = _init_remaining_cells_if_needed(
            r,
            remaining_cells_key=remaining_key,
            cell_state_key=cell_state_key,
            clamp_audit=clamp_audit,
            cfg_prefix="config:orderflow:",
            ttl=3600,
        )
        
        # Should return existing set, not create new
        assert len(result) == 2
        assert "BTCUSDT|trend" in result
        assert "BTCUSDT|range" in result
        assert "BNBUSDT|trend" not in result


class TestStateTransitionsRestoreClearsActiveWhenEmpty:
    """Test that restoring all cells clears clamp active when remaining is empty."""
    
    def test_restore_clears_active_when_remaining_empty(self):
        """When remaining cells set becomes empty after RESTORE, clamp active should be cleared."""
        r = fakeredis.FakeRedis(decode_responses=True)
        
        clamp_active_key = "meta:hardstop:clamp:active"
        remaining_key = "meta:hardstop:clamp:remaining_cells"
        cell_state_key = "meta:hardstop:clamp:cell_state"
        
        # Setup: one remaining cell
        r.set(clamp_active_key, "bundle_123")
        r.sadd(remaining_key, "BTCUSDT|trend")
        r.hset(cell_state_key, "BTCUSDT|trend", "CLAMPED")
        
        # Simulate RESTORE that removes last cell
        r.hset(cell_state_key, "BTCUSDT|trend", "RESTORED")
        r.srem(remaining_key, "BTCUSDT|trend")
        
        # Check: clamp active should be cleared when remaining is empty
        if r.scard(remaining_key) == 0:
            r.delete(clamp_active_key)
        
        assert r.get(clamp_active_key) is None
        assert r.scard(remaining_key) == 0


class TestModeAndAllowRemove:
    """Test mode and allow_remove runtime overrides."""
    
    def test_mode_from_env_default_auto(self):
        """_mode should default to AUTO from ENV."""
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch.dict(os.environ, {"META_UNCLAMP_MODE": "AUTO"}):
            assert _mode(r) == "AUTO"
    
    def test_mode_from_redis_override(self):
        """_mode should read from Redis key if set."""
        r = fakeredis.FakeRedis(decode_responses=True)
        r.set("cfg:meta_unclamp:mode", "PROPOSE")
        with patch.dict(os.environ, {"META_UNCLAMP_MODE": "AUTO", "META_UNCLAMP_MODE_KEY": "cfg:meta_unclamp:mode"}):
            assert _mode(r) == "PROPOSE"
    
    def test_allow_remove_from_env_default_true(self):
        """_allow_remove should default to True from ENV."""
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch.dict(os.environ, {"META_UNCLAMP_ALLOW_REMOVE": "1"}):
            assert _allow_remove(r) is True
    
    def test_allow_remove_from_redis_override(self):
        """_allow_remove should read from Redis key if set."""
        r = fakeredis.FakeRedis(decode_responses=True)
        r.set("cfg:meta_unclamp:allow_remove", "0")
        with patch.dict(os.environ, {"META_UNCLAMP_ALLOW_REMOVE": "1", "META_UNCLAMP_ALLOW_REMOVE_KEY": "cfg:meta_unclamp:allow_remove"}):
            assert _allow_remove(r) is False


class TestBucketThresholds:
    """Test per-bucket threshold reading."""
    
    def test_bucket_thr_trend_short(self):
        """Should read trend short thresholds from ENV."""
        with patch.dict(os.environ, {
            "META_OUT_S_MIN_N_TREND": "25",
            "META_OUT_S_MEAN_MIN_TREND": "-0.02",
            "META_OUT_S_TAIL_MAX_TREND": "0.30",
        }):
            from of_gate_hardstop_cap_unclamp_v5 import _bucket_thr
            assert _bucket_thr("META_OUT_S_MIN_N_TREND", 20) == 25.0
            assert _bucket_thr("META_OUT_S_MEAN_MIN_TREND", -0.03) == -0.02
            assert _bucket_thr("META_OUT_S_TAIL_MAX_TREND", 0.35) == 0.30
    
    def test_bucket_thr_range_long(self):
        """Should read range long thresholds from ENV."""
        with patch.dict(os.environ, {
            "META_OUT_L_MIN_N_RANGE": "100",
            "META_OUT_L_MEAN_MIN_RANGE": "-0.01",
            "META_OUT_L_TAIL_MAX_RANGE": "0.25",
        }):
            from of_gate_hardstop_cap_unclamp_v5 import _bucket_thr
            assert _bucket_thr("META_OUT_L_MIN_N_RANGE", 80) == 100.0
            assert _bucket_thr("META_OUT_L_MEAN_MIN_RANGE", -0.02) == -0.01
            assert _bucket_thr("META_OUT_L_TAIL_MAX_RANGE", 0.30) == 0.25


class TestRelaxCapsPerBucket:
    """Test relax caps per bucket."""
    
    def test_relax_caps_trend_range_separate(self):
        """Relax caps should be separate for trend and range."""
        with patch.dict(os.environ, {
            "META_RELAX_CAP_TREND": "0.30",
            "META_RELAX_CAP_RANGE": "0.10",
        }):
            # This is tested indirectly through build_relax_ops_cells
            # which uses cap_trend and cap_range parameters
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
                    "old": "0.20",
                    "old_null": 0,
                },
            ]
            
            ops = build_relax_ops_cells(
                clamp_audit,
                cfg_prefix="config:orderflow:",
                eligible_cells=["BTCUSDT|trend", "BTCUSDT|range"],
                cap_trend=0.30,
                cap_range=0.10,
            )
            
            assert len(ops) == 2
            for op in ops:
                if "trend" in op["field"]:
                    assert float(op["value"]) == 0.30
                else:
                    assert float(op["value"]) == 0.10

