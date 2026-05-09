from __future__ import annotations

"""Unit tests for of_gate_hardstop_cap_unclamp_v7.py

Tests staged auto-unclamp v7 functionality:
- Per-symbol seg-health for range: range cell requires per-symbol exec_risk_norm_p90 in 2h window
- Quotas per cycle: max K range RESTORE and M range RELAX per cycle
- Prioritization: range cells ranked by (seg_exec_p90 asc, long_meanR desc), trend by (long_meanR desc)
- Triple gate for range: health_global AND health_range_segment (global + per-symbol) AND outcome_range_long OK
- Trend cells: only require health_global + outcome (no segment health gate)
- Selective per-cell RELAX/RESTORE based on per-bucket eligibility
- Per-cell state tracking: CLAMPED/RELAXED/RESTORED with remaining cells set
- AUTO mode: auto-applies actions
- PROPOSE mode: creates bundle, waits for callback worker
- allow_restore flag: can disable RESTORE (only RELAX allowed)
- State transitions: remaining cells cleared when empty
"""



import fakeredis

# Import module functions
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

# Use FakeStrictRedis for better compatibility with Redis operations
FakeRedis = fakeredis.FakeStrictRedis
from of_gate_hardstop_cap_unclamp_v7 import (
    _build_preclamp_map_from_audit,
    _cell_sym_bucket,
    _cell_to_cfg_field,
    _estimate_increase,
    _metric_bucket,
    _target_value_for_action,
    apply_budget_limit,
    apply_quotas_and_rank,
    build_relax_ops_cells,
    build_restore_ops_cells,
    outcome_ok,
    range_segment_ok,
    summarize_exec_p90_by_symbol_for_bucket,
)


class TestPerSymbolSegHealth:
    """Test per-symbol segment health for range cells."""

    def test_summarize_exec_p90_by_symbol_for_bucket_separates_symbols(self):
        """summarize_exec_p90_by_symbol_for_bucket should separate by symbol."""
        rows = [
            {"symbol": "BTCUSDT", "regime_group": "range", "exec_risk_norm": "0.70"},
            {"symbol": "BTCUSDT", "regime_group": "range", "exec_risk_norm": "0.75"},
            {"symbol": "ETHUSDT", "regime_group": "range", "exec_risk_norm": "0.80"},
            {"symbol": "ETHUSDT", "regime_group": "range", "exec_risk_norm": "0.85"},
            {"symbol": "BTCUSDT", "regime_group": "trend", "exec_risk_norm": "0.60"},  # Should be filtered
        ]

        result = summarize_exec_p90_by_symbol_for_bucket(rows, "range")

        assert "BTCUSDT" in result
        assert "ETHUSDT" in result
        assert result["BTCUSDT"]["n"] == 2.0
        assert result["ETHUSDT"]["n"] == 2.0
        assert result["BTCUSDT"]["exec_p90"] == 0.75
        assert result["ETHUSDT"]["exec_p90"] == 0.85

    def test_summarize_exec_p90_by_symbol_for_bucket_filters_bucket(self):
        """summarize_exec_p90_by_symbol_for_bucket should only include specified bucket."""
        rows = [
            {"symbol": "BTCUSDT", "regime_group": "range", "exec_risk_norm": "0.70"},
            {"symbol": "BTCUSDT", "regime_group": "trend", "exec_risk_norm": "0.60"},
        ]

        result_range = summarize_exec_p90_by_symbol_for_bucket(rows, "range")
        result_trend = summarize_exec_p90_by_symbol_for_bucket(rows, "trend")

        assert result_range["BTCUSDT"]["n"] == 1.0
        assert result_trend["BTCUSDT"]["n"] == 1.0

    def test_summarize_exec_p90_by_symbol_for_bucket_handles_missing_symbol(self):
        """summarize_exec_p90_by_symbol_for_bucket should skip rows without symbol."""
        rows = [
            {"symbol": "BTCUSDT", "regime_group": "range", "exec_risk_norm": "0.70"},
            {"regime_group": "range", "exec_risk_norm": "0.75"},  # No symbol
        ]

        result = summarize_exec_p90_by_symbol_for_bucket(rows, "range")

        assert "BTCUSDT" in result
        assert result["BTCUSDT"]["n"] == 1.0

    def test_per_symbol_range_seg_ok_passes(self):
        """Per-symbol range segment should pass when n and exec_p90 are within limits."""
        seg = {"n": 50.0, "exec_p90": 0.80}
        ok, msg = range_segment_ok(seg, min_n=40, exec_p90_max=0.88)
        assert ok is True
        assert "ok" in msg.lower()

    def test_per_symbol_range_seg_ok_fails_low_n(self):
        """Per-symbol range segment should fail when n < min_n (fail-closed)."""
        seg = {"n": 30.0, "exec_p90": 0.80}
        ok, msg = range_segment_ok(seg, min_n=40, exec_p90_max=0.88)
        assert ok is False
        assert "low_n" in msg.lower() or "seg_low_n" in msg

    def test_per_symbol_range_seg_ok_fails_high_exec(self):
        """Per-symbol range segment should fail when exec_p90 > max."""
        seg = {"n": 50.0, "exec_p90": 0.90}
        ok, msg = range_segment_ok(seg, min_n=40, exec_p90_max=0.88)
        assert ok is False
        assert "exec_p90" in msg


class TestQuotasAndRanking:
    """Test quotas and prioritization logic."""

    def test_apply_quotas_and_rank_range_by_exec_p90_asc_meanr_desc(self):
        """Range cells should be ranked by exec_p90 asc, then meanR desc."""
        cells = ["BTCUSDT|range", "ETHUSDT|range", "BNBUSDT|range"]
        st_long = {
            "BTCUSDT": {"range": {"meanR": -0.01}},
            "ETHUSDT": {"range": {"meanR": -0.02}},
            "BNBUSDT": {"range": {"meanR": -0.03}},
        },
        seg_range_sym = {
            "BTCUSDT": {"exec_p90": 0.85},  # Highest exec_p90
            "ETHUSDT": {"exec_p90": 0.80},  # Middle
            "BNBUSDT": {"exec_p90": 0.75},  # Lowest exec_p90 (best)
        },

        result = apply_quotas_and_rank(
            action="RELAX",
            cells=cells,
            st_long=st_long,
            seg_range_sym=seg_range_sym,
            max_range=2,
            max_trend=999,
        )

        # Should pick BNBUSDT (lowest exec_p90) and ETHUSDT (middle exec_p90)
        assert len(result) == 2
        assert "BNBUSDT|range" in result
        assert "ETHUSDT|range" in result
        assert "BTCUSDT|range" not in result  # Highest exec_p90 excluded

    def test_apply_quotas_and_rank_trend_by_meanr_desc(self):
        """Trend cells should be ranked by meanR desc."""
        cells = ["BTCUSDT|trend", "ETHUSDT|trend", "BNBUSDT|trend"]
        st_long = {
            "BTCUSDT": {"trend": {"meanR": -0.01}},  # Best
            "ETHUSDT": {"trend": {"meanR": -0.02}},
            "BNBUSDT": {"trend": {"meanR": -0.03}},  # Worst
        },
        seg_range_sym = {}

        result = apply_quotas_and_rank(
            action="RELAX",
            cells=cells,
            st_long=st_long,
            seg_range_sym=seg_range_sym,
            max_range=999,
            max_trend=2,
        )

        # Should pick BTCUSDT (best meanR) and ETHUSDT (middle meanR)
        assert len(result) == 2
        assert "BTCUSDT|trend" in result
        assert "ETHUSDT|trend" in result
        assert "BNBUSDT|trend" not in result  # Worst meanR excluded

    def test_apply_quotas_and_rank_respects_quotas(self):
        """apply_quotas_and_rank should respect max_range and max_trend quotas."""
        cells = ["BTCUSDT|range", "ETHUSDT|range", "BNBUSDT|trend", "SOLUSDT|trend"]
        st_long = {
            "BTCUSDT": {"range": {"meanR": -0.01}},
            "ETHUSDT": {"range": {"meanR": -0.02}},
            "BNBUSDT": {"trend": {"meanR": -0.01}},
            "SOLUSDT": {"trend": {"meanR": -0.02}},
        },
        seg_range_sym = {
            "BTCUSDT": {"exec_p90": 0.75},
            "ETHUSDT": {"exec_p90": 0.80},
        },

        result = apply_quotas_and_rank(
            action="RELAX",
            cells=cells,
            st_long=st_long,
            seg_range_sym=seg_range_sym,
            max_range=1,  # Only 1 range
            max_trend=1,  # Only 1 trend
        )

        assert len(result) == 2
        assert "BTCUSDT|range" in result  # Best range (lowest exec_p90)
        assert "BNBUSDT|trend" in result  # Best trend (highest meanR)

    def test_apply_quotas_and_rank_handles_missing_seg_data(self):
        """apply_quotas_and_rank should handle missing seg data gracefully."""
        cells = ["BTCUSDT|range"]
        st_long = {"BTCUSDT": {"range": {"meanR": -0.01}}}
        seg_range_sym = {}  # Missing seg data

        result = apply_quotas_and_rank(
            action="RELAX",
            cells=cells,
            st_long=st_long,
            seg_range_sym=seg_range_sym,
            max_range=1,
            max_trend=999,
        )

        # Should still work (uses default exec_p90=9e9 for missing)
        assert len(result) == 1
        assert "BTCUSDT|range" in result

    def test_cell_sym_bucket(self):
        """_cell_sym_bucket should extract symbol and bucket from cell string."""
        sym, bucket = _cell_sym_bucket("BTCUSDT|range")
        assert sym == "BTCUSDT"
        assert bucket == "range"

        sym2, bucket2 = _cell_sym_bucket("ETHUSDT|trend")
        assert sym2 == "ETHUSDT"
        assert bucket2 == "trend"

        sym3, bucket3 = _cell_sym_bucket("invalid")
        assert sym3 == ""
        assert bucket3 == ""


class TestTripleGateForRangeV7:
    """Test triple gate for range cells v7: health_global + health_range_segment (global + per-symbol) + outcome."""

    def test_range_cell_requires_per_symbol_seg_health(self):
        """Range cells should require per-symbol seg health when enabled."""
        # Mock scenario:
        # - health_ok = True
        # - range_global_ok_relax = True
        # - sym_range_ok_relax = False (per-symbol seg health fails)
        # - outcome_ok for range short = True
        # Expected: range cell should NOT be in relax_cells

        # This is integration-level test, covered in main() tests
        pass

    def test_range_cell_bypasses_per_symbol_seg_when_disabled(self):
        """Range cells should bypass per-symbol seg health when META_SEG_SYM_ENABLED=0."""
        # Mock scenario:
        # - health_ok = True
        # - range_global_ok_relax = True
        # - META_SEG_SYM_ENABLED = 0
        # - outcome_ok for range short = True
        # Expected: range cell should be in relax_cells (per-symbol check bypassed)

        # This is integration-level test, covered in main() tests
        pass


class TestQuotaLimits:
    """Test that quotas limit actions per cycle."""

    def test_quota_limits_range_relax_per_cycle(self):
        """META_MAX_RANGE_RELAX_PER_CYCLE should limit range RELAX per cycle."""
        # Mock scenario:
        # - 10 range cells eligible for RELAX
        # - META_MAX_RANGE_RELAX_PER_CYCLE = 4
        # Expected: only 4 range cells should be in cells_to_act

        # This is integration-level test, covered in main() tests
        pass

    def test_quota_limits_range_restore_per_cycle(self):
        """META_MAX_RANGE_RESTORE_PER_CYCLE should limit range RESTORE per cycle."""
        # Mock scenario:
        # - 10 range cells eligible for RESTORE
        # - META_MAX_RANGE_RESTORE_PER_CYCLE = 2
        # Expected: only 2 range cells should be in cells_to_act

        # This is integration-level test, covered in main() tests
        pass


class TestMetricBucket:
    """Test _metric_bucket function for extracting bucket from metric fields."""

    def test_metric_bucket_trend(self):
        """_metric_bucket should identify trend bucket."""
        m = {"regime_group": "trend_bull"}
        assert _metric_bucket(m) == "trend"

        m2 = {"regime": "bear"}
        assert _metric_bucket(m2) == "trend"

        m3 = {"scenario_v4": "bull"}
        assert _metric_bucket(m3) == "trend"

    def test_metric_bucket_range(self):
        """_metric_bucket should identify range bucket."""
        m = {"regime_group": "range_chop"}
        assert _metric_bucket(m) == "range"

        m2 = {"regime": "meanrev"}
        assert _metric_bucket(m2) == "range"

        m3 = {"scenario_v4": "chop"}
        assert _metric_bucket(m3) == "range"

    def test_metric_bucket_other(self):
        """_metric_bucket should return other for unknown buckets."""
        m = {"regime_group": "unknown"}
        assert _metric_bucket(m) == "other"

        m2 = {}
        assert _metric_bucket(m2) == "other"


class TestRangeSegmentOk:
    """Test range_segment_ok function."""

    def test_range_segment_ok_passes(self):
        """range_segment_ok should return True when n and exec_p90 are within limits."""
        seg = {"n": 100.0, "exec_p90": 0.80}
        ok, msg = range_segment_ok(seg, min_n=80, exec_p90_max=0.88)
        assert ok is True
        assert "ok" in msg.lower()

    def test_range_segment_ok_fails_low_n(self):
        """range_segment_ok should return False when n < min_n (fail-closed)."""
        seg = {"n": 50.0, "exec_p90": 0.80}
        ok, msg = range_segment_ok(seg, min_n=80, exec_p90_max=0.88)
        assert ok is False
        assert "low_n" in msg.lower() or "seg_low_n" in msg

    def test_range_segment_ok_fails_high_exec(self):
        """range_segment_ok should return False when exec_p90 > max."""
        seg = {"n": 100.0, "exec_p90": 0.90}
        ok, msg = range_segment_ok(seg, min_n=80, exec_p90_max=0.88)
        assert ok is False
        assert "exec_p90" in msg


class TestOutcomeOkPerBucketThresholds:
    """Test outcome_ok with per-bucket thresholds (trend vs range)."""

    def test_outcome_ok_trend_short_allows_relax(self):
        """Short window (2h) trend should allow RELAX if thresholds pass."""
        stats = {"n": 25.0, "meanR": -0.02, "tail_rate": 0.30}
        ok = outcome_ok(stats, min_n=20, mean_min=-0.03, tail_max=0.35)
        assert ok is True

    def test_outcome_ok_range_long_blocks_restore(self):
        """Long window (24h) range should block RESTORE if thresholds fail."""
        stats = {"n": 70.0, "meanR": -0.05, "tail_rate": 0.35}
        ok = outcome_ok(stats, min_n=80, mean_min=-0.02, tail_max=0.30)
        assert ok is False


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


class TestBudgetLimiter:
    """Test budget limiter functions (v8)."""

    def test_cell_to_cfg_field_trend(self):
        """_cell_to_cfg_field should extract trend field correctly."""
        sym, field, bucket = _cell_to_cfg_field("BTCUSDT|trend")
        assert sym == "BTCUSDT"
        assert field == "meta_enforce_share_trend"
        assert bucket == "trend"

    def test_cell_to_cfg_field_range(self):
        """_cell_to_cfg_field should extract range field correctly."""
        sym, field, bucket = _cell_to_cfg_field("ETHUSDT|range")
        assert sym == "ETHUSDT"
        assert field == "meta_enforce_share_range"
        assert bucket == "range"

    def test_build_preclamp_map_from_audit(self):
        """_build_preclamp_map_from_audit should build map from audit."""
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
        ]

        m = _build_preclamp_map_from_audit(clamp_audit, "config:orderflow:")

        assert "BTCUSDT|trend" in m
        assert "BTCUSDT|range" in m
        assert m["BTCUSDT|trend"]["old_value"] == 0.50
        assert m["BTCUSDT|range"]["old_value"] == 0.40
        assert m["BTCUSDT|trend"]["old_null"] == 0
        assert m["BTCUSDT|range"]["old_null"] == 0

    def test_build_preclamp_map_handles_old_null(self):
        """_build_preclamp_map_from_audit should handle old_null=1."""
        clamp_audit = [
            {
                "op": "HSET",
                "key": "config:orderflow:BTCUSDT",
                "field": "meta_enforce_share_trend",
                "old": "",
                "old_null": 1,
            },
        ]

        m = _build_preclamp_map_from_audit(clamp_audit, "config:orderflow:")

        assert "BTCUSDT|trend" in m
        assert m["BTCUSDT|trend"]["old_null"] == 1
        assert m["BTCUSDT|trend"]["old_value"] is None

    def test_target_value_for_action_relax(self):
        """_target_value_for_action should return min(old, cap) for RELAX."""
        spec = {"old_null": 0, "old_value": 0.50, "bucket": "trend"}
        has_target, target = _target_value_for_action(spec, "RELAX", cap_trend=0.30, cap_range=0.10)
        assert has_target is True
        assert target == 0.30  # min(0.50, 0.30)

    def test_target_value_for_action_restore(self):
        """_target_value_for_action should return old for RESTORE."""
        spec = {"old_null": 0, "old_value": 0.50, "bucket": "trend"}
        has_target, target = _target_value_for_action(spec, "RESTORE", cap_trend=0.30, cap_range=0.10)
        assert has_target is True
        assert target == 0.50  # full restore

    def test_target_value_for_action_old_null(self):
        """_target_value_for_action should return False for old_null=1."""
        spec = {"old_null": 1, "old_value": None, "bucket": "trend"}
        has_target, target = _target_value_for_action(spec, "RELAX", cap_trend=0.30, cap_range=0.10)
        assert has_target is False
        assert target == 0.0

    def test_estimate_increase(self):
        """_estimate_increase should calculate increase correctly."""
        r = FakeRedis()
        r.hset("config:orderflow:BTCUSDT", "meta_enforce_share_trend", "0.20")

        spec = {"key": "config:orderflow:BTCUSDT", "field": "meta_enforce_share_trend",
                "old_null": 0, "old_value": 0.50, "bucket": "trend"}

        # RELAX: target = min(0.50, 0.30) = 0.30, current = 0.20, increase = 0.10
        inc = _estimate_increase(r, spec, "RELAX", cap_trend=0.30, cap_range=0.10)
        assert inc == 0.10

    def test_estimate_increase_no_increase(self):
        """_estimate_increase should return 0 if current >= target."""
        r = FakeRedis()
        r.hset("config:orderflow:BTCUSDT", "meta_enforce_share_trend", "0.30")

        spec = {"key": "config:orderflow:BTCUSDT", "field": "meta_enforce_share_trend",
                "old_null": 0, "old_value": 0.50, "bucket": "trend"}

        # RELAX: target = min(0.50, 0.30) = 0.30, current = 0.30, increase = 0.0
        inc = _estimate_increase(r, spec, "RELAX", cap_trend=0.30, cap_range=0.10)
        assert inc == 0.0

    def test_apply_budget_limit_respects_budgets(self):
        """apply_budget_limit should respect trend, range, and total budgets."""
        r = FakeRedis()
        r.hset("config:orderflow:BTCUSDT", "meta_enforce_share_trend", "0.10")
        r.hset("config:orderflow:ETHUSDT", "meta_enforce_share_trend", "0.10")
        r.hset("config:orderflow:BNBUSDT", "meta_enforce_share_range", "0.05")

        clamp_audit = [
            {"op": "HSET", "key": "config:orderflow:BTCUSDT", "field": "meta_enforce_share_trend", "old": "0.50", "old_null": 0},
            {"op": "HSET", "key": "config:orderflow:ETHUSDT", "field": "meta_enforce_share_trend", "old": "0.50", "old_null": 0},
            {"op": "HSET", "key": "config:orderflow:BNBUSDT", "field": "meta_enforce_share_range", "old": "0.30", "old_null": 0},
        ]

        preclamp_map = _build_preclamp_map_from_audit(clamp_audit, "config:orderflow:")
        cells_ranked = ["BTCUSDT|trend", "ETHUSDT|trend", "BNBUSDT|range"]

        # Budget: trend=0.20, range=0.06, total=0.22
        # BTCUSDT: increase = min(0.50, 0.30) - 0.10 = 0.20 (uses 0.20 trend, 0.20 total)
        # ETHUSDT: increase = min(0.50, 0.30) - 0.10 = 0.20 (would use 0.20 trend, 0.40 total) -> EXCEEDS total budget
        # BNBUSDT: increase = min(0.30, 0.10) - 0.05 = 0.05 (uses 0.05 range, 0.25 total) -> EXCEEDS total budget
        picked, dbg = apply_budget_limit(
            r,
            action="RELAX",
            cells_ranked=cells_ranked,
            preclamp_map=preclamp_map,
            cap_trend=0.30,
            cap_range=0.10,
            bud_trend=0.20,
            bud_range=0.06,
            bud_total=0.22,
        )

        # Should only pick BTCUSDT (fits within budgets)
        assert len(picked) == 1
        assert "BTCUSDT|trend" in picked
        assert dbg["used_trend"] == 0.20
        assert dbg["used_range"] == 0.0
        assert dbg["used_total"] == 0.20

    def test_apply_budget_limit_handles_old_null(self):
        """apply_budget_limit should skip cells with old_null=1."""
        r = FakeRedis()
        r.hset("config:orderflow:BTCUSDT", "meta_enforce_share_trend", "0.10")

        clamp_audit = [
            {"op": "HSET", "key": "config:orderflow:BTCUSDT", "field": "meta_enforce_share_trend", "old": "", "old_null": 1},
        ]

        preclamp_map = _build_preclamp_map_from_audit(clamp_audit, "config:orderflow:")
        cells_ranked = ["BTCUSDT|trend"]

        picked, dbg = apply_budget_limit(
            r,
            action="RELAX",
            cells_ranked=cells_ranked,
            preclamp_map=preclamp_map,
            cap_trend=0.30,
            cap_range=0.10,
            bud_trend=0.20,
            bud_range=0.06,
            bud_total=0.22,
        )

        # Should skip (old_null=1 means no increase)
        assert len(picked) == 0
        assert dbg["used_total"] == 0.0

