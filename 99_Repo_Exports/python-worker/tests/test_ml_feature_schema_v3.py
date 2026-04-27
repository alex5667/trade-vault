"""
Tests for ML Feature Schema V3 — continuation-context quality features.

Validates:
  1. V3 feature list contains new cont_ctx features
  2. build_feature_vector correctly extracts cont_ctx_age_ms & hidden_ctx_recent
  3. trend_dir_source one-hots are set correctly
  4. V1/V2 backward compatibility is preserved
  5. cont_ctx_recent is intentionally EXCLUDED (train≠serve drift)
"""

from __future__ import annotations

import os
import sys

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, "/home/alex/front/trade/scanner_infra/python-worker")

import pytest

# Force V3 for these tests
os.environ["ML_FEATURE_SCHEMA_VERSION"] = "3"

from core.ml_feature_schema import (
    FEATURES_V1,
    FEATURES_V2,
    FEATURES_V3,
    build_feature_vector,
    feature_names,
    _feats_for_ver,
)


# -----------------------------------------------------------------------
# Feature list structure
# -----------------------------------------------------------------------


class TestFeatureListV3:

    def test_v3_appends_to_v2(self):
        """V3 is a strict superset of V2."""
        v2_names = [f.name for f in FEATURES_V2]
        v3_names = [f.name for f in FEATURES_V3]
        # First N features must be identical to V2
        assert v3_names[: len(v2_names)] == v2_names

    def test_v3_new_features(self):
        """V3 adds exactly 5 new features."""
        expected_new = {
            "cont_ctx_age_ms",
            "hidden_ctx_recent",
            "trend_src_hidden_div",
            "trend_src_regime",
            "trend_src_dz_bypass",
        }
        v2_names = {f.name for f in FEATURES_V2}
        v3_names = {f.name for f in FEATURES_V3}
        actual_new = v3_names - v2_names
        assert actual_new == expected_new

    def test_cont_ctx_recent_excluded(self):
        """cont_ctx_recent must NOT be in V3 (train≠serve drift risk)."""
        v3_names = {f.name for f in FEATURES_V3}
        assert "cont_ctx_recent" not in v3_names

    def test_feature_names_v3(self):
        """feature_names(3) returns V3 list."""
        names = feature_names(3)
        assert "cont_ctx_age_ms" in names
        assert "hidden_ctx_recent" in names
        assert "trend_src_hidden_div" in names

    def test_feature_names_v1_unchanged(self):
        """V1 list is unchanged."""
        names = feature_names(1)
        assert "cont_ctx_age_ms" not in names
        assert len(names) == len(FEATURES_V1)

    def test_feature_names_v2_unchanged(self):
        """V2 list is unchanged."""
        names = feature_names(2)
        assert "cont_ctx_age_ms" not in names
        assert len(names) == len(FEATURES_V2)

    def test_feats_for_ver_routing(self):
        assert _feats_for_ver(1) is FEATURES_V1
        assert _feats_for_ver(2) is FEATURES_V2
        assert _feats_for_ver(3) is FEATURES_V3
        assert _feats_for_ver(4) is FEATURES_V3  # >= 3 → V3


# -----------------------------------------------------------------------
# build_feature_vector V3 extraction
# -----------------------------------------------------------------------


class TestBuildFeatureVectorV3:

    def _base_indicators(self, **overrides):
        """Minimal indicators for a continuation signal."""
        ind = {
            "delta_z": 2.5,
            "ofi_z": 1.0,
            "ofi": 0.5,
            "ofi_stability_score": 0.8,
            "exec_risk_norm": 0.3,
            "spread_bps": 5.0,
            "expected_slippage_bps": 2.0,
            "liq_score": 0.7,
            "hawkes_taker_lam": 0.1,
            "hawkes_cancel_lam": 0.05,
            "hawkes_churn_lam": 0.02,
            "sweep_recent": 0,
            "reclaim_recent": 0,
            "obi_stable": 1,
            "iceberg_strict": 0,
            "abs_lvl_ok": 0,
            "weak_progress": 1,
            "fp_edge_absorb": 0,
            "ofi_stable": 1,
            "ofi_dir_ok": 1,
            "rsi_agree": 1,
            "div_match": 0,
            "sweep_any": 0,
            "sweep_eqh": 0,
            "sweep_eql": 0,
            # V3 cont_ctx
            "cont_ctx_age_ms": 85000,
            "hidden_ctx_recent": 1,
            "trend_dir_source": "hidden_div",
        }
        ind.update(overrides)
        return ind

    def test_cont_ctx_age_extracted(self):
        """cont_ctx_age_ms appears in the vector."""
        vec, missing = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="continuation",
            indicators=self._base_indicators(),
            rule_score=0.65,
            rule_have=2,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=3,
        )
        names = feature_names(3)
        idx = names.index("cont_ctx_age_ms")
        assert vec[idx] == 85000.0

    def test_hidden_ctx_recent_extracted(self):
        """hidden_ctx_recent appears as 1.0."""
        vec, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="continuation",
            indicators=self._base_indicators(),
            rule_score=0.65,
            rule_have=2,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=3,
        )
        names = feature_names(3)
        idx = names.index("hidden_ctx_recent")
        assert vec[idx] == 1.0

    def test_trend_src_hidden_div(self):
        """trend_src_hidden_div=1 when trend_dir_source=hidden_div."""
        vec, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="continuation",
            indicators=self._base_indicators(trend_dir_source="hidden_div"),
            rule_score=0.65,
            rule_have=2,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=3,
        )
        names = feature_names(3)
        assert vec[names.index("trend_src_hidden_div")] == 1.0
        assert vec[names.index("trend_src_regime")] == 0.0
        assert vec[names.index("trend_src_dz_bypass")] == 0.0

    def test_trend_src_regime(self):
        """trend_src_regime=1 when trend_dir_source=regime."""
        vec, _ = build_feature_vector(
            symbol="ETHUSDT",
            ts_ms=1700000000000,
            direction="SHORT",
            scenario="continuation",
            indicators=self._base_indicators(trend_dir_source="regime"),
            rule_score=0.5,
            rule_have=1,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=3,
        )
        names = feature_names(3)
        assert vec[names.index("trend_src_hidden_div")] == 0.0
        assert vec[names.index("trend_src_regime")] == 1.0

    def test_trend_src_dz_bypass(self):
        """trend_src_dz_bypass=1 when scenario_dz_bypass=1."""
        vec, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="continuation",
            indicators=self._base_indicators(
                trend_dir_source="dz_bypass",
                scenario_dz_bypass=1,
            ),
            rule_score=0.5,
            rule_have=1,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=3,
        )
        names = feature_names(3)
        assert vec[names.index("trend_src_dz_bypass")] == 1.0

    def test_missing_cont_ctx_defaults_to_zero(self):
        """Missing cont_ctx_age_ms defaults to 0.0 and appears in missing list."""
        ind = self._base_indicators()
        del ind["cont_ctx_age_ms"]
        del ind["hidden_ctx_recent"]
        vec, missing = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="reversal",  # non-continuation
            indicators=ind,
            rule_score=0.5,
            rule_have=1,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=3,
        )
        names = feature_names(3)
        assert vec[names.index("cont_ctx_age_ms")] == 0.0
        assert vec[names.index("hidden_ctx_recent")] == 0.0
        assert "cont_ctx_age_ms" in missing
        assert "hidden_ctx_recent" in missing


# -----------------------------------------------------------------------
# V1/V2 backward compatibility
# -----------------------------------------------------------------------


class TestBackwardCompatibility:

    def test_v1_vector_length(self):
        """V1 vector has correct length."""
        vec, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="reversal",
            indicators={"delta_z": 1.0},
            rule_score=0.5,
            rule_have=1,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=1,
        )
        assert len(vec) == len(FEATURES_V1)

    def test_v2_vector_length(self):
        """V2 vector has correct length."""
        vec, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="reversal",
            indicators={"delta_z": 1.0},
            rule_score=0.5,
            rule_have=1,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=2,
        )
        assert len(vec) == len(FEATURES_V2)

    def test_v3_vector_length(self):
        """V3 vector has correct length."""
        vec, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="continuation",
            indicators={"delta_z": 1.0, "cont_ctx_age_ms": 90000},
            rule_score=0.5,
            rule_have=1,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=3,
        )
        assert len(vec) == len(FEATURES_V3)

    def test_v1_prefix_stable_across_versions(self):
        """First N features of V3 match V1 exactly."""
        ind = {"delta_z": 2.0, "obi_stable": 1}
        vec1, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="reversal",
            indicators=ind.copy(),
            rule_score=0.5,
            rule_have=1,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=1,
        )
        vec3, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="reversal",
            indicators=ind.copy(),
            rule_score=0.5,
            rule_have=1,
            rule_need=2,
            cancel_spike_veto=0,
            schema_ver=3,
        )
        # V1 prefix must be identical
        assert vec3[: len(FEATURES_V1)] == vec1
