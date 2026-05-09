"""Tests for MLFeatureSchemaV3Online (serving-safe, no leakage).

Verifies:
  - Feature count and name structure (no mae_r / mfe_r)
  - vectorize() output length matches feature_names() length
  - determinism: same input → same output
  - vectorize_row() as thin wrapper
  - No-crash on missing/None values
"""
from core.ml_feature_schema_v3_online import MLFeatureSchemaV3Online

SCHEMA_HASH = "78e60fa067e1"



SCHEMA = MLFeatureSchemaV3Online()


class TestFeatureNames:
    def test_feature_names_count(self):
        names = SCHEMA.feature_names()
        # num_keys(19) + bool_keys(9) + dir(2) + bucket(3) + hour(24) + dow(7) = 64
        assert len(names) == 64

    def test_no_mae_mfe_leakage(self):
        """Training-only outcome fields must not appear in serving schema."""
        names = SCHEMA.feature_names()
        assert "n:mae_r" not in names
        assert "n:mfe_r" not in names

    def test_no_duplicates_in_names(self):
        names = SCHEMA.feature_names()
        assert len(names) == len(set(names))

    def test_n_prefix_on_num_keys(self):
        names = set(SCHEMA.feature_names())
        for k in SCHEMA.num_keys:
            assert f"n:{k}" in names

    def test_b_prefix_on_bool_keys(self):
        names = set(SCHEMA.feature_names())
        for k in SCHEMA.bool_keys:
            assert f"b:{k}" in names

    def test_adverse_lambda_present(self):
        names = set(SCHEMA.feature_names())
        for k in ("n:adverse_proxy", "n:lambda_taker", "n:lambda_cancel", "n:lambda_spread_widen"):
            assert k in names, f"{k} missing from feature_names"


class TestVectorize:
    def _dummy_indicators(self, **overrides):
        ind = {
            "delta_z": 0.5,
            "ofi_z": 1.2,
            "ofi_stability_score": 0.8,
            "obi": 0.3,
            "obi_z": 0.2,
            "spread_bps": 5.0,
            "expected_slippage_bps": 2.0,
            "exec_risk_norm": 0.4,
            "liq_score": 0.7,
            "book_staleness_ms": 100.0,
            "pressure": 0.6,
            "triggers_per_min": 3.0,
            "ofi_stable": 1,
            "ofi_dir_ok": True,
            "obi_stable": 1,
            "iceberg_strict": 0,
            "fp_edge_absorb": 0,
            "abs_lvl_ok": 0,
            "reclaim_recent": 1,
            "sweep_recent": 0,
        }
        ind.update(overrides)
        return ind

    def test_vectorize_length_matches_feature_names(self):
        x = SCHEMA.vectorize(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="trend:up",
            indicators=self._dummy_indicators(),
            rule_score=0.75,
            rule_have=3,
            rule_need=3,
            cancel_spike_veto=0,
        )
        assert len(x) == len(SCHEMA.feature_names())

    def test_all_values_finite(self):
        x = SCHEMA.vectorize(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="trend:up",
            indicators=self._dummy_indicators(),
            rule_score=0.75,
            rule_have=3,
            rule_need=3,
            cancel_spike_veto=0,
        )
        import math
        for v in x:
            assert math.isfinite(v), f"non-finite value: {v}"

    def test_direction_one_hot_long(self):
        x = SCHEMA.vectorize(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="range",
            indicators={},
            rule_score=0.0,
            rule_have=0,
            rule_need=0,
            cancel_spike_veto=0,
        )
        names = SCHEMA.feature_names()
        assert x[names.index("dir:LONG")] == 1.0
        assert x[names.index("dir:SHORT")] == 0.0

    def test_direction_case_insensitive(self):
        x = SCHEMA.vectorize(
            symbol="X",
            ts_ms=1700000000000,
            direction="short",
            scenario="other",
            indicators={},
            rule_score=0.0,
            rule_have=0,
            rule_need=0,
            cancel_spike_veto=0,
        )
        names = SCHEMA.feature_names()
        assert x[names.index("dir:SHORT")] == 1.0

    def test_determinism(self):
        kwargs = dict(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="trend:up",
            indicators=self._dummy_indicators(),
            rule_score=0.75,
            rule_have=3,
            rule_need=3,
            cancel_spike_veto=0,
        )
        assert SCHEMA.vectorize(**kwargs) == SCHEMA.vectorize(**kwargs)

    def test_missing_indicators_gives_zeros(self):
        x = SCHEMA.vectorize(
            symbol="X",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="range",
            indicators={},
            rule_score=0.0,
            rule_have=0,
            rule_need=0,
            cancel_spike_veto=0,
        )
        # All numeric indicators should be 0 when missing
        for k in SCHEMA.num_keys:
            idx = SCHEMA.feature_names().index(f"n:{k}")
            assert x[idx] == 0.0, f"{k} should be 0 when missing"


class TestVectorizeRow:
    def test_vectorize_row_equivalent(self):
        row = {
            "symbol": "ETHUSDT",
            "ts_ms": 1700000000000,
            "direction": "SHORT",
            "scenario_v4": "range",
            "rule_score": 0.6,
            "rule_have": 2,
            "rule_need": 3,
            "cancel_spike_veto": 1,
            "indicators": {"delta_z": 1.5},
        }
        x_row = SCHEMA.vectorize_row(row)
        x_direct = SCHEMA.vectorize(
            symbol="ETHUSDT",
            ts_ms=1700000000000,
            direction="SHORT",
            scenario="range",
            indicators={"delta_z": 1.5},
            rule_score=0.6,
            rule_have=2,
            rule_need=3,
            cancel_spike_veto=1,
        )
        assert x_row == x_direct
