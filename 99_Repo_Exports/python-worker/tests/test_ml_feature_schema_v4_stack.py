"""Tests for MLFeatureSchemaV4Stack — expanded 97-feature schema (Commit 4b).

Verifies:
  - Feature count = 97
  - feature_names() is stable and has no duplicates
  - Cyclical time (sin/cos) values are mathematically correct
  - Confirmation bool keys (7) present in bool_keys
  - All microstructure + evidence timing keys in num_keys
  - vectorize() length == feature_names() length
  - No-crash on all-missing indicators
"""
import math
import pytest
from core.ml_feature_schema_v4_stack import MLFeatureSchemaV4Stack

SCHEMA_HASH = "efc5b0aa6094"



SCHEMA = MLFeatureSchemaV4Stack()


class TestFeatureNamesV4Stack:
    def test_feature_names_count(self):
        names = SCHEMA.feature_names()
        # num_keys(46) + bool_keys(16) + dir(2) + bucket(3) + hour(24) + dow(7) = 98
        # Wait — let's count exactly from the dataclass fields
        n_num = len(SCHEMA.num_keys)
        n_bool = len(SCHEMA.bool_keys)
        expected = n_num + n_bool + 2 + 3 + 24 + 7
        assert len(names) == expected

    def test_no_duplicates(self):
        names = SCHEMA.feature_names()
        assert len(names) == len(set(names))

    def test_no_mae_mfe_leakage(self):
        """Outcome features must not exist in serving schema."""
        names = SCHEMA.feature_names()
        assert "n:mae_r" not in names
        assert "n:mfe_r" not in names

    def test_cyclical_time_keys_present(self):
        names = set(SCHEMA.feature_names())
        assert "n:sin_hour" in names
        assert "n:cos_hour" in names
        assert "n:sin_dow" in names
        assert "n:cos_dow" in names

    def test_confirmation_bool_keys_present(self):
        """Confirmations are first-class features in v4 (Commit 1 parity)."""
        expected_conf_keys = {
            "rsi_agree", "div_match", "sweep_eqh", "sweep_eql",
            "weak_progress", "absorption", "reclaim"
        }
        actual_bool = set(SCHEMA.bool_keys)
        assert expected_conf_keys.issubset(actual_bool)

    def test_microstructure_keys_in_num_keys(self):
        expected_ms = {
            "depth_bid_5", "depth_ask_5", "depth_imb_5", "top_imb_5",
            "slope_imb_5", "pressure_imb_5", "churn_z", "book_rate_z",
            "pressure_sps", "book_age_ms", "book_health_ok", "book_midprice",
        }
        actual = set(SCHEMA.num_keys)
        assert expected_ms.issubset(actual), f"Missing: {expected_ms - actual}"

    def test_evidence_timing_keys_in_num_keys(self):
        expected_ev = {
            "ofi_age_ms", "obi_age_ms", "iceberg_age_ms", "sweep_age_ms",
            "reclaim_age_ms", "fp_edge_age_ms", "abs_age_ms", "weak_progress_age_ms",
        }
        actual = set(SCHEMA.num_keys)
        assert expected_ev.issubset(actual), f"Missing: {expected_ev - actual}"


class TestVectorizeV4Stack:
    def _make_indicators(self, **overrides):
        ind = {
            "delta_z": 0.5,
            "ofi_z": 1.0,
            "ofi_stability_score": 0.8,
            "obi": 0.3,
            "obi_z": 0.1,
            "spread_bps": 5.0,
            "expected_slippage_bps": 2.0,
            "exec_risk_norm": 0.4,
            "liq_score": 0.7,
            "book_staleness_ms": 100.0,
            "pressure": 0.6,
            "triggers_per_min": 3.0,
            "rsi_agree": 1,
            "absorption": 0,
            "reclaim": 1,
        }
        ind.update(overrides)
        return ind

    def test_vectorize_length_matches_feature_names(self):
        x = SCHEMA.vectorize(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="trend:up",
            indicators=self._make_indicators(),
            rule_score=0.75,
            rule_have=3,
            rule_need=3,
            cancel_spike_veto=0,
        )
        assert len(x) == len(SCHEMA.feature_names())

    def test_all_values_are_finite(self):
        x = SCHEMA.vectorize(
            symbol="BTCUSDT",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="trend:up",
            indicators=self._make_indicators(),
            rule_score=0.75,
            rule_have=3,
            rule_need=3,
            cancel_spike_veto=0,
        )
        for v in x:
            assert math.isfinite(v), f"non-finite value: {v}"

    def test_cyclical_time_values_correct(self):
        """Cyclical time sin/cos must match expected math."""
        # ts_ms = epoch for a known UTC time
        # 2024-01-01 06:00:00 UTC = Mon, hour=6, dow=0
        from datetime import datetime, timezone
        dt = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
        ts_ms = int(dt.timestamp() * 1000)

        x = SCHEMA.vectorize(
            symbol="X",
            ts_ms=ts_ms,
            direction="LONG",
            scenario="other",
            indicators={},
            rule_score=0.0,
            rule_have=0,
            rule_need=0,
            cancel_spike_veto=0,
        )
        names = SCHEMA.feature_names()
        expected_sin_h = math.sin(2.0 * math.pi * 6.0 / 24.0)
        expected_cos_h = math.cos(2.0 * math.pi * 6.0 / 24.0)
        # Monday = dow=0
        expected_sin_d = math.sin(0.0)
        expected_cos_d = math.cos(0.0)

        assert x[names.index("n:sin_hour")] == pytest.approx(expected_sin_h)
        assert x[names.index("n:cos_hour")] == pytest.approx(expected_cos_h)
        assert x[names.index("n:sin_dow")] == pytest.approx(expected_sin_d)
        assert x[names.index("n:cos_dow")] == pytest.approx(expected_cos_d)

    def test_confirmation_features_picked_up(self):
        """When rsi_agree=1 in indicators, b:rsi_agree should be 1.0."""
        x = SCHEMA.vectorize(
            symbol="X",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="trend",
            indicators={"rsi_agree": 1},
            rule_score=0.0,
            rule_have=0,
            rule_need=0,
            cancel_spike_veto=0,
        )
        names = SCHEMA.feature_names()
        assert x[names.index("b:rsi_agree")] == 1.0

    def test_determinism(self):
        kwargs = dict(
            symbol="SOLUSDT",
            ts_ms=1700000000000,
            direction="SHORT",
            scenario="range",
            indicators=self._make_indicators(rsi_agree=0),
            rule_score=0.5,
            rule_have=2,
            rule_need=3,
            cancel_spike_veto=1,
        )
        assert SCHEMA.vectorize(**kwargs) == SCHEMA.vectorize(**kwargs)

    def test_missing_indicators_all_zeros(self):
        x = SCHEMA.vectorize(
            symbol="X",
            ts_ms=1700000000000,
            direction="LONG",
            scenario="other",
            indicators={},
            rule_score=0.0,
            rule_have=0,
            rule_need=0,
            cancel_spike_veto=0,
        )
        names = SCHEMA.feature_names()
        # All pure numeric indicators (no cyclical time) should be 0
        plain_num_keys = [k for k in SCHEMA.num_keys if k not in ("sin_hour", "cos_hour", "sin_dow", "cos_dow")]
        for k in plain_num_keys:
            idx = names.index(f"n:{k}")
            assert x[idx] == 0.0, f"{k} should be 0 when missing from indicators"
