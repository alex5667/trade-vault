"""Tests for meta_features_v7 builder — confirmation flags from strings and indicators.

Exercises both offline (confirmations list) and online (indicators["conf_*"]) paths.
"""

from core.meta_features_v7 import (
    META_FEAT_V7_COLS,
    META_FEAT_V7_HASH,
    META_FEAT_V7_NAME,
    META_FEAT_V7_NEW_COLS,
    META_FEAT_V7_VERSION,
    build_meta_features_v7,
)


def _base_kwargs(**overrides):
    """Return minimal valid kwargs for build_meta_features_v7."""
    kw = dict(
        evidence={"scenario_v4": "trend_cont"},
        indicators={},
        runtime_snap=None,
        runtime_prev_snap=None,
        indicators_with_v4={},
        legs={},
        have=0,
        need=0,
        ok_soft=0,
        rule_score=0.5,
        exec_risk_norm=0.0,
        exec_risk_bps=0.0,
        ml_scenario="trend_cont",
    )
    kw.update(overrides)
    return kw


class TestMeta7Schema:
    def test_name_and_version(self):
        assert META_FEAT_V7_NAME == "meta_feat_v7"
        assert META_FEAT_V7_VERSION == 7

    def test_new_cols_present_in_full_cols(self):
        for col in META_FEAT_V7_NEW_COLS:
            assert col in META_FEAT_V7_COLS

    def test_all_new_cols(self):
        assert set(META_FEAT_V7_NEW_COLS) == {
            "conf_rsi_agree",
            "conf_div_match",
            "conf_div_match_fallback",
            "conf_sweep_eqh",
            "conf_sweep_eql",
            "conf_sweep_any",
            "conf_iceberg_strict",
            "conf_obi_stable",
            "conf_reclaim",
            "conf_weak_progress",
        }

    def test_hash_is_stable(self):
        """Hash must be deterministic (same value on every import)."""
        import hashlib
        expected = hashlib.sha1(
            ",".join(META_FEAT_V7_COLS).encode("utf-8")
        ).hexdigest()
        assert expected == META_FEAT_V7_HASH


class TestMeta7FromStrings:
    """Offline path: confirmations list inside evidence[]."""

    def test_flags_extracted_from_strings(self):
        evidence = {
            "confirmations": ["rsi_agree=1", "div_match=1", "sweep_eqh=1"],
            "scenario_v4": "trend_cont",
        }
        feat, _ = build_meta_features_v7(**_base_kwargs(evidence=evidence))
        assert feat["conf_rsi_agree"] == 1.0
        assert feat["conf_div_match"] == 1.0
        assert feat["conf_sweep_eqh"] == 1.0
        assert feat["conf_sweep_eql"] == 0.0

    def test_sweep_eql_from_string(self):
        evidence = {"confirmations": ["sweep_eql=1"], "scenario_v4": ""}
        feat, _ = build_meta_features_v7(**_base_kwargs(evidence=evidence))
        assert feat["conf_sweep_eql"] == 1.0
        assert feat["conf_sweep_eqh"] == 0.0

    def test_all_zeros_no_confirmations(self):
        feat, _ = build_meta_features_v7(**_base_kwargs())
        for col in META_FEAT_V7_NEW_COLS:
            assert feat[col] == 0.0


class TestMeta7FromIndicators:
    """Online path: indicators dict with conf_* keys."""

    def test_flags_extracted_from_indicators(self):
        indicators = {"conf_rsi_agree": 1, "conf_div_match": 0, "conf_sweep_eql": 1}
        feat, _ = build_meta_features_v7(**_base_kwargs(indicators=indicators))
        assert feat["conf_rsi_agree"] == 1.0
        assert feat["conf_div_match"] == 0.0
        assert feat["conf_sweep_eqh"] == 0.0
        assert feat["conf_sweep_eql"] == 1.0

    def test_indicators_override_list(self):
        """When both indicators and list are present, OR semantics apply (max wins)."""
        evidence = {"confirmations": ["rsi_agree=1"], "scenario_v4": ""}
        indicators = {"conf_rsi_agree": 0}
        # max(list=1, indicator=0) should be 1
        feat, _ = build_meta_features_v7(
            **_base_kwargs(evidence=evidence, indicators=indicators)
        )
        assert feat["conf_rsi_agree"] == 1.0

    def test_all_confirmation_cols_are_floats(self):
        indicators = {"conf_rsi_agree": 1}
        feat, _ = build_meta_features_v7(**_base_kwargs(indicators=indicators))
        for col in META_FEAT_V7_NEW_COLS:
            assert isinstance(feat[col], float), f"{col} must be float"

    def test_v6_cols_still_present(self):
        """Confirm v7 still carries all v6 columns (backward compat)."""
        from core.meta_features_v6 import META_FEAT_V6_COLS

        feat, _ = build_meta_features_v7(**_base_kwargs())
        for col in META_FEAT_V6_COLS:
            assert col in feat, f"v6 col {col!r} missing from v7 output"
