"""
Tests for core.feature_enricher_v1._enrich_vol_features

Goal: verify that _enrich_vol_features correctly extracts vol_fast_bps,
vol_slow_bps, vol_regime_code, vol_of_vol, vol_ratio_z from indicators dict,
and returns {} (not zeros) when no alias is present.

This test was added as part of the P1 audit (vol estimators = 0 investigation):
  - _enrich_vol_features reads only from indicators (not Redis).
  - Returns {} when no canonical or alias key present → upstream not publishing.
  - Returns non-zero when any alias key is present.
"""
import pytest
from core.feature_enricher_v1 import _enrich_vol_features


class TestEnrichVolFeaturesCanonicalKeys:
    """Direct canonical key → output mapping."""

    def test_vol_fast_bps_direct(self):
        out = _enrich_vol_features({"vol_fast_bps": 25.0})
        assert out.get("vol_fast_bps") == 25.0

    def test_vol_slow_bps_direct(self):
        out = _enrich_vol_features({"vol_slow_bps": 15.0})
        assert out.get("vol_slow_bps") == 15.0

    def test_vol_ratio_z_direct(self):
        out = _enrich_vol_features({"vol_ratio_z": 1.5})
        assert out.get("vol_ratio_z") == 1.5

    def test_vol_of_vol_direct(self):
        out = _enrich_vol_features({"vol_of_vol": 0.3})
        assert out.get("vol_of_vol") == 0.3

    def test_vol_regime_code_direct(self):
        out = _enrich_vol_features({"vol_regime_code": 3.0})
        assert out.get("vol_regime_code") == 3.0


class TestEnrichVolFeaturesAliasKeys:
    """Alias keys must resolve to canonical output keys."""

    def test_vol_fast_alias_vol_compression_score(self):
        out = _enrich_vol_features({"vol_compression_score": 18.5})
        assert "vol_fast_bps" in out
        assert out["vol_fast_bps"] == pytest.approx(18.5)

    def test_vol_fast_alias_atr_q(self):
        out = _enrich_vol_features({"atr_q": 12.0})
        assert out.get("vol_fast_bps") == pytest.approx(12.0)

    def test_vol_fast_alias_vol_fast(self):
        out = _enrich_vol_features({"vol_fast": 30.0})
        assert out.get("vol_fast_bps") == pytest.approx(30.0)

    def test_vol_slow_alias_vol_expansion_score(self):
        out = _enrich_vol_features({"vol_expansion_score": 10.0})
        assert "vol_slow_bps" in out
        assert out["vol_slow_bps"] == pytest.approx(10.0)

    def test_vol_ratio_z_alias_sc_vol_ratio_z(self):
        out = _enrich_vol_features({"sc_vol_ratio_z": 2.1})
        assert out.get("vol_ratio_z") == pytest.approx(2.1)

    def test_vol_regime_code_alias_regime_code(self):
        out = _enrich_vol_features({"regime_code": 2.0})
        assert out.get("vol_regime_code") == pytest.approx(2.0)

    def test_vol_regime_code_alias_deribit(self):
        out = _enrich_vol_features({"deribit_vol_regime_code": 4.0})
        assert out.get("vol_regime_code") == pytest.approx(4.0)


class TestEnrichVolFeaturesZeroSkip:
    """Zero values must be SKIPPED — continue scanning aliases (not returned as 0.0)."""

    def test_zero_vol_fast_bps_skips_to_alias(self):
        # canonical key = 0 → should skip and try alias
        out = _enrich_vol_features({
            "vol_fast_bps": 0.0,  # skipped
            "vol_compression_score": 22.0,  # should be picked
        })
        # vol_fast_bps should come from compression_score, not 0
        assert out.get("vol_fast_bps") == pytest.approx(22.0)

    def test_zero_vol_slow_bps_no_fallback(self):
        # only zero slow → not emitted
        out = _enrich_vol_features({"vol_slow_bps": 0.0})
        assert "vol_slow_bps" not in out


class TestEnrichVolFeaturesDerivedKeys:
    """Derived keys: vol_slow from ratio, vol_regime_code from fast/slow."""

    def test_vol_slow_derived_from_ratio(self):
        # vol_fast_bps=20, vol_ratio_fast_slow=2.0 → vol_slow_bps=10
        out = _enrich_vol_features({
            "vol_fast_bps": 20.0,
            "vol_ratio_fast_slow": 2.0,
        })
        assert "vol_slow_bps" in out
        assert out["vol_slow_bps"] == pytest.approx(10.0, rel=1e-4)

    def test_vol_regime_code_derived_from_fast_slow(self):
        # should derive regime_code when only fast/slow available
        out = _enrich_vol_features({
            "vol_fast_bps": 30.0,
            "vol_slow_bps": 10.0,
        })
        assert "vol_regime_code" in out
        assert out["vol_regime_code"] >= 0  # valid regime code

    def test_no_vol_slow_if_no_ratio(self):
        # fast only, no ratio → slow not derived
        out = _enrich_vol_features({"vol_fast_bps": 20.0})
        # slow_bps absent (no ratio source)
        # regime_code may be derived from fast alone
        assert "vol_slow_bps" not in out


class TestEnrichVolFeaturesEmptyInput:
    """Empty indicators → empty output (no fake zeros)."""

    def test_empty_indicators_returns_empty(self):
        out = _enrich_vol_features({})
        # No vol keys at all → _enrich_vol_features must return {} not {key: 0.0}
        assert out == {}

    def test_unrelated_keys_ignored(self):
        out = _enrich_vol_features({
            "delta_z": 1.5,
            "ofi_z": 0.3,
            "spread_bps": 5.0,
        })
        assert out == {}


class TestEnrichVolFeaturesP1Diagnostic:
    """
    P1 diagnostic: 'vol estimators = 0' root cause.

    _enrich_vol_features returns {} when no alias key is present.
    This means vol_fast_bps/slow/regime appear as 0 in the feature vector
    NOT because _enrich_vol_features writes 0, but because the schema
    defaults missing keys to 0.0.

    Root cause: upstream publisher does not emit ANY of these aliases:
      vol_fast_bps, vol_fast, vol_fast_atr, atr_fast_bps,
      vol_compression_score, atr_q

    Fix: check which key the publisher emits and add it to indicators before
    _enrich_vol_features is called (or start microstructure_metrics_v2 service).
    """

    def test_vol_fast_bps_absent_means_empty_not_zero(self):
        """
        CRITICAL: if no vol_fast alias present, output is {} not {vol_fast_bps: 0}.
        The 0 appears downstream when schema fills missing keys with 0.0.
        """
        out = _enrich_vol_features({
            "delta_z": 2.0,
            "spread_bps": 4.5,
        })
        assert "vol_fast_bps" not in out
        assert "vol_slow_bps" not in out
        assert "vol_regime_code" not in out

    def test_microstructure_vol_of_vol_path(self):
        """
        vol_of_vol comes from microstruct:ctx:{symbol} via _enrich_microstruct_ctx,
        not from _enrich_vol_features. Confirm it passes through when present.
        """
        out = _enrich_vol_features({"vol_of_vol": 0.42})
        assert out.get("vol_of_vol") == pytest.approx(0.42)
