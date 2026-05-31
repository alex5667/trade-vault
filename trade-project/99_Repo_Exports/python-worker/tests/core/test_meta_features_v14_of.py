"""Smoke tests for meta_features_v14_of.

Train==Serve guarantee: every key in META_FEAT_V14_OF_COLS must be
populated by build_meta_features_v14_of even when source dicts are empty.
The schema registry must expose v14_of with stable hash matching the
sha1 of joined column names.
"""
from __future__ import annotations

import hashlib

import pytest

from core.meta_features_v13_of import META_FEAT_V13_OF_COLS
from core.meta_features_v14_of import (
    META_FEAT_V14_OF_COLS,
    META_FEAT_V14_OF_HASH,
    META_FEAT_V14_OF_NAME,
    META_FEAT_V14_OF_NEW_COLS,
    META_FEAT_V14_OF_TRANSFORMS,
    META_FEAT_V14_OF_VERSION,
    build_meta_features_v14_of,
)


class TestSchemaInvariants:
    def test_version_pin(self):
        assert META_FEAT_V14_OF_VERSION == 14
        assert META_FEAT_V14_OF_NAME == "meta_feat_v14_of"

    def test_v13_is_subset(self):
        """Append-only contract: v14_of contains all v13_of keys."""
        assert set(META_FEAT_V13_OF_COLS).issubset(set(META_FEAT_V14_OF_COLS))

    def test_new_cols_disjoint_from_v13(self):
        """New keys must not duplicate v13_of."""
        v13 = set(META_FEAT_V13_OF_COLS)
        for k in META_FEAT_V14_OF_NEW_COLS:
            assert k not in v13, f"{k} already in v13_of"

    def test_cols_total(self):
        assert len(META_FEAT_V14_OF_COLS) == len(META_FEAT_V13_OF_COLS) + len(META_FEAT_V14_OF_NEW_COLS)

    def test_hash_stable(self):
        """schema_hash is determined by ordered column names — pin it."""
        expected = hashlib.sha1(",".join(META_FEAT_V14_OF_COLS).encode("utf-8")).hexdigest()
        assert META_FEAT_V14_OF_HASH == expected

    def test_transforms_cover_new_cols(self):
        for k in META_FEAT_V14_OF_NEW_COLS:
            assert k in META_FEAT_V14_OF_TRANSFORMS, f"{k} has no transform spec"


class TestBuilder:
    def test_empty_inputs_produce_full_schema(self):
        feat, missing = build_meta_features_v14_of(evidence={}, indicators={})
        # Every column must be present
        for k in META_FEAT_V14_OF_COLS:
            assert k in feat, f"{k} missing from build output"
        # All present cols also reported as missing (since sources empty)
        for k in META_FEAT_V14_OF_NEW_COLS:
            assert feat[k] == 0.0

    def test_indicators_with_v4_priority(self):
        """indicators_with_v4 is the primary source."""
        feat, _ = build_meta_features_v14_of(
            evidence={"fear_greed_index": 10.0},
            indicators={"fear_greed_index": 20.0},
            indicators_with_v4={"fear_greed_index": 30.0},
        )
        assert feat["fear_greed_index"] == 30.0

    def test_indicators_fallback(self):
        """When indicators_with_v4 lacks the key, indicators wins."""
        feat, _ = build_meta_features_v14_of(
            evidence={"og_ok": 1.0},
            indicators={"og_ok": 2.0},
            indicators_with_v4={},
        )
        assert feat["og_ok"] == 2.0

    def test_evidence_last_resort(self):
        feat, _ = build_meta_features_v14_of(
            evidence={"squeeze_risk_score": 0.7},
            indicators={},
        )
        assert feat["squeeze_risk_score"] == 0.7

    def test_non_numeric_returns_default(self):
        feat, _ = build_meta_features_v14_of(
            evidence={},
            indicators={"liq_impulse_score": "not_a_number"},
        )
        # non-numeric → default 0.0 (no exception)
        assert feat["liq_impulse_score"] == 0.0


class TestRegistryWiring:
    def test_registered_in_meta_schema_registry(self):
        from core.meta_schema_registry import META_SCHEMA_REGISTRY, get_meta_schema_spec
        assert META_FEAT_V14_OF_NAME in META_SCHEMA_REGISTRY
        spec = get_meta_schema_spec(META_FEAT_V14_OF_NAME)
        assert spec.version == META_FEAT_V14_OF_VERSION
        assert spec.hash == META_FEAT_V14_OF_HASH
        assert spec.builder is build_meta_features_v14_of

    def test_engine_schemas_has_v14_of(self):
        """SCHEMAS dict in of_confirm_engine (built at runtime inside build())
        is sourced from these constants — ensure constants are importable
        from the engine module path."""
        from core.of_confirm_engine import (
            META_FEAT_V14_OF_HASH as eng_hash,
            META_FEAT_V14_OF_NAME as eng_name,
            META_FEAT_V14_OF_VERSION as eng_version,
        )
        assert eng_name == META_FEAT_V14_OF_NAME
        assert eng_version == META_FEAT_V14_OF_VERSION
        assert eng_hash == META_FEAT_V14_OF_HASH


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
