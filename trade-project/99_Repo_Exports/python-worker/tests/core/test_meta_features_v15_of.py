"""Smoke tests for meta_features_v15_of.

Append-only contract: v14_of ⊆ v15_of cols. New keys must come from
external_features_payload_v1 producer (so Train==Serve holds).
"""
from __future__ import annotations

import hashlib

import pytest

from core.meta_features_v14_of import META_FEAT_V14_OF_COLS
from core.meta_features_v15_of import (
    META_FEAT_V15_OF_COLS,
    META_FEAT_V15_OF_HASH,
    META_FEAT_V15_OF_NAME,
    META_FEAT_V15_OF_NEW_COLS,
    META_FEAT_V15_OF_TRANSFORMS,
    META_FEAT_V15_OF_VERSION,
    build_meta_features_v15_of,
)


class TestSchemaInvariants:
    def test_version_pin(self):
        assert META_FEAT_V15_OF_VERSION == 15
        assert META_FEAT_V15_OF_NAME == "meta_feat_v15_of"

    def test_v14_is_subset(self):
        assert set(META_FEAT_V14_OF_COLS).issubset(set(META_FEAT_V15_OF_COLS))

    def test_new_cols_disjoint_from_v14(self):
        v14 = set(META_FEAT_V14_OF_COLS)
        for k in META_FEAT_V15_OF_NEW_COLS:
            assert k not in v14, f"{k} already in v14_of"

    def test_cols_total(self):
        assert len(META_FEAT_V15_OF_COLS) == len(META_FEAT_V14_OF_COLS) + len(META_FEAT_V15_OF_NEW_COLS)

    def test_hash_stable(self):
        expected = hashlib.sha1(",".join(META_FEAT_V15_OF_COLS).encode("utf-8")).hexdigest()
        assert META_FEAT_V15_OF_HASH == expected

    def test_transforms_cover_new_cols(self):
        for k in META_FEAT_V15_OF_NEW_COLS:
            assert k in META_FEAT_V15_OF_TRANSFORMS

    def test_new_keys_in_external_payload(self):
        """Every new v15_of key must have a producer in
        core/external_features_payload_v1._NUM_KEYS — otherwise serve-time
        will silently emit 0.0 and skew the model."""
        from core.external_features_payload_v1 import external_feature_keys
        producers = set(external_feature_keys())
        for k in META_FEAT_V15_OF_NEW_COLS:
            assert k in producers, (
                f"{k} is in meta v15_of but no producer in external_features_payload — "
                "Train==Serve at risk"
            )


class TestBuilder:
    def test_empty_inputs_produce_full_schema(self):
        feat, _ = build_meta_features_v15_of(evidence={}, indicators={})
        for k in META_FEAT_V15_OF_COLS:
            assert k in feat
        for k in META_FEAT_V15_OF_NEW_COLS:
            assert feat[k] == 0.0

    def test_indicators_with_v4_priority(self):
        feat, _ = build_meta_features_v15_of(
            evidence={},
            indicators={"macro_event_severity": 1.0},
            indicators_with_v4={"macro_event_severity": 3.0},
        )
        assert feat["macro_event_severity"] == 3.0

    def test_hawkes_lam_pickup(self):
        feat, _ = build_meta_features_v15_of(
            evidence={},
            indicators={
                "hawkes_taker_buy_lam": 0.42,
                "hawkes_taker_sell_lam": 0.18,
            },
        )
        assert feat["hawkes_taker_buy_lam"] == 0.42
        assert feat["hawkes_taker_sell_lam"] == 0.18


class TestRegistryWiring:
    def test_registered_in_meta_schema_registry(self):
        from core.meta_schema_registry import META_SCHEMA_REGISTRY, get_meta_schema_spec
        assert META_FEAT_V15_OF_NAME in META_SCHEMA_REGISTRY
        spec = get_meta_schema_spec(META_FEAT_V15_OF_NAME)
        assert spec.version == META_FEAT_V15_OF_VERSION
        assert spec.hash == META_FEAT_V15_OF_HASH
        assert spec.builder is build_meta_features_v15_of

    def test_engine_schemas_has_v15_of(self):
        from core.of_confirm_engine import (
            META_FEAT_V15_OF_HASH as eng_hash,
            META_FEAT_V15_OF_NAME as eng_name,
            META_FEAT_V15_OF_VERSION as eng_version,
        )
        assert eng_name == META_FEAT_V15_OF_NAME
        assert eng_version == META_FEAT_V15_OF_VERSION
        assert eng_hash == META_FEAT_V15_OF_HASH


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
