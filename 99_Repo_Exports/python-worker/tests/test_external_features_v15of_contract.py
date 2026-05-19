"""Contract: every key emitted by external_features_payload_v1 must be
present in V15_OF_NUMERIC_KEYS.

This pins the schema-gap remediation: v15_of was introduced (2026-05-18)
specifically to be the append-only superset that covers
core.external_features_payload_v1._NUM_KEYS. If a new Phase-X key is added
to _NUM_KEYS without bumping v15_of, train/serve skew returns silently
(infer_feature_cols fallback leaks the key non-deterministically).
"""
from __future__ import annotations

from core.external_features_payload_v1 import _NUM_KEYS
from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS


def test_external_features_payload_keys_subset_of_v15_of():
    payload_keys = set(_NUM_KEYS)
    schema_keys = set(V15_OF_NUMERIC_KEYS)
    missing = payload_keys - schema_keys
    assert not missing, (
        f"{len(missing)} keys emitted by external_features_payload but absent "
        f"from V15_OF_NUMERIC_KEYS — train/serve skew risk. "
        f"Add them to core/ml_feature_schema_v15_of.py and reseed pin "
        f"`cfg:feature_registry:edge_stack:v15_of`. Missing: {sorted(missing)[:20]}"
    )


def test_v15_of_has_no_duplicate_keys():
    assert len(V15_OF_NUMERIC_KEYS) == len(set(V15_OF_NUMERIC_KEYS)), (
        "V15_OF_NUMERIC_KEYS contains duplicates — vectorizer column order "
        "becomes non-deterministic. Inspect the tuple ordering in "
        "core/ml_feature_schema_v15_of.py."
    )
