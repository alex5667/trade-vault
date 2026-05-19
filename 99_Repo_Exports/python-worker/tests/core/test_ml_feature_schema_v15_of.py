"""Unit tests for ml_feature_schema_v15_of — append-only schema invariants."""
from __future__ import annotations


def test_v15_of_count_pinned():
    """v15_of must hold exactly the pinned _EXPECTED_KEYS count."""
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS, _EXPECTED_KEYS
    assert len(V15_OF_NUMERIC_KEYS) == _EXPECTED_KEYS


def test_v15_of_schema_hash_pinned():
    """SCHEMA_HASH must stay stable until a coordinated bump."""
    from core.ml_feature_schema_v15_of import SCHEMA_HASH
    assert SCHEMA_HASH == "v15of_v14base_p82_p83_p84_p85_p1_p2_p3_2026_05_18"


def test_v15_of_append_only_over_v14_of():
    """Append-only invariant: every v14_of key must remain in v15_of."""
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
    removed = set(V14_OF_NUMERIC_KEYS) - set(V15_OF_NUMERIC_KEYS)
    assert not removed, f"v15_of removed keys from v14_of: {sorted(removed)}"


def test_v15_of_append_only_over_v13_of():
    """Transitive: v13_of (prod champion) must remain ⊆ v15_of."""
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
    removed = set(V13_OF_NUMERIC_KEYS) - set(V15_OF_NUMERIC_KEYS)
    assert not removed, f"v15_of removed keys from v13_of: {sorted(removed)}"


def test_v15_of_keys_unique_and_sorted():
    """V15_OF_NUMERIC_KEYS must be deduplicated and sorted (deterministic order)."""
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
    assert V15_OF_NUMERIC_KEYS == sorted(set(V15_OF_NUMERIC_KEYS))


def test_v15_of_covers_external_features_payload():
    """v15_of must be a superset of every key emitted by external_features_payload_v1.

    Closes the schema gap from audit_v14_of_schema_gap_fixes_2026_05_18.
    """
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
    from core.external_features_payload_v1 import _NUM_KEYS, _BOOL_KEYS
    schema = set(V15_OF_NUMERIC_KEYS)
    emitted = set(_NUM_KEYS) | set(_BOOL_KEYS)
    gap = emitted - schema
    assert not gap, (
        f"v15_of missing {len(gap)} keys emitted by external_features_payload: "
        f"{sorted(gap)[:10]}"
    )


def test_v15_of_covers_og_keys():
    """v15_of must include all 16 og_* keys (carried over from v14_of)."""
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
    from core.v14_of_features import og_keys
    missing = [k for k in og_keys() if k not in V15_OF_NUMERIC_KEYS]
    assert not missing, f"v15_of missing og_* keys: {missing}"


def test_v15_of_info_groups_consistent():
    """v15_of_info() group counts must sum to (n_new_keys) after dedup."""
    from core.ml_feature_schema_v15_of import v15_of_info
    info = v15_of_info()
    assert info["ver"] == "v15_of"
    assert info["n_numeric_keys"] == 515
    assert info["n_v14_of_base"] == 359
    assert info["n_new_keys"] == 156


def test_v15_of_feature_registry_dispatcher():
    """feature_registry must recognize v15_of and v15 alias."""
    from core.feature_registry import get_schema_info, get_edge_stack_feature_spec
    info = get_schema_info("v15_of")
    info_alias = get_schema_info("v15")
    assert info.feature_names == info_alias.feature_names
    spec = get_edge_stack_feature_spec("v15_of")
    assert spec.feature_cols  # non-empty
    # feature_cols_hash should be deterministic
    spec2 = get_edge_stack_feature_spec("v15_of")
    assert spec.feature_cols_hash == spec2.feature_cols_hash
