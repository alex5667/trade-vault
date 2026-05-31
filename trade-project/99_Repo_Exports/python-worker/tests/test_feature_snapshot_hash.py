"""Plan 3 / Step 1 — schema fingerprinting tests."""
from __future__ import annotations

from core.feature_snapshot_hash import (
    compute_feature_cols_hash,
    compute_schema_hash,
    extract_feature_cols,
)


def test_schema_hash_empty():
    assert compute_schema_hash({}) == "empty"


def test_schema_hash_deterministic_under_key_order():
    a = {"alpha": 1.0, "beta": 2.0, "gamma": {"x": 1, "y": 2}}
    b = {"gamma": {"y": 2, "x": 1}, "beta": 2.0, "alpha": 1.0}
    assert compute_schema_hash(a) == compute_schema_hash(b)


def test_schema_hash_changes_when_key_added():
    a = {"alpha": 1.0, "beta": 2.0}
    b = {"alpha": 1.0, "beta": 2.0, "gamma": 3.0}
    assert compute_schema_hash(a) != compute_schema_hash(b)


def test_schema_hash_changes_when_nested_key_added():
    a = {"alpha": 1.0, "nested": {"x": 1}}
    b = {"alpha": 1.0, "nested": {"x": 1, "y": 2}}
    assert compute_schema_hash(a) != compute_schema_hash(b)


def test_schema_hash_ignores_value_changes():
    """Schema is about KEYS — value drift is detected by the model, not the hash."""
    a = {"alpha": 1.0, "beta": 2.0}
    b = {"alpha": 99.0, "beta": -42.0}
    assert compute_schema_hash(a) == compute_schema_hash(b)


def test_schema_hash_with_list_indexed():
    """Lists are positional — different lengths change the schema."""
    a = {"buckets": [1, 2, 3]}
    b = {"buckets": [1, 2, 3, 4]}
    assert compute_schema_hash(a) != compute_schema_hash(b)


def test_feature_cols_hash_empty():
    assert compute_feature_cols_hash([]) == "empty"


def test_feature_cols_hash_order_independent():
    assert compute_feature_cols_hash(["a", "b", "c"]) == compute_feature_cols_hash(["c", "b", "a"])


def test_feature_cols_hash_dedups():
    assert compute_feature_cols_hash(["a", "a", "b"]) == compute_feature_cols_hash(["a", "b"])


def test_feature_cols_hash_skips_empty():
    assert compute_feature_cols_hash(["a", "", "b", None]) == compute_feature_cols_hash(["a", "b"])  # type: ignore


def test_extract_feature_cols_top_level_only():
    inds = {
        "spread_bps": 1.0,
        "depth": 5,
        "regime": "momentum",
        "nested": {"hidden": 1.0},  # excluded
        "list_field": [1, 2, 3],     # excluded
    }
    cols = extract_feature_cols(inds)
    assert "spread_bps" in cols
    assert "depth" in cols
    assert "regime" in cols
    assert "nested" not in cols
    assert "list_field" not in cols


def test_hash_short_prefix():
    h = compute_schema_hash({"x": 1, "y": 2})
    assert len(h) == 12
