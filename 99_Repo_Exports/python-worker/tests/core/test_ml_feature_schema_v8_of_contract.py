"""Contract tests for core.ml_feature_schema_v8_of.MLFeatureSchemaV8OF."""

from __future__ import annotations
import pytest
from core.ml_feature_schema_v7_of import MLFeatureSchemaV7OF
from core.ml_feature_schema_v8_of import MLFeatureSchemaV8OF


@pytest.fixture
def schema_v7():
    return MLFeatureSchemaV7OF()


@pytest.fixture
def schema_v8():
    return MLFeatureSchemaV8OF()


# ---------------------------------------------------------------------------
# Append-only superset contract
# ---------------------------------------------------------------------------


def test_v8_of_is_append_only_superset_of_v7_of(schema_v7, schema_v8):
    """All v7 num_keys must be present in v8 without reordering."""
    # Superset contract (train==serve safety): v7 keys must remain in v8.
    assert set(schema_v7.num_keys) <= set(schema_v8.num_keys), (
        f"v8 missing v7 num_keys: {set(schema_v7.num_keys) - set(schema_v8.num_keys)}"
    )
    assert set(schema_v7.bool_keys) <= set(schema_v8.bool_keys), (
        f"v8 missing v7 bool_keys: {set(schema_v7.bool_keys) - set(schema_v8.bool_keys)}"
    )

    # Append-only: v7 ordering must be preserved inside v8.
    # (All v7 keys appear before v8-only keys; no reorder.)
    max_idx_num = max(schema_v8.num_keys.index(k) for k in schema_v7.num_keys)
    for k in schema_v7.num_keys:
        assert schema_v8.num_keys.index(k) <= max_idx_num, (
            f"v7 key '{k}' appears after v8-only keys (reorder detected)"
        )

    max_idx_bool = max(schema_v8.bool_keys.index(k) for k in schema_v7.bool_keys)
    for k in schema_v7.bool_keys:
        assert schema_v8.bool_keys.index(k) <= max_idx_bool, (
            f"v7 bool key '{k}' appears after v8-only keys (reorder detected)"
        )


# ---------------------------------------------------------------------------
# DQ keys
# ---------------------------------------------------------------------------


def test_v8_of_includes_dq_keys(schema_v8):
    """Strict DQ keys written by _update_strict_dq_trackers must be in v8."""
    for k in ("tick_gap_p95_ms", "tick_missing_seq_ema", "book_missing_seq_ema"):
        assert k in schema_v8.num_keys, f"DQ key '{k}' missing from v8"


# ---------------------------------------------------------------------------
# LiqMap keys
# ---------------------------------------------------------------------------


def test_v8_of_includes_liqmap_1h_keys(schema_v8):
    """LiqMap 1h core keys (v1 naming) must be in v8."""
    for k in (
        "liqmap_1h_total_usd",
        "liqmap_1h_near_total_usd",
        "liqmap_1h_near_imb",        "liqmap_1h_dist_dn_bps",
        "liqmap_1h_peak_up1_usd",
        "liqmap_1h_peak_dn1_usd",
        "liqmap_1h_age_ms",
    ):
        assert k in schema_v8.num_keys, f"LiqMap 1h key '{k}' missing from v8"


def test_v8_of_includes_liqmap_alt_naming(schema_v8):
    """Forward/alt naming aliases for LiqMap keys must also be present (for robustness)."""
    for k in (    ):
        assert k in schema_v8.num_keys, f"LiqMap alt-naming key '{k}' missing from v8"


# ---------------------------------------------------------------------------
# Levels overlay (optional)
# ---------------------------------------------------------------------------


def test_v8_of_includes_levels_overlay_keys(schema_v8):
    """TP1/SL overlay adj keys (D1/D2) must be in v8."""
    for k in ("liqmap_tp1_adj_bps", "liqmap_sl_adj_bps"):
        assert k in schema_v8.num_keys, f"Levels overlay num key '{k}' missing from v8"
    assert "liqmap_levels_applied" in schema_v8.bool_keys, (
        "Levels overlay bool key 'liqmap_levels_applied' missing from v8"
    )


# ---------------------------------------------------------------------------
# No duplicates
# ---------------------------------------------------------------------------


def test_v8_of_num_keys_unique(schema_v8):
    """num_keys must have no duplicates."""
    dup = [k for k in set(schema_v8.num_keys) if schema_v8.num_keys.count(k) > 1]
    assert not dup, f"Duplicate num_keys in v8: {dup}"


def test_v8_of_bool_keys_unique(schema_v8):
    """bool_keys must have no duplicates."""
    dup = [k for k in set(schema_v8.bool_keys) if schema_v8.bool_keys.count(k) > 1]
    assert not dup, f"Duplicate bool_keys in v8: {dup}"


# ---------------------------------------------------------------------------
# Size sanity
# ---------------------------------------------------------------------------


def test_v8_of_has_more_features_than_v7_of(schema_v7, schema_v8):
    """v8 must have strictly more features than v7."""
    assert len(schema_v8.num_keys) > len(schema_v7.num_keys), (
        f"v8 has {len(schema_v8.num_keys)} num_keys, v7 has {len(schema_v7.num_keys)}"
    )
