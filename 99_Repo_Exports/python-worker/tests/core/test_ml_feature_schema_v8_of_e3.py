"""E3: Tests for MLFeatureSchemaV8OF.

Goals
-----
1) Uniqueness: no duplicate keys within num/bool lists, and no overlap between them.
2) Superset/append-only: v8 keeps v7 keys as a stable prefix (Train==Serve determinism).
3) Presence: v8 contains DQ + LiqMap (1h) + optional levels overlay keys.

These tests are intentionally lightweight (no Redis, no heavy engine wiring).
"""

from __future__ import annotations

from core.ml_feature_schema_v7_of import MLFeatureSchemaV7OF
from core.ml_feature_schema_v8_of import MLFeatureSchemaV8OF, MLFeatureSchemaV8OFStable

SCHEMA_HASH = "d6c13063a0f2"



def _assert_unique_keys(*, num_keys: list[str], bool_keys: list[str]) -> None:
    # Uniqueness within each list.
    assert len(num_keys) == len(set(num_keys)), "duplicate numeric keys in schema"
    assert len(bool_keys) == len(set(bool_keys)), "duplicate boolean keys in schema"
    # Disjoint sets: a key must never be both numeric and boolean.
    inter = set(num_keys).intersection(bool_keys)
    assert not inter, f"keys overlap between num/bool: {sorted(list(inter))[:20]}"


def test_v8_unique_keys() -> None:
    s = MLFeatureSchemaV8OF()
    _assert_unique_keys(num_keys=list(s.num_keys or []), bool_keys=list(s.bool_keys or []))


def test_v8_stable_unique_keys() -> None:
    # Stable variant must preserve uniqueness even when denylist prunes keys.
    s = MLFeatureSchemaV8OFStable()
    _assert_unique_keys(num_keys=list(s.num_keys or []), bool_keys=list(s.bool_keys or []))


def test_v8_superset_of_v7_append_only_prefix() -> None:
    v7 = MLFeatureSchemaV7OF()
    v8 = MLFeatureSchemaV8OF()

    v7_num = list(v7.num_keys or [])
    v7_bool = list(v7.bool_keys or [])
    v8_num = list(v8.num_keys or [])
    v8_bool = list(v8.bool_keys or [])

    # v8 must keep v7 as a *stable prefix* to avoid silent feature reordering.
    assert v8_num[: len(v7_num)] == v7_num, "v8.num_keys must be v7.num_keys as prefix"
    assert v8_bool[: len(v7_bool)] == v7_bool, "v8.bool_keys must be v7.bool_keys as prefix"

    # And of course v8 must be a superset.
    assert set(v7_num).issubset(set(v8_num))
    assert set(v7_bool).issubset(set(v8_bool))


def test_v8_contains_liqmap_and_dq_keys() -> None:
    v8 = MLFeatureSchemaV8OF()
    num = set(v8.num_keys or [])
    boo = set(v8.bool_keys or [])

    # --- DQ (strict data quality trackers) ---
    for k in ("tick_gap_p95_ms", "tick_missing_seq_ema", "book_missing_seq_ema"):
        assert k in num, f"missing DQ numeric key: {k}"

    # --- LiqMap (fixed minimal window=1h) ---
    # Prefer v1 naming (from core/liqmap_features_v1.py), but we also allow doc-style aliases.
    required_liqmap = [
        "liqmap_1h_total_usd",
        "liqmap_1h_near_total_usd",
        "liqmap_1h_near_imb",        "liqmap_1h_dist_dn_bps",
        "liqmap_1h_peak_up1_usd",
        "liqmap_1h_peak_dn1_usd",
        "liqmap_1h_age_ms",
    ]
    for k in required_liqmap:
        assert k in num, f"missing liqmap(1h) numeric key: {k}"

    # Optional doc-style aliases (should be present if schema included them).
    # If later removed intentionally, adjust this list, but keep at least v1 naming above.
    for k in (    ):
        assert k in num, f"missing liqmap alias key (kept for migration safety): {k}"

    # --- Levels overlay (optional runtime feature; safe to keep in schema) ---
    assert "liqmap_tp1_adj_bps" in num
    assert "liqmap_sl_adj_bps" in num
    assert "liqmap_levels_applied" in boo
