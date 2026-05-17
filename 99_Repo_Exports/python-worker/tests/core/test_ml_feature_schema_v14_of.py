from __future__ import annotations

"""
Smoke + contract tests for ml_feature_schema_v14_of.

Scope:
  - schema imports cleanly
  - key count within sanity range
  - no collision between og_*/oe_*/xv_*/xvi_*/xvii_* groups and v13_of base
  - registry resolves v14_of and aliases (v14, "v14_of")
  - v14 keys = v13 keys ∪ all new groups (no removal, append-only)
  - registry FeatureSchemaInfo names contain all new columns
  Group OG   (16): og_* — rule-gate consensus indicators
  Group OE   (61): external data (composites + breadth + deribit + PIT rolling
                    + macro + fng_delta + prior_stale bool-as-float)
  Group XV    (4): cross-venue sanity (OKX/Kraken/Coinbase)
  Group XVI   (7): CoinGecko macro context (global + per-symbol)
  Group XVII  (3): Deribit extended (options OI + per-symbol perp basis)
  Group XVIII (9): DefiLlama slow-regime (stablecoins + TVL + DEX + fees + perps)
  Group XIX   (4): CoinPaprika fallback
  Group XX    (4): CoinMarketCap fallback
  Group XXI   (6): Bybit cross-venue
  Total new = 114 keys
"""

import pytest


def test_v14_schema_imports():
    from core.ml_feature_schema_v14_of import (
        V14_OF_NUMERIC_KEYS,
        SCHEMA_HASH,
        get_v14_of_numeric_keys,
        v14_of_info,
    )
    assert isinstance(V14_OF_NUMERIC_KEYS, list)
    assert len(V14_OF_NUMERIC_KEYS) > 0
    assert isinstance(SCHEMA_HASH, str) and len(SCHEMA_HASH) > 0
    assert get_v14_of_numeric_keys() == list(V14_OF_NUMERIC_KEYS)

    info = v14_of_info()
    assert info["ver"] == "v14_of"
    assert info["schema_hash"] == SCHEMA_HASH
    assert info["n_new_keys"] == 114  # 16+61+4+7+3+9+4+4+6
    assert info["groups"]["group_og_rule_consensus"] == 16
    assert info["groups"]["group_oe_external_data"] == 61
    assert info["groups"]["group_xv_cross_venue"] == 4
    assert info["groups"]["group_xvi_coingecko_macro"] == 7
    assert info["groups"]["group_xvii_deribit_ext"] == 3
    assert info["groups"]["group_xviii_defillama"] == 9
    assert info["groups"]["group_xix_coinpaprika"] == 4
    assert info["groups"]["group_xx_cmc"] == 4
    assert info["groups"]["group_xxi_bybit"] == 6


def test_v14_extends_v13_strictly_append_only():
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS

    v13 = set(V13_OF_NUMERIC_KEYS)
    v14 = set(V14_OF_NUMERIC_KEYS)

    removed = v13 - v14
    assert not removed, f"v14_of removed keys (append-only violation): {sorted(removed)}"

    added = v14 - v13
    assert len(added) == 114, (
        f"expected 114 new keys (16+61+4+7+3+9+4+4+6), "
        f"got {len(added)}: {sorted(added)}"
    )
    og_added = {k for k in added if k.startswith("og_")}
    assert len(og_added) == 16, f"expected 16 og_* keys, got {len(og_added)}"


def test_v14_og_no_collision_with_v13():
    """Hard assert at module import; this just exercises it."""
    from core.ml_feature_schema_v14_of import _GROUP_OG_RULE_CONSENSUS, _V13_OF_BASE
    assert not (set(_GROUP_OG_RULE_CONSENSUS) & set(_V13_OF_BASE))


def test_v14_keys_sorted_deterministic():
    """V14_OF_NUMERIC_KEYS must be sorted (deterministic feature_cols order)."""
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    assert list(V14_OF_NUMERIC_KEYS) == sorted(V14_OF_NUMERIC_KEYS)


def test_v14_key_count_within_sanity_bounds():
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    n = len(V14_OF_NUMERIC_KEYS)
    assert 245 <= n <= 380, f"v14_of key count {n} outside sanity bounds [245, 380]"


def test_registry_resolves_v14_of():
    from core.feature_registry import get_schema_info
    info = get_schema_info("v14_of")
    assert info is not None
    names = getattr(info, "names", None) or getattr(info, "feature_names", None)
    assert names, "FeatureSchemaInfo must expose names/feature_names"
    og_keys = [n for n in names if n.startswith("n:og_")]
    assert len(og_keys) == 16, f"expected 16 n:og_* entries, got {len(og_keys)}: {og_keys}"
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    v13_with_prefix = {f"n:{k}" for k in V13_OF_NUMERIC_KEYS}
    new_in_registry = [n for n in names if n.startswith("n:") and n not in v13_with_prefix]
    assert len(new_in_registry) == 114, (
        f"expected 114 new n:* entries (all groups), got {len(new_in_registry)}"
    )


def test_registry_v14_alias_normalization():
    """Both 'v14' and 'v14_of' must yield equivalent feature_cols."""
    from core.feature_registry import get_schema_info
    a = get_schema_info("v14_of")
    b = get_schema_info("v14")
    a_names = getattr(a, "names", None) or getattr(a, "feature_names", None)
    b_names = getattr(b, "names", None) or getattr(b, "feature_names", None)
    assert a_names is not None and b_names is not None
    assert list(a_names) == list(b_names)


def test_edge_stack_spec_accepts_v14():
    from core.feature_registry import get_edge_stack_feature_spec
    spec = get_edge_stack_feature_spec("v14_of")
    cols = getattr(spec, "feature_cols", None) or getattr(spec, "cols", None)
    assert cols, "EdgeStackFeatureSpec must expose feature_cols"
    og_cols = [c for c in cols if c.startswith("f_og_")]
    assert len(og_cols) == 16, (
        f"v14_of edge-stack must include all 16 f_og_* cols; got {len(og_cols)}: {og_cols}"
    )
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    v13_f = {f"f_{k}" for k in V13_OF_NUMERIC_KEYS}
    new_cols = [c for c in cols if c.startswith("f_") and c not in v13_f]
    assert len(new_cols) == 114, (
        f"v14_of edge-stack must include all 114 new f_* cols; got {len(new_cols)}"
    )


def test_edge_stack_spec_rejects_v15_not_yet_defined():
    from core.feature_registry import get_edge_stack_feature_spec
    with pytest.raises(ValueError):
        get_edge_stack_feature_spec("v15_of")
