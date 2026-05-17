from __future__ import annotations

"""
Smoke + contract tests for ml_feature_schema_v13_of.

v13_of is the production champion schema (ML_FEATURE_SCHEMA_VER=v13_of in
docker-compose-crypto-orderflow.yml, used by the main signal pipelines). Any
accidental edit to the key list, group counts, or SCHEMA_HASH must fail CI —
silently shifting v13_of would invalidate the trained champion model.

Scope:
  - schema imports cleanly
  - SCHEMA_HASH frozen at the production-pinned value
  - key count within sanity range
  - v13 keys = v12 keys ∪ all new NA/NB/NC/ND/NE/NF/NX groups (append-only)
  - registry resolves v13_of and the "v13" alias
  - edge-stack feature spec accepts v13_of
  Group NA (4): garman_klass/parkinson/yang_zhang/vol_of_vol — OHLC-based vol
  Group NB (4): amihud/corwin_schultz/hasbrouck/depth_resilience — liquidity
  Group NC (4): pin/lambda_asym/toxicity_regime/aggressive_sweep — toxicity
  Group ND (5): btc_dominance_momentum/oi_weighted_funding/... — cross-asset
  Group NE (3): price_entropy_50/order_size_gini/mutual_info — information
  Group NF (3): half_life/adf_pvalue_50/zscore_mid_to_vwap — mean reversion
  Group NX (5): interactions (vpin_x_funding, hurst_x_vol_regime, ...)
  Total new = 28 keys vs v12_of base (214) → v13_of total = 242 keys
"""

# Frozen production constants.
# Bumping these requires retraining the v13_of champion model — never edit
# casually. Bumping SCHEMA_HASH while keeping the same key list is also wrong;
# the hash is intentionally tied to the pinned snapshot.
_V13_FROZEN_SCHEMA_HASH = "7838afd8be98"
_V13_FROZEN_NUMERIC_COUNT = 242
_V13_FROZEN_NEW_KEYS = 28
_V13_FROZEN_V12_BASE = 214


def test_v13_schema_imports():
    from core.ml_feature_schema_v13_of import (
        V13_OF_NUMERIC_KEYS,
        SCHEMA_HASH,
        get_v13_of_numeric_keys,
    )
    assert isinstance(V13_OF_NUMERIC_KEYS, list)
    assert len(V13_OF_NUMERIC_KEYS) > 0
    assert isinstance(SCHEMA_HASH, str) and len(SCHEMA_HASH) > 0
    assert get_v13_of_numeric_keys() == list(V13_OF_NUMERIC_KEYS)


def test_v13_schema_hash_frozen():
    """SCHEMA_HASH is the prod-pinned identifier of the champion model's
    feature space. Any change here means the live model must be retrained."""
    from core.ml_feature_schema_v13_of import SCHEMA_HASH
    assert SCHEMA_HASH == _V13_FROZEN_SCHEMA_HASH, (
        f"v13_of SCHEMA_HASH drift: {SCHEMA_HASH!r} != frozen "
        f"{_V13_FROZEN_SCHEMA_HASH!r}. If this is intentional, bump the "
        f"frozen constant AND retrain the champion model."
    )


def test_v13_info_groups_frozen():
    from core.ml_feature_schema_v13_of import v13_of_info
    info = v13_of_info()
    assert info["ver"] == "v13_of"
    assert info["n_numeric_keys"] == _V13_FROZEN_NUMERIC_COUNT
    assert info["n_new_keys"] == _V13_FROZEN_NEW_KEYS
    assert info["n_v12_of_base"] == _V13_FROZEN_V12_BASE
    assert info["groups"]["group_na_volatility"] == 4
    assert info["groups"]["group_nb_liquidity"] == 4
    assert info["groups"]["group_nc_toxicity"] == 4
    assert info["groups"]["group_nd_cross_asset"] == 5
    assert info["groups"]["group_ne_entropy"] == 3
    assert info["groups"]["group_nf_mean_reversion"] == 3
    assert info["groups"]["group_nx_interactions"] == 5
    assert sum(info["groups"].values()) == _V13_FROZEN_NEW_KEYS


def test_v13_extends_v12_strictly_append_only():
    from core.ml_feature_schema_v12_of import V12_OF_NUMERIC_KEYS
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS

    v12 = set(V12_OF_NUMERIC_KEYS)
    v13 = set(V13_OF_NUMERIC_KEYS)

    removed = v12 - v13
    assert not removed, (
        f"v13_of removed keys from v12_of base (append-only violation): "
        f"{sorted(removed)}"
    )

    added = v13 - v12
    assert len(added) == _V13_FROZEN_NEW_KEYS, (
        f"expected {_V13_FROZEN_NEW_KEYS} new keys (4+4+4+5+3+3+5), "
        f"got {len(added)}: {sorted(added)}"
    )


def test_v13_new_group_keys_present():
    """Each declared group's identifying keys must land in V13_OF_NUMERIC_KEYS."""
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS

    keys = set(V13_OF_NUMERIC_KEYS)
    # Group NA
    assert {"garman_klass_vol", "parkinson_vol", "yang_zhang_vol", "vol_of_vol"} <= keys
    # Group NB
    assert {
        "amihud_illiquidity", "corwin_schultz_spread",
        "hasbrouck_info_share", "depth_resilience_half_life",
    } <= keys
    # Group NC
    assert {
        "pin_estimate", "lambda_asym",
        "toxicity_regime_score", "aggressive_sweep_ratio",
    } <= keys
    # Group ND
    assert {
        "btc_dominance_momentum", "oi_weighted_funding",
        "total_market_oi_delta", "liq_heatmap_distance_bps", "long_short_ratio",
    } <= keys
    # Group NE
    assert {"price_entropy_50", "order_size_gini", "mutual_info_price_volume"} <= keys
    # Group NF
    assert {"half_life_mean_reversion", "adf_pvalue_50", "zscore_mid_to_vwap"} <= keys
    # Group NX
    assert {
        "vpin_x_funding", "hurst_x_vol_regime",
        "entropy_x_spread", "depth_resil_x_sweep", "amihud_x_oi_delta",
    } <= keys


def test_v13_keys_sorted_deterministic():
    """V13_OF_NUMERIC_KEYS must be sorted (deterministic feature_cols order)."""
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    assert list(V13_OF_NUMERIC_KEYS) == sorted(V13_OF_NUMERIC_KEYS)


def test_v13_no_duplicate_keys():
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    assert len(V13_OF_NUMERIC_KEYS) == len(set(V13_OF_NUMERIC_KEYS))


def test_v13_key_count_within_sanity_bounds():
    """Mirrors the module-level guard inside ml_feature_schema_v13_of."""
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    n = len(V13_OF_NUMERIC_KEYS)
    assert 230 <= n <= 260, (
        f"v13_of key count {n} outside sanity bounds [230, 260]"
    )


def test_registry_resolves_v13_of():
    from core.feature_registry import get_schema_info
    info = get_schema_info("v13_of")
    assert info is not None
    names = getattr(info, "names", None) or getattr(info, "feature_names", None)
    assert names, "FeatureSchemaInfo must expose names/feature_names"
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    v13_with_prefix = {f"n:{k}" for k in V13_OF_NUMERIC_KEYS}
    registry_numeric = {n for n in names if n.startswith("n:")}
    missing = v13_with_prefix - registry_numeric
    assert not missing, (
        f"FeatureSchemaInfo for v13_of missing n:* entries: {sorted(missing)[:10]}"
    )


def test_registry_v13_alias_normalization():
    """Both 'v13' and 'v13_of' must yield equivalent feature_cols."""
    from core.feature_registry import get_schema_info
    a = get_schema_info("v13_of")
    b = get_schema_info("v13")
    a_names = getattr(a, "names", None) or getattr(a, "feature_names", None)
    b_names = getattr(b, "names", None) or getattr(b, "feature_names", None)
    assert a_names is not None and b_names is not None
    assert list(a_names) == list(b_names)


def test_edge_stack_spec_accepts_v13():
    from core.feature_registry import get_edge_stack_feature_spec
    spec = get_edge_stack_feature_spec("v13_of")
    cols = getattr(spec, "feature_cols", None) or getattr(spec, "cols", None)
    assert cols, "EdgeStackFeatureSpec must expose feature_cols"
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    v13_f = {f"f_{k}" for k in V13_OF_NUMERIC_KEYS}
    missing = v13_f - set(cols)
    assert not missing, (
        f"v13_of edge-stack spec missing f_* cols: {sorted(missing)[:10]}"
    )


def test_v13_not_in_deprecated_set():
    """Production champion must never be flagged DEPRECATED."""
    from core.feature_registry import DEPRECATED_SCHEMAS
    assert "v13_of" not in {str(x) for x in DEPRECATED_SCHEMAS}
    assert "v13" not in {str(x) for x in DEPRECATED_SCHEMAS}
