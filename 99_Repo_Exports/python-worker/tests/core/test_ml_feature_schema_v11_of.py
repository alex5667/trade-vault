
import pytest

from core.feature_registry import _EDGE_CACHE, _SCHEMA_CACHE, get_edge_stack_feature_spec, get_schema_info
from core.ml_feature_schema_v11_of import V11_OF_NUMERIC_KEYS

SCHEMA_HASH = "aa0a390ad3fb"


def test_v11_of_schema_smoke():
    """v11_of must have >185 numeric keys, schema resolves, hashes stable."""
    info = get_schema_info("v11_of")
    assert info.ver == "v11_of"
    assert isinstance(info.feature_names, list)

    # purely numeric keys (193)
    assert len(info.feature_names) >= 190, (
        f"v11_of only has {len(info.feature_names)} feature_names, expected >= 190"
    )
    # Must be unique and stable
    assert len(set(info.feature_names)) == len(info.feature_names)
    assert len(info.schema_hash) == 64

    # ensure new keys propagate
    assert "n:hurst_exp_50" in info.feature_names


def test_v11_of_edge_stack_feature_cols():
    """v11_of EdgeStackFeatureSpec has >240 cols, correct f_* keys."""
    spec = get_edge_stack_feature_spec("v11_of")
    assert spec.ver == "v11_of"
    assert len(spec.feature_cols) >= 230, (
        f"v11_of only has {len(spec.feature_cols)} feature_cols, expected >= 230"
    )
    assert len(spec.feature_cols_hash) == 16

    # session one-hots
    for sess in ("session_asia", "session_eu", "session_us", "session_off"):
        assert sess in spec.feature_cols, f"v11_of missing {sess}"

    # spot-check v11 keys
    new_keys = [
        "f_hurst_exp_50", "f_vol_regime_code", "f_trade_size_skew",
        "f_kyle_lambda", "f_conf_ma_ratio", "f_kyle_x_vpin"
    ]
    for key in new_keys:
        assert key in spec.feature_cols, f"v11_of edge-spec missing expected key: {key}"


def test_v11_of_alias_resolves():
    """'v11' must resolve to 'v11_of' with identical schema_hash."""
    full = get_schema_info("v11_of")
    alias = get_schema_info("v11")
    assert alias.ver == "v11_of"
    assert alias.schema_hash == full.schema_hash


def test_v11_of_hash_stable():
    """v11_of schema_hash and feature_cols_hash are stable across repeated calls."""
    _SCHEMA_CACHE.clear()
    h1 = get_schema_info("v11_of").schema_hash
    _SCHEMA_CACHE.clear()
    h2 = get_schema_info("v11_of").schema_hash
    assert h1 == h2

    _EDGE_CACHE.clear()
    c1 = get_edge_stack_feature_spec("v11_of").feature_cols_hash
    _EDGE_CACHE.clear()
    c2 = get_edge_stack_feature_spec("v11_of").feature_cols_hash
    assert c1 == c2


def test_v11_of_is_superset_of_v10_of():
    """All v10_of keys must be present in v11_of."""
    try:
        from core.ml_feature_schema_v10_of import V10_OF_NUMERIC_KEYS
    except ImportError:
        pytest.skip("v10_of schema not found")

    v11_set = set(V11_OF_NUMERIC_KEYS)
    for k in V10_OF_NUMERIC_KEYS:
        assert k in v11_set, f"v10_of key {k} missing from v11_of"

# -----------------
# Feature Computers
# -----------------

def test_group_a_computers():
    from core.v11_of_computers.regime_computers import (
        compute_hurst_exp_50,
        compute_roll_spread_est,
        compute_vol_regime_code,
    )

    # Hurst
    assert compute_hurst_exp_50([]) == 0.5
    # Vol regime
    assert compute_vol_regime_code(0.0, 0.0) == 0.0
    assert compute_vol_regime_code(5.0, 0.0) == 1.0  # normal for typical crypto

    # Roll spread (undefined if cov > 0)
    prices = [100.0, 101.0, 100.0, 101.0, 100.0] * 10
    s_bps = compute_roll_spread_est(prices)
    assert s_bps > 0


def test_group_b_computers():
    from core.v11_of_computers.session_computers import compute_kelly_fraction_roll, compute_profit_factor_roll20

    trades = [
        {"pnl_ratio": 0.02}, {"pnl_ratio": 0.01}, {"pnl_ratio": -0.01}
    ]
    k = compute_kelly_fraction_roll(trades)
    assert 0.0 < k < 1.0

    pf = compute_profit_factor_roll20(trades)
    assert pf == 3.0  # 0.03 / 0.01


def test_group_d_computers():
    from core.v11_of_computers.microstructure_computers import compute_large_trade_ratio, compute_sweep_velocity_bps_s

    sizes = [1.0, 1.0, 1.0, 1.0, 100.0] * 10
    ltr = compute_large_trade_ratio(sizes, threshold_mult=3.0)
    assert ltr > 0.0

    vel = compute_sweep_velocity_bps_s(1000.0, 2000.0, 10.0)
    assert vel == 10.0  # 10 bps / 1 sec


def test_group_f_computers():
    from core.v11_of_computers.interaction_computers import (
        compute_kyle_x_vpin,
        compute_liq_score_x_spread,
        compute_momentum_x_vol_ratio,
    )

    assert compute_kyle_x_vpin(2.5, 0.8) == 2.0
    assert compute_momentum_x_vol_ratio(-15.0, 2.0) == -30.0
    assert compute_liq_score_x_spread(0.9, 5.0) == 4.5
