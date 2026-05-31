"""Integration tests for MLFeatureSchemaV14OF (merged v13_of + v5_of)."""

import pytest

def test_v14_of_schema_exists():
    """v14_of schema module can be imported."""
    from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys, v14_of_info
    keys = get_v14_of_numeric_keys()
    info = v14_of_info()
    assert isinstance(keys, list)
    assert len(keys) > 242
    assert info["ver"] == "v14_of"

def test_v14_of_includes_v13_base():
    """Live v13_of keys (minus dead key prune) should be in v14_of."""
    from core.ml_feature_schema_v13_of import get_v13_of_numeric_keys
    from core.ml_feature_schema_v14_of import _V14_DEAD_KEYS, get_v14_of_numeric_keys
    live_v13 = set(get_v13_of_numeric_keys()) - _V14_DEAD_KEYS
    v14_keys = set(get_v14_of_numeric_keys())
    assert live_v13.issubset(v14_keys)

def test_v14_of_execution_tca_features():
    """Plan 4.2: Execution/TCA features present."""
    from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys
    keys = set(get_v14_of_numeric_keys())
    required = ["exec_cost_to_tp1_ratio", "tca_eff_spread_bps_ema", "spread_p95_bps_symbol_kind_session"]
    for key in required:
        assert key in keys, f"Missing: {key}"

def test_v14_of_queue_fill_features():
    """Plan 4.3: Queue/fill probability features present."""
    from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys
    keys = set(get_v14_of_numeric_keys())
    required = ["fill_prob_proxy", "eta_fill_sec", "fill_prob_1s", "queue_ahead_qty_l1"]
    for key in required:
        assert key in keys, f"Missing: {key}"

def test_v14_of_lob_dynamics_features():
    """Plan 4.4: LOB velocity features present."""
    from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys
    keys = set(get_v14_of_numeric_keys())
    required = ["obi_slope_1s", "qimb_slope_1s", "spread_widen_velocity_bps_s"]
    for key in required:
        assert key in keys, f"Missing: {key}"

def test_v14_of_cross_symbol_features():
    """Plan 4.6: Cross-symbol leadership features present."""
    from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys
    keys = set(get_v14_of_numeric_keys())
    required = ["btc_ret_1m", "eth_ret_1m", "rel_ret_1m_vs_btc", "leader_confidence"]
    for key in required:
        assert key in keys, f"Missing: {key}"

def test_v14_of_signal_age_features():
    """Plan 4.8: Signal age and horizon awareness features present."""
    from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys
    keys = set(get_v14_of_numeric_keys())
    required = ["signal_age_ms", "signal_age_to_half_life", "atr_age_ms"]
    for key in required:
        assert key in keys, f"Missing: {key}"

def test_v14_of_dq_features():
    """Plan 4.9: Data quality features present."""
    from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys
    keys = set(get_v14_of_numeric_keys())
    required = ["dq_score", "dq_flag_count", "tick_lag_ms", "book_age_ms"]
    for key in required:
        assert key in keys, f"Missing: {key}"

def test_v14_of_no_collisions():
    """No duplicate keys."""
    from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys
    keys = get_v14_of_numeric_keys()
    unique_keys = set(keys)
    assert len(keys) == len(unique_keys)

def test_v14_of_deterministic():
    """Schema key order is stable."""
    from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys
    keys1 = get_v14_of_numeric_keys()
    keys2 = get_v14_of_numeric_keys()
    assert keys1 == keys2

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
