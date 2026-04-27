from __future__ import annotations


def test_bucket2_derive_high_var_from_scenario():
    from core.bucket2_v1 import derive_bucket2_label

    assert derive_bucket2_label("vol_shock_news_proxy") == "high_var"
    assert derive_bucket2_label("VOL_SHOCK_NEWS_PROXY|v4") == "high_var"


def test_bucket2_derive_high_var_from_exec_bucket():
    from core.bucket2_v1 import derive_bucket2_label

    assert derive_bucket2_label("na", indicators={"exec_regime_bucket": "HIGH_VAR"}) == "high_var"
    assert derive_bucket2_label("na", indicators={"exec_regime_bucket": "EXTREME"}) == "high_var"


def test_bucket2_derive_reversal_from_evidence():
    from core.bucket2_v1 import derive_bucket2_label

    assert derive_bucket2_label("na", evidence={"sweep": 1}) == "reversal"
    assert derive_bucket2_label("na", evidence={"reclaim": 1}) == "reversal"


def test_bucket2_derive_breakout_from_range_expansion_flag():
    from core.bucket2_v1 import derive_bucket2_label

    assert derive_bucket2_label("na", indicators={"fp_edge_range_expansion": 1}) == "breakout"
    assert derive_bucket2_label("breakout") == "breakout"


def test_feature_registry_bucket2_is_opt_in():
    from core.feature_registry import get_edge_stack_feature_spec

    spec0 = get_edge_stack_feature_spec("v7_of")
    assert not any(str(c).startswith("bucket2:") for c in spec0.feature_cols)

    spec1 = get_edge_stack_feature_spec("v7_of", include_bucket2_onehot=True)
    assert "bucket2:breakout" in spec1.feature_cols
    assert "bucket2:reversal" in spec1.feature_cols
    assert "bucket2:high_var" in spec1.feature_cols
