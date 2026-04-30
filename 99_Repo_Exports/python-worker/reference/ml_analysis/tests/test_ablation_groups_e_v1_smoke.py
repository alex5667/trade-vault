from ml_analysis.common.feature_groups_e_v1 import build_e_groups, group_features


def test_group_features_e_block_smoke() -> None:
    groups = build_e_groups()
    feats = [
        "n:hawkes_taker_buy_lam"
        "n:hawkes_cancel_bid_lam"
        "n:vpin_tox_z"
        "n:limit_add_total_rate_ema"
        "n:lambda_trade_buy"
        "n:some_other_feature"
    ]
    m = group_features(feats, groups)
    assert "E_vpin" in m and "vpin_tox_z" in m["E_vpin"]
    assert "E_hawkes_split" in m and "hawkes_taker_buy_lam" in m["E_hawkes_split"]
    assert "E_limit_add" in m and "limit_add_total_rate_ema" in m["E_limit_add"]
    assert "E_lambda_alias" in m and "lambda_trade_buy" in m["E_lambda_alias"]
