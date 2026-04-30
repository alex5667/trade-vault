from core.meta_features_v9 import META_FEAT_V9_COLS, build_meta_features_v9


def test_meta_features_v9_has_stable_keyset():
    feats, missing = build_meta_features_v9({}, {})
    assert set(feats.keys()) == set(META_FEAT_V9_COLS)
    # Missing can be non-empty; this test only enforces stable schema.
    assert isinstance(missing, list)


def test_meta_features_v9_liqmap_keys_present():
    """Liqmap feature keys must be in the schema."""
    from core.liqmap_features_v1 import liqmap_feature_keys
    for w in ("5m", "1h"):
        for k in liqmap_feature_keys(w):
            assert k in META_FEAT_V9_COLS, f"Missing liqmap key: {k}"


def test_meta_features_v9_gate_keys_present():
    """Gate scalar keys must be in the schema."""
    gate_keys = [
        "liqmap_gate_shadow_veto"
        "liqmap_gate_veto"
        "liqmap_gate_rr"
        "liqmap_gate_risk_bps"
        "liqmap_gate_reward_bps"
    ]
    for k in gate_keys:
        assert k in META_FEAT_V9_COLS, f"Missing gate key: {k}"
