from core.meta_features_v6 import (
    META_FEAT_V6_COLS,
    META_FEAT_V6_NAME,
    META_FEAT_V6_NEW_COLS,
    META_FEAT_V6_VERSION,
    build_meta_features_v6,
)


def test_v6_schema_basics():
    assert META_FEAT_V6_NAME == "meta_feat_v6"
    assert META_FEAT_V6_VERSION == 6
    assert len(META_FEAT_V6_COLS) > len(META_FEAT_V6_NEW_COLS)
    assert "exec_risk_ref_bps" in META_FEAT_V6_COLS
    assert "hawkes_taker_lam" in META_FEAT_V6_COLS

def test_v6_builder_smoke():
    evidence = {
        "exec_risk_ref_bps": 12.5,
        "exec_pen": 0.5,
    }
    indicators = {
        "book_staleness_ms": 100,
        "taker_buy_rate_ema": 1.2,
    }

    feat, missing = build_meta_features_v6(
        evidence=evidence,
        indicators=indicators,
        have=3,
        need=4,
    )

    # Check new cols
    assert feat["exec_risk_ref_bps"] == 12.5
    assert feat["exec_pen"] == 0.5
    assert feat["book_staleness_ms"] == 100
    assert feat["taker_buy_rate_ema"] == 1.2
    assert feat["have_need_ratio"] == 0.75  # 3/4

    # Check inherited cols (e.g. from v5 or earlier)
    # v5 contains things like 'obi', 'ofi_z', etc.
    # We didn't provide them, so they should be 0.0 or missing.
    assert "obi_z" in feat

def test_v6_missing_features():
    feat, missing = build_meta_features_v6(
        evidence={},
        indicators={},
        have=0,
        need=0,
    )
    assert "hawkes_cancel_lam" in missing
    assert feat["hawkes_cancel_lam"] == 0.0
