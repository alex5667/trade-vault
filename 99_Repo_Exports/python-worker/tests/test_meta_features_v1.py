import pytest
from core.meta_features_v1 import build_meta_features_v1, META_FEAT_V1_COLS, META_FEAT_V1_NAME, META_FEAT_V1_VERSION

def test_build_meta_features_v1_all_present():
    evidence = {
        "meta_context": {
            "age_ms": 100,
            "is_weekend": 0,
            "is_eu_hours": 1,
            "is_us_hours": 0,
            "is_asia_hours": 0,
        }
    }
    indicators = {
        "lag1_pnl_15m": 10.0,
        "lag1_pnl_1h": 20.0,
        "lag1_pnl_4h": 30.0,
        "lag1_pnl_24h": 40.0,
        "lag1_win_15m": 0.5,
        "lag1_win_1h": 0.6,
        "lag1_win_4h": 0.7,
        "lag1_win_24h": 0.8,
        "lag2_pnl_15m": 11.0,
        "lag2_pnl_1h": 21.0,
        "lag2_pnl_4h": 31.0,
        "lag2_pnl_24h": 41.0,
        "lag2_win_15m": 0.51,
        "lag2_win_1h": 0.61,
        "lag2_win_4h": 0.71,
        "lag2_win_24h": 0.81,
        "spread_bps": 2.5,
        "volatility_15m_bps": 5.0,
        "volatility_1h_bps": 10.0,
        "volatility_4h_bps": 15.0,
        "volatility_24h_bps": 20.0,
        "ofi_15m": 1.0,
        "ofi_1h": 2.0,
        "ofi_4h": 3.0,
        "ofi_24h": 4.0,
        "book_churn_15m": 0.1,
        "book_churn_1h": 0.2,
        "book_churn_4h": 0.3,
        "book_churn_24h": 0.4,
        "liq_imbal_15m": -0.1,
        "liq_imbal_1h": -0.2,
        "liq_imbal_4h": -0.3,
        "liq_imbal_24h": -0.4,
        "trade_imbal_15m": 0.05,
        "trade_imbal_1h": 0.06,
        "trade_imbal_4h": 0.07,
        "trade_imbal_24h": 0.08,
    }

    feat, missing = build_meta_features_v1(evidence, indicators)
    assert len(missing) == 0
    assert len(feat) == len(META_FEAT_V1_COLS)
    assert feat["age_ms"] == 100.0
    assert feat["lag1_pnl_15m"] == 10.0

def test_build_meta_features_v1_missing_critical():
    evidence = {}
    indicators = {}
    feat, missing = build_meta_features_v1(evidence, indicators)
    
    assert "age_ms" in missing
    assert feat["age_ms"] == 0.0
    
    # Check all columns present in output even if missing in input
    for col in META_FEAT_V1_COLS:
        assert col in feat

def test_build_meta_features_v1_nan_handling():
    evidence = {"meta_context": {"age_ms": float("nan")}}
    indicators = {"spread_bps": float("inf")}
    
    feat, missing = build_meta_features_v1(evidence, indicators)
    
    assert "age_ms" in missing
    assert feat["age_ms"] == 0.0
    
    assert "spread_bps" in missing
    assert feat["spread_bps"] == 0.0

def test_constants():
    assert META_FEAT_V1_NAME == "meta_feat_v1"
    assert META_FEAT_V1_VERSION == 1
    assert len(META_FEAT_V1_COLS) > 0
