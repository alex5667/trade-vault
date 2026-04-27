from core.ml_feature_schema_v3 import MLFeatureSchemaV3

def test_schema_v3_len():
    s = MLFeatureSchemaV3()
    x = s.vectorize(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"delta_z": 2.0, "spread_bps": 4.0, "ofi_stable": 1, "mae_r": 0.4, "mfe_r": 1.2},
        rule_score=0.7,
        rule_have=2,
        rule_need=3,
        cancel_spike_veto=0,
    )
    assert len(x) == len(s.feature_names())

