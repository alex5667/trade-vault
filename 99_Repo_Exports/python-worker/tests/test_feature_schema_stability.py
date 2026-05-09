
from core.ml_feature_schema import build_feature_vector, feature_names


def test_feature_order_stable():
    names = feature_names()
    assert len(names) > 10
    assert names[0] == "dir_long"
    assert "exec_risk_norm" in names

def test_build_vector_length_matches():
    indicators = {"delta_z": 2.0, "exec_risk_norm": 0.5, "sweep_recent": 1}
    vec, miss = build_feature_vector(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="reversal",
        indicators=indicators,
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
    )
    assert len(vec) == len(feature_names())

