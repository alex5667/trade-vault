from __future__ import annotations

from core.ml_feature_schema_v2 import MLFeatureSchemaV2


def test_schema_vector_len():
    """Test that vectorize returns the same length as feature_names."""
    s = MLFeatureSchemaV2()
    x = s.vectorize(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"delta_z": 2.0, "spread_bps": 4.0, "ofi_stable": 1},
        rule_score=0.7,
        rule_have=2,
        rule_need=3,
        cancel_spike_veto=0,
    )
    assert len(x) == len(s.feature_names())


def test_schema_vectorize_row():
    """Test vectorize_row with dict input."""
    s = MLFeatureSchemaV2()
    row = {
        "symbol": "ETHUSDT",
        "ts_ms": 1700000001000,
        "direction": "SHORT",
        "scenario_v4": "trend_continuation",
        "indicators": {
            "delta_z": -1.5,
            "ofi_z": 0.8,
            "spread_bps": 2.0,
            "obi_stable": True,
        },
        "rule_score": 0.85,
        "rule_have": 3,
        "rule_need": 3,
        "cancel_spike_veto": 0,
    }
    x = s.vectorize_row(row)
    assert len(x) == len(s.feature_names())
    assert all(isinstance(v, (int, float)) for v in x)


def test_schema_defaults():
    """Test that missing values default to 0."""
    s = MLFeatureSchemaV2()
    x = s.vectorize(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="unknown",
        indicators={},
        rule_score=0.0,
        rule_have=0,
        rule_need=0,
        cancel_spike_veto=0,
    )
    assert len(x) == len(s.feature_names())
    # All numeric features should be 0 or 1 (for one-hot)
    assert all(v >= 0.0 for v in x)
    assert all(v <= 1.0 for v in x)


def test_schema_bucket_onehot():
    """Test bucket one-hot encoding."""
    s = MLFeatureSchemaV2()
    
    # Test trend bucket
    x_trend = s.vectorize(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="trend_continuation",
        indicators={},
        rule_score=0.0,
        rule_have=0,
        rule_need=0,
        cancel_spike_veto=0,
    )
    names = s.feature_names()
    trend_idx = names.index("bucket:trend")
    range_idx = names.index("bucket:range")
    other_idx = names.index("bucket:other")
    assert x_trend[trend_idx] == 1.0
    assert x_trend[range_idx] == 0.0
    assert x_trend[other_idx] == 0.0
    
    # Test range bucket
    x_range = s.vectorize(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={},
        rule_score=0.0,
        rule_have=0,
        rule_need=0,
        cancel_spike_veto=0,
    )
    assert x_range[trend_idx] == 0.0
    assert x_range[range_idx] == 1.0
    assert x_range[other_idx] == 0.0


def test_schema_direction_onehot():
    """Test direction one-hot encoding."""
    s = MLFeatureSchemaV2()
    names = s.feature_names()
    long_idx = names.index("dir:LONG")
    short_idx = names.index("dir:SHORT")
    
    x_long = s.vectorize(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="trend",
        indicators={},
        rule_score=0.0,
        rule_have=0,
        rule_need=0,
        cancel_spike_veto=0,
    )
    assert x_long[long_idx] == 1.0
    assert x_long[short_idx] == 0.0
    
    x_short = s.vectorize(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="SHORT",
        scenario="trend",
        indicators={},
        rule_score=0.0,
        rule_have=0,
        rule_need=0,
        cancel_spike_veto=0,
    )
    assert x_short[long_idx] == 0.0
    assert x_short[short_idx] == 1.0
