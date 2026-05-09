from core.ml_feature_schema_v4 import MLFeatureSchemaV4

SCHEMA_HASH = "3de5c11b0fbb"


def test_ml_feature_schema_v4_vectorize():
    schema = MLFeatureSchemaV4()

    # Check feature names list
    feats = schema.feature_names()
    assert "rsi_agree" in schema.bool_keys, "Schema should have rsi_agree"
    assert "sweep_eqh" in schema.bool_keys, "Schema should have sweep_eqh"
    assert "b:rsi_agree" in feats, "Feature list should include b:rsi_agree"
    assert "b:sweep_eql" in feats, "Feature list should include b:sweep_eql"

    # Test vectorization
    row = {
        "indicators": {
            "rsi_agree": 1,
            "sweep_eqh": 0,
            "sweep_eql": 1,
            "div_match": 1,
            "ofi_z": 1.5,
            "direction": "LONG"
        }
    }

    vectorized = schema.vectorize(row)

    # Verify vectorization length matches feature names
    assert len(vectorized) == len(feats)
