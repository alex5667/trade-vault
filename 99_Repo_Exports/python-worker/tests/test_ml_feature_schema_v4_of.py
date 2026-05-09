import math

from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF

SCHEMA_HASH = "4ed09b43d54f"


def test_ml_feature_schema_v4_of_vectorize():
    schema = MLFeatureSchemaV4OF()

    indicators = {
        "delta_z": 2.5,
        "ofi_stable": 1,
        "conf_sweep": True
    }

    x = schema.vectorize(
        ts_ms=1600000000000,
        direction="LONG",
        scenario="trend",
        indicators=indicators,
        cancel_spike_veto=False
    )

    assert len(x) == schema.n_features
    names = schema.feature_names()
    assert len(names) == len(x)
    assert names[0] == "n:delta_z"

    idx_delta_z = names.index("n:delta_z")
    assert x[idx_delta_z] == 2.5

    idx_conf_sweep = names.index("b:conf_sweep")
    assert x[idx_conf_sweep] == 1.0

    idx_dir_long = names.index("dir:LONG")
    assert x[idx_dir_long] == 1.0

    idx_bucket_trend = names.index("bucket:trend")
    assert x[idx_bucket_trend] == 1.0

def test_ml_feature_schema_v4_of_nan_handling():
    schema = MLFeatureSchemaV4OF()
    indicators = {
        "delta_z": None,
        "spread_bps": math.nan,
        "ofi_stable": None
    }

    x = schema.vectorize(
        ts_ms=0,
        direction="SHORT",
        scenario="range",
        indicators=indicators,
        cancel_spike_veto=False
    )

    names = schema.feature_names()

    idx_delta_z = names.index("n:delta_z")
    assert x[idx_delta_z] == 0.0  # None is mapped to 0.0

    idx_ofi_stable = names.index("b:ofi_stable")
    assert x[idx_ofi_stable] == 0.0  # None is mapped to 0.0
