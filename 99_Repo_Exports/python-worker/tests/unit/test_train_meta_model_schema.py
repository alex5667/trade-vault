
import json
from unittest.mock import patch

import pandas as pd
import pytest

from core.meta_features_v1 import META_FEAT_V1_HASH, META_FEAT_V1_NAME, META_FEAT_V1_VERSION
from tools.train_meta_model_lr_v2 import main


@pytest.fixture
def mock_parquet_file_v1(tmp_path):
    # Create a dummy parquet file
    data = {
        "y": [0, 1, 0, 1] * 50,
        "indicators": [
            json.dumps({"score_breakdown": {"base_score": 0.5, "final_score_raw": 0.4}, "exec_pen": 0.1, "delta_z": 1.0})
        ] * 200,
        "direction": ["BUY"] * 200,
        "scenario_v4": ["trend"] * 200
    }
    df = pd.DataFrame(data)
    path = tmp_path / "train_data_v1.parquet"
    df.to_parquet(path)
    return str(path)

def test_train_meta_model_schema_metadata(mock_parquet_file_v1, tmp_path):
    """Verify that the training script asserts schema metadata in output JSON."""
    out_json = tmp_path / "model_v1.json"

    # Mock sys.argv
    with patch("sys.argv", [
        "train_meta_model_lr_v2.py",
        "--parquet", mock_parquet_file_v1,
        "--out_json", str(out_json),
        "--threshold", "0.6"
    ]):
        main()

    assert out_json.exists()

    with open(out_json) as f:
        model = json.load(f)

    # Standard fields
    assert "features" in model
    assert "coef" in model
    assert "intercept" in model

    # Schema Metadata (The crucial part of P1)
    assert model.get("schema_name") == META_FEAT_V1_NAME
    assert model.get("schema_version") == META_FEAT_V1_VERSION
    assert model.get("schema_hash") == META_FEAT_V1_HASH

    # Ensure features match canonical list (implicit in script, but good to check)
    from core.meta_features_v1 import META_FEAT_V1_COLS
    assert model["features"] == META_FEAT_V1_COLS
