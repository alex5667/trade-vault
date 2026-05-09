
import json
from unittest.mock import patch

import pandas as pd
import pytest

from tools.train_meta_model_lr_v2 import main


@pytest.fixture
def mock_parquet_file(tmp_path):
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
    path = tmp_path / "train_data.parquet"
    df.to_parquet(path)
    return str(path)

def test_train_meta_model_lr_v2_end_to_end(mock_parquet_file, tmp_path):
    out_json = tmp_path / "model.json"
    out_joblib = tmp_path / "model.joblib"

    # Mock sys.argv
    with patch("sys.argv", [
        "train_meta_model_lr_v2.py",
        "--parquet", mock_parquet_file,
        "--out_json", str(out_json),
        "--out_joblib", str(out_joblib),
        "--threshold", "0.6"
    ]):
        main()

    assert out_json.exists()
    assert out_joblib.exists()

    with open(out_json) as f:
        model = json.load(f)

    assert "features" in model
    assert "coef" in model
    assert "intercept" in model
    assert model["threshold"] == 0.6
    assert "robust_scaler" in model
    assert "delta_z" in model["robust_scaler"]["params"]

def test_train_meta_model_lr_v2_missing_y(tmp_path):
    path = tmp_path / "bad_data.parquet"
    pd.DataFrame({"x": [1, 2]}).to_parquet(path)

    with patch("sys.argv", [
        "train_meta_model_lr_v2.py",
        "--parquet", str(path),
        "--out_json", str(tmp_path / "out.json")
    ]), pytest.raises(SystemExit, match="missing column y"):
        main()
