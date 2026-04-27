import json
import os
import tempfile
import pandas as pd

from tools.confirmations_coverage_nightly_job_v1 import build_report


def test_build_report_missing_dataset():
    rep = build_report("/no/such/path.parquet", min_rows=10, conf_min_nonzero_rate_warn=0.01)
    assert "dataset_missing" in rep["reasons"]


def test_build_report_conf_all_zero(tmp_path):
    df = pd.DataFrame({
        "conf_rsi_agree": [0,0,0,0],
        "conf_div_match": [0,0,0,0],
        "conf_sweep_eqh": [0,0,0,0],
        "conf_sweep_eql": [0,0,0,0],
        "conf_sweep_any": [0,0,0,0],
    })
    p = tmp_path / "d.parquet"
    df.to_parquet(p, engine="pyarrow")
    rep = build_report(str(p), min_rows=1, conf_min_nonzero_rate_warn=0.01)
    assert "conf_all_zero" in rep["reasons"]
    assert rep["summary"]["conf_bad_all_zero"] is True
