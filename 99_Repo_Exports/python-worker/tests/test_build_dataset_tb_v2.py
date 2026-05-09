"""Tests for build_dataset_from_inputs_tb_labels_v2."""

import json
import os
import subprocess
import sys

import pandas as pd


def _w(path, rows):
    """Write NDJSON file."""
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def test_join_build(tmp_path):
    """Test dataset building with inputs and TB labels."""
    inputs = tmp_path / "inputs.ndjson"
    tb = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"

    _w(inputs, [{
        "sid": "s1",
        "ts_ms": 1,
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "scenario_v4": "reversal",
        "indicators": {"delta_z": 2.0, "spread_bps": 2.0, "expected_slippage_bps": 2.0},
    }])
    _w(tb, [{
        "sid": "s1",
        "horizons": {"180000": {"label": "TP", "y_edge": 1, "r_mult": 1.0, "ret_bps": 50.0, "mae_bps": 10.0, "mfe_bps": 60.0, "adverse_proxy": 0.2}},
        "primary": {"label": "TP", "y_edge": 1, "r_mult": 1.0, "ret_bps": 50.0, "mae_bps": 10.0, "mfe_bps": 60.0, "adverse_proxy": 0.2},
        "meta": {"util_r": 0.5},
    }])

    subprocess.check_call([
        sys.executable, "-m", "tools.build_dataset_from_inputs_tb_labels_v2",
        "--inputs", str(inputs),
        "--tb", str(tb),
        "--out", str(out),
        "--primary-h-ms", "180000",
        "--drop-no-ticks", "1",
    ], env={**os.environ, "PYTHONPATH": ".:.."})

    df = pd.read_parquet(out)
    assert len(df) == 1
    assert int(df["y_edge"].iloc[0]) == 1
    assert "f_delta_z" in df.columns
    assert "f_spread_bps" in df.columns


def test_missing_tb_label(tmp_path):
    """Test handling of missing TB labels."""
    inputs = tmp_path / "inputs.ndjson"
    tb = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"

    _w(inputs, [
        {"sid": "s1", "ts_ms": 1, "symbol": "BTCUSDT", "indicators": {}},
        {"sid": "s2", "ts_ms": 2, "symbol": "ETHUSDT", "indicators": {}},
    ])
    _w(tb, [
        {"sid": "s1", "primary": {"label": "TP", "y_edge": 1}},
    ])

    subprocess.check_call([
        sys.executable, "-m", "tools.build_dataset_from_inputs_tb_labels_v2",
        "--inputs", str(inputs),
        "--tb", str(tb),
        "--out", str(out),
        "--primary-h-ms", "180000",
        "--drop-no-ticks", "1",
    ], env={**os.environ, "PYTHONPATH": ".:.."})

    df = pd.read_parquet(out)
    assert len(df) == 1
    assert df["sid"].iloc[0] == "s1"


def test_drop_no_ticks(tmp_path):
    """Test dropping NO_TICKS labels."""
    inputs = tmp_path / "inputs.ndjson"
    tb = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"

    _w(inputs, [
        {"sid": "s1", "ts_ms": 1, "symbol": "BTCUSDT", "indicators": {}},
        {"sid": "s2", "ts_ms": 2, "symbol": "ETHUSDT", "indicators": {}},
    ])
    _w(tb, [
        {"sid": "s1", "primary": {"label": "TP", "y_edge": 1}},
        {"sid": "s2", "primary": {"label": "NO_TICKS", "y_edge": 0}},
    ])

    subprocess.check_call([
        sys.executable, "-m", "tools.build_dataset_from_inputs_tb_labels_v2",
        "--inputs", str(inputs),
        "--tb", str(tb),
        "--out", str(out),
        "--primary-h-ms", "180000",
        "--drop-no-ticks", "1",
    ], env={**os.environ, "PYTHONPATH": ".:.."})

    df = pd.read_parquet(out)
    assert len(df) == 1
    assert df["sid"].iloc[0] == "s1"

