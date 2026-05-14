"""
Smoke tests for tools/build_dataset_from_inputs_outcomes_v4_tb.py.

Covers:
  - flat-payload (OFInputsV2) → indicators dict harvested without `indicators` key
  - og_* keys present in payload flow into indicators (the v14_of integration point)
  - cost-aware label columns surface in dataset
  - --y-label-col selects active label
  - label flip rate computed correctly
  - nested `indicators` legacy path still works
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


_BUILDER = "tools.build_dataset_from_inputs_outcomes_v4_tb"


def _write_ndjson(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _run_builder(*, inputs: Path, tb_labels: Path, out: Path, y_label_col: str = "y_edge") -> dict:
    cmd = [
        sys.executable, "-m", _BUILDER,
        "--inputs", str(inputs),
        "--tb-labels", str(tb_labels),
        "--out", str(out),
        "--y-label-col", y_label_col,
        "--out-format", "jsonl",  # tests don't depend on pyarrow
    ]
    cwd = str(Path(__file__).resolve().parent.parent)
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    assert proc.returncode == 0, f"builder failed:\nstdout={proc.stdout}\nstderr={proc.stderr}"
    with (out.parent / (out.name + ".json")).open() as f:
        return json.load(f)


def _read_out_jsonl(out: Path) -> pd.DataFrame:
    rows = []
    with out.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return pd.DataFrame(rows)


def _make_input_row(sid: str, *, indicators: dict | None = None, extras: dict | None = None) -> dict:
    """Build a OFInputsV2-shaped flat dict (mimics what strategy.py writes to signals:of:inputs)."""
    payload = {
        "v": 2,
        "sid": sid,
        "symbol": "BTCUSDT",
        "ts_ms": 1_700_000_000_000,
        "direction": "LONG",
        "scenario": "reversal",
        "scenario_v4": "reversal",
        "delta_z": 2.5,
        "spread_bps": 1.2,
        "ofi_z": 0.8,
        # og_* keys merged at publish time in strategy.py:
        "og_have": 2.0,
        "og_need": 2.0,
        "og_have_minus_need": 0.0,
        "og_ok": 1.0,
        "og_contrib_z": 0.3,
        "og_contrib_obi": 0.25,
        "og_reason_code_id": 17.0,
    }
    if indicators is not None:
        payload["indicators"] = indicators
    if extras:
        payload.update(extras)
    return {"payload": json.dumps(payload)}


def _make_tb_row(sid: str, *, y_edge: int = 1, y_edge_cost_aware: int | None = None,
                 cost_bps: float = 0.0, edge_after_cost: float = 0.0,
                 outcome: str = "TP_HIT", **extras) -> dict:
    """Build a TB labels row (from label_triple_barrier_from_ticks_v1.py output)."""
    row = {
        "sid": sid,
        "symbol": "BTCUSDT",
        "ts_ms": 1_700_000_000_000,
        "direction": "LONG",
        "entry_px": 100.0,
        "h_ms": 180000,
        "tp_bps": 10.0,
        "sl_bps": 10.0,
        "tb_outcome": outcome,
        "tb_hit_ms": 1_700_000_000_500,
        "mae_bps": 2.0,
        "mfe_bps": 12.0,
        "mae_r": 0.2,
        "mfe_r": 1.2,
        "adverse_proxy": 0.17,
        "y_edge": y_edge,
        "cost_bps": cost_bps,
        "realized_close_bps": 11.0,
        "edge_after_cost_bps": edge_after_cost,
        "y_edge_cost_aware": (y_edge_cost_aware if y_edge_cost_aware is not None else y_edge),
    }
    row.update(extras)
    return row


# ---------------------------------------------------------------------------
# Flat-payload (OFInputsV2) → indicators harvested
# ---------------------------------------------------------------------------

def test_flat_payload_indicators_harvested(tmp_path):
    inputs = tmp_path / "inputs.ndjson"
    labels = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"

    _write_ndjson(inputs, [_make_input_row("BTCUSDT:1700000000000:LONG")])
    _write_ndjson(labels, [_make_tb_row("BTCUSDT:1700000000000:LONG")])
    summary = _run_builder(inputs=inputs, tb_labels=labels, out=out)

    assert summary["joined_rows"] == 1
    df = _read_out_jsonl(out)
    assert len(df) == 1
    ind = df.iloc[0]["indicators"]
    # og_* keys must be present
    assert ind["og_have"] == 2.0
    assert ind["og_contrib_z"] == 0.3
    assert ind["og_reason_code_id"] == 17.0
    # legacy v13_of keys also flow through
    assert ind["delta_z"] == 2.5
    assert ind["spread_bps"] == 1.2
    # meta keys are NOT in indicators (extracted into named columns)
    assert "sid" not in ind
    assert "symbol" not in ind
    assert "direction" not in ind
    assert "scenario_v4" not in ind
    assert "ts_ms" not in ind


def test_flat_payload_meta_columns_extracted(tmp_path):
    inputs = tmp_path / "inputs.ndjson"
    labels = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"
    _write_ndjson(inputs, [_make_input_row("BTCUSDT:1700000000000:LONG")])
    _write_ndjson(labels, [_make_tb_row("BTCUSDT:1700000000000:LONG")])
    _run_builder(inputs=inputs, tb_labels=labels, out=out)

    df = _read_out_jsonl(out)
    row = df.iloc[0]
    assert row["symbol"] == "BTCUSDT"
    assert row["direction"] == "LONG"
    assert row["scenario_v4"] == "reversal"
    assert row["ts_ms"] == 1_700_000_000_000


# ---------------------------------------------------------------------------
# Cost-aware columns
# ---------------------------------------------------------------------------

def test_cost_aware_columns_surface(tmp_path):
    inputs = tmp_path / "inputs.ndjson"
    labels = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"

    _write_ndjson(inputs, [_make_input_row("S1")])
    _write_ndjson(labels, [_make_tb_row(
        "S1", y_edge=1, y_edge_cost_aware=0,
        cost_bps=20.0, edge_after_cost=-9.0,
    )])
    summary = _run_builder(inputs=inputs, tb_labels=labels, out=out)

    df = _read_out_jsonl(out)
    assert df.iloc[0]["cost_bps"] == 20.0
    assert df.iloc[0]["edge_after_cost_bps"] == -9.0
    assert df.iloc[0]["realized_close_bps"] == 11.0
    # Both labels preserved
    assert df.iloc[0]["y_edge_legacy"] == 1
    assert df.iloc[0]["y_edge_cost_aware"] == 0
    # Default --y-label-col=y_edge → active y_edge = legacy = 1
    assert df.iloc[0]["y_edge"] == 1
    assert summary["label_flip_rate"] == 1.0  # one row, label flipped


def test_y_label_col_switches_active_label(tmp_path):
    inputs = tmp_path / "inputs.ndjson"
    labels = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"

    _write_ndjson(inputs, [_make_input_row("S1")])
    _write_ndjson(labels, [_make_tb_row(
        "S1", y_edge=1, y_edge_cost_aware=0,
        cost_bps=20.0, edge_after_cost=-9.0,
    )])
    _run_builder(inputs=inputs, tb_labels=labels, out=out, y_label_col="y_edge_cost_aware")

    df = _read_out_jsonl(out)
    # Active y_edge column now reflects cost-aware label = 0
    assert df.iloc[0]["y_edge"] == 0
    assert df.iloc[0]["y_edge_legacy"] == 1  # legacy preserved


def test_summary_flip_rate(tmp_path):
    """Mixed flip cases: 1 of 3 flips → flip_rate == 1/3."""
    inputs = tmp_path / "inputs.ndjson"
    labels = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"

    _write_ndjson(inputs, [
        _make_input_row("S1"), _make_input_row("S2"), _make_input_row("S3"),
    ])
    _write_ndjson(labels, [
        _make_tb_row("S1", y_edge=1, y_edge_cost_aware=1),  # no flip
        _make_tb_row("S2", y_edge=0, y_edge_cost_aware=0),  # no flip
        _make_tb_row("S3", y_edge=1, y_edge_cost_aware=0),  # flip
    ])
    summary = _run_builder(inputs=inputs, tb_labels=labels, out=out)
    assert summary["joined_rows"] == 3
    assert abs(summary["label_flip_rate"] - (1.0 / 3.0)) < 1e-9


# ---------------------------------------------------------------------------
# Legacy nested `indicators` still works (backward-compat)
# ---------------------------------------------------------------------------

def test_legacy_nested_indicators_path(tmp_path):
    """If payload has an `indicators` sub-dict, prefer it (don't double-harvest)."""
    inputs = tmp_path / "inputs.ndjson"
    labels = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"

    # Payload with nested indicators (the legacy format some producers may use)
    row = _make_input_row(
        "S1",
        indicators={"custom_legacy_key": 42.0, "delta_z": 999.0},
    )
    _write_ndjson(inputs, [row])
    _write_ndjson(labels, [_make_tb_row("S1")])
    _run_builder(inputs=inputs, tb_labels=labels, out=out)

    df = _read_out_jsonl(out)
    ind = df.iloc[0]["indicators"]
    # Nested wins: only its keys are present
    assert ind["custom_legacy_key"] == 42.0
    assert ind["delta_z"] == 999.0  # nested value, not flat 2.5
    # og_* keys at top level are IGNORED when nested indicators present (legacy contract)
    assert "og_have" not in ind


# ---------------------------------------------------------------------------
# Missing TB labels: rows dropped + counted
# ---------------------------------------------------------------------------

def test_missing_tb_labels_dropped_and_counted(tmp_path):
    inputs = tmp_path / "inputs.ndjson"
    labels = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"

    _write_ndjson(inputs, [_make_input_row("S1"), _make_input_row("S2")])
    _write_ndjson(labels, [_make_tb_row("S1")])  # only S1, S2 missing
    summary = _run_builder(inputs=inputs, tb_labels=labels, out=out)

    assert summary["joined_rows"] == 1
    assert summary["missing_tb"] == 1
    df = _read_out_jsonl(out)
    assert len(df) == 1
    assert df.iloc[0]["sid"] == "S1"


def test_zero_cost_dataset_no_flip(tmp_path):
    """Legacy labeler run (cost_bps=0) → label_flip_rate=0 → backward-compat clean."""
    inputs = tmp_path / "inputs.ndjson"
    labels = tmp_path / "tb.ndjson"
    out = tmp_path / "ds.parquet"

    _write_ndjson(inputs, [_make_input_row("S1"), _make_input_row("S2")])
    _write_ndjson(labels, [
        _make_tb_row("S1", y_edge=1, y_edge_cost_aware=1, cost_bps=0.0),
        _make_tb_row("S2", y_edge=0, y_edge_cost_aware=0, cost_bps=0.0),
    ])
    summary = _run_builder(inputs=inputs, tb_labels=labels, out=out)
    assert summary["label_flip_rate"] == 0.0
