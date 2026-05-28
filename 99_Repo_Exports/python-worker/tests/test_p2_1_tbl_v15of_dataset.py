"""P2.1 — TBL × v15_of join dataset → train_v15_lgbm pipeline.

Covers:
  1.  norm_sid strips prefix correctly (≥3 parts → last 2)
  2.  norm_sid returns None for <3 parts
  3.  load_tbl_labels: parses sid, label, outcome from NDJSON
  4.  load_tbl_labels: derives label from hit_tp1 when no explicit label field
  5.  load_tbl_labels: skips lines with bad SID
  6.  load_tbl_labels: tp2 outcome_col uses hit_tp2 for label derivation
  7.  load_tbl_labels: skips malformed JSON lines (no crash)
  8.  _load_features: parses flat float dict from indicators JSON string
  9.  _load_features: ignores non-finite values
 10.  join_and_write: matched rows written correctly
 11.  join_and_write: unmatched snapshots silently skipped
 12.  join_and_write: output NDJSON has correct schema (sid, hit, tbl_outcome, features)
 13.  load_tbl_dataset: round-trips the NDJSON written by join_and_write
 14.  train_v15_lgbm: --source=tbl missing path → error code 2
 15.  train_v15_lgbm: --source=tbl with valid NDJSON loads samples
 16.  train_v15_lgbm: load_dataset_tbl skips rows with empty features
 17.  train_v15_lgbm: load_dataset_tbl skips rows with bad JSON
 18.  mfe_bps/mae_bps NaN → None in output JSON (JSON-serializable)
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest


# ─── helpers ──────────────────────────────────────────────────────────────────

def _write_ndjson(path: str, records: list[dict]) -> None:
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _make_tbl_label(*, sid: str, label: int | None = None, outcome: str = "tp1",
                    hit_tp1: bool = True, hit_tp2: bool = False, hit_sl: bool = False,
                    barrier_ms: int = 30000, mfe_bps: float = 15.0, mae_bps: float = 5.0):
    d: dict = {"sid": sid, "outcome": outcome, "hit_tp1": hit_tp1, "hit_tp2": hit_tp2,
               "hit_sl": hit_sl, "barrier_ms": barrier_ms, "mfe_bps": mfe_bps, "mae_bps": mae_bps}
    if label is not None:
        d["label"] = label
    return d


def _make_snapshot(*, sid: str, ts_ms: int = 1_700_000_000_000,
                   symbol: str = "BTCUSDT", regime: str = "trending_bull",
                   features: dict | None = None):
    return {
        "sid": sid,
        "ts_ms": ts_ms,
        "symbol": symbol,
        "regime": regime,
        "features": features or {"delta_z": 1.5, "ofi_z": 0.8, "obi": 0.3},
    }


# ─── 1. norm_sid: ≥3 parts → last 2 ──────────────────────────────────────────

def test_norm_sid_strips_prefix():
    from tools.build_dataset_v5_tb_v15of import norm_sid
    assert norm_sid("of:BTCUSDT:1700000000000") == "BTCUSDT:1700000000000"
    assert norm_sid("iceberg:BTCUSDT:1700000000000") == "BTCUSDT:1700000000000"
    assert norm_sid("a:b:c:d") == "c:d"


# ─── 2. norm_sid returns None for <3 parts ─────────────────────────────────────

def test_norm_sid_too_few_parts():
    from tools.build_dataset_v5_tb_v15of import norm_sid
    assert norm_sid("BTCUSDT:1700000000000") is None
    assert norm_sid("BTCUSDT") is None
    assert norm_sid("") is None
    assert norm_sid(None) is None


# ─── 3. load_tbl_labels: parses label, outcome ────────────────────────────────

def test_load_tbl_labels_parses_records():
    from tools.build_dataset_v5_tb_v15of import load_tbl_labels
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as f:
        f.write(json.dumps(_make_tbl_label(sid="of:BTCUSDT:1700000000000", label=1)) + "\n")
        f.write(json.dumps(_make_tbl_label(sid="of:ETHUSDT:1700000000001", label=0,
                                           outcome="sl")) + "\n")
        path = f.name
    try:
        labels = load_tbl_labels(path, outcome_col="tp1")
        assert "BTCUSDT:1700000000000" in labels
        assert labels["BTCUSDT:1700000000000"]["label"] == 1
        assert "ETHUSDT:1700000000001" in labels
        assert labels["ETHUSDT:1700000000001"]["label"] == 0
        assert labels["ETHUSDT:1700000000001"]["outcome"] == "sl"
    finally:
        os.unlink(path)


# ─── 4. load_tbl_labels: derives label from hit_<outcome_col> ─────────────────

def test_load_tbl_labels_derives_from_hit_field():
    from tools.build_dataset_v5_tb_v15of import load_tbl_labels
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as f:
        # No explicit label, but hit_tp1=True → label=1
        f.write(json.dumps(_make_tbl_label(sid="of:BTCUSDT:1000", hit_tp1=True)) + "\n")
        # hit_tp1=False → label=0
        f.write(json.dumps(_make_tbl_label(sid="of:ETHUSDT:2000", hit_tp1=False,
                                           outcome="sl")) + "\n")
        path = f.name
    try:
        labels = load_tbl_labels(path, outcome_col="tp1")
        assert labels["BTCUSDT:1000"]["label"] == 1
        assert labels["ETHUSDT:2000"]["label"] == 0
    finally:
        os.unlink(path)


# ─── 5. load_tbl_labels: skips bad SID ────────────────────────────────────────

def test_load_tbl_labels_skips_bad_sid():
    from tools.build_dataset_v5_tb_v15of import load_tbl_labels
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as f:
        f.write(json.dumps({"sid": "BTCUSDT", "label": 1}) + "\n")  # only 1 part
        f.write(json.dumps({"sid": "", "label": 1}) + "\n")           # empty
        f.write(json.dumps(_make_tbl_label(sid="of:SOLUSDT:9999", label=1)) + "\n")
        path = f.name
    try:
        labels = load_tbl_labels(path, outcome_col="tp1")
        assert len(labels) == 1
        assert "SOLUSDT:9999" in labels
    finally:
        os.unlink(path)


# ─── 6. load_tbl_labels: tp2 outcome_col uses hit_tp2 ─────────────────────────

def test_load_tbl_labels_tp2_outcome_col():
    from tools.build_dataset_v5_tb_v15of import load_tbl_labels
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as f:
        # hit_tp1=True but hit_tp2=False; outcome_col=tp2 → label=0
        f.write(json.dumps(_make_tbl_label(
            sid="of:BTCUSDT:1000", hit_tp1=True, hit_tp2=False
        )) + "\n")
        path = f.name
    try:
        labels = load_tbl_labels(path, outcome_col="tp2")
        assert labels["BTCUSDT:1000"]["label"] == 0
    finally:
        os.unlink(path)


# ─── 7. load_tbl_labels: skips malformed JSON ─────────────────────────────────

def test_load_tbl_labels_skips_bad_json():
    from tools.build_dataset_v5_tb_v15of import load_tbl_labels
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as f:
        f.write("NOT JSON\n")
        f.write(json.dumps(_make_tbl_label(sid="of:BTCUSDT:1000", label=1)) + "\n")
        path = f.name
    try:
        labels = load_tbl_labels(path, outcome_col="tp1")
        assert len(labels) == 1
    finally:
        os.unlink(path)


# ─── 8. _load_features: parses float dict from JSON string ────────────────────

def test_load_features_from_json_string():
    from tools.build_dataset_v5_tb_v15of import _load_features
    ind = json.dumps({"delta_z": 1.5, "ofi": 0.3, "flag": True, "na": None})
    result = _load_features(ind)
    assert result["delta_z"] == pytest.approx(1.5)
    assert result["ofi"] == pytest.approx(0.3)
    assert result["flag"] == pytest.approx(1.0)  # True → 1.0
    assert "na" not in result                     # None dropped


# ─── 9. _load_features: ignores non-finite values ─────────────────────────────

def test_load_features_drops_nonfinite():
    from tools.build_dataset_v5_tb_v15of import _load_features
    ind = {"good": 1.5, "bad_nan": float("nan"), "bad_inf": float("inf"),
           "bad_str": "not_a_number"}
    result = _load_features(ind)
    assert "good" in result
    assert "bad_nan" not in result
    assert "bad_inf" not in result
    assert "bad_str" not in result


# ─── 10. join_and_write: matched rows written correctly ────────────────────────

def test_join_and_write_matched():
    from tools.build_dataset_v5_tb_v15of import join_and_write
    snapshots = {"BTCUSDT:1000": _make_snapshot(sid="BTCUSDT:1000")}
    tbl_labels = {"BTCUSDT:1000": {
        "label": 1, "outcome": "tp1", "barrier_ms": 30000,
        "mfe_bps": 15.0, "mae_bps": 5.0,
    }}
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as f:
        path = f.name
    try:
        n_joined, n_unmatched = join_and_write(snapshots, tbl_labels, path)
        assert n_joined == 1
        assert n_unmatched == 0
        with open(path) as f:
            rec = json.loads(f.readline())
        assert rec["sid"] == "BTCUSDT:1000"
        assert rec["hit"] == 1
        assert rec["tbl_outcome"] == "tp1"
        assert rec["tbl_barrier_ms"] == 30000
        assert "features" in rec
        assert "delta_z" in rec["features"]
    finally:
        os.unlink(path)


# ─── 11. join_and_write: unmatched snapshots silently skipped ─────────────────

def test_join_and_write_unmatched():
    from tools.build_dataset_v5_tb_v15of import join_and_write
    snapshots = {
        "BTCUSDT:1000": _make_snapshot(sid="BTCUSDT:1000"),
        "ETHUSDT:2000": _make_snapshot(sid="ETHUSDT:2000"),
    }
    tbl_labels = {"BTCUSDT:1000": {
        "label": 0, "outcome": "sl", "barrier_ms": 5000,
        "mfe_bps": float("nan"), "mae_bps": 10.0,
    }}
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as f:
        path = f.name
    try:
        n_joined, n_unmatched = join_and_write(snapshots, tbl_labels, path)
        assert n_joined == 1
        assert n_unmatched == 1
    finally:
        os.unlink(path)


# ─── 12. join_and_write: output has correct schema ────────────────────────────

def test_join_and_write_schema():
    from tools.build_dataset_v5_tb_v15of import join_and_write
    snapshots = {"BTCUSDT:1000": _make_snapshot(sid="BTCUSDT:1000")}
    tbl_labels = {"BTCUSDT:1000": {
        "label": 1, "outcome": "tp1", "barrier_ms": 15000,
        "mfe_bps": 12.0, "mae_bps": float("nan"),
    }}
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as f:
        path = f.name
    try:
        join_and_write(snapshots, tbl_labels, path)
        with open(path) as f:
            rec = json.loads(f.readline())
        required = {"sid", "ts_ms", "symbol", "regime", "hit", "r",
                    "tbl_outcome", "tbl_barrier_ms", "tbl_mfe_bps", "tbl_mae_bps", "features"}
        assert required.issubset(rec.keys())
        # NaN → None (JSON serializable)
        assert rec["tbl_mae_bps"] is None
        assert rec["tbl_mfe_bps"] == pytest.approx(12.0)
    finally:
        os.unlink(path)


# ─── 13. load_tbl_dataset: round-trips join output ────────────────────────────

def test_load_tbl_dataset_round_trip():
    from tools.build_dataset_v5_tb_v15of import join_and_write, load_tbl_dataset
    snapshots = {
        "BTCUSDT:1000": _make_snapshot(sid="BTCUSDT:1000"),
        "ETHUSDT:2000": _make_snapshot(sid="ETHUSDT:2000", features={"ofi_z": -0.5}),
    }
    tbl_labels = {
        "BTCUSDT:1000": {"label": 1, "outcome": "tp1", "barrier_ms": 20000,
                         "mfe_bps": 10.0, "mae_bps": 3.0},
        "ETHUSDT:2000": {"label": 0, "outcome": "sl", "barrier_ms": 5000,
                         "mfe_bps": 2.0, "mae_bps": 8.0},
    }
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as f:
        path = f.name
    try:
        join_and_write(snapshots, tbl_labels, path)
        records = load_tbl_dataset(path)
        assert len(records) == 2
        sids = {r["sid"] for r in records}
        assert "BTCUSDT:1000" in sids
        assert "ETHUSDT:2000" in sids
    finally:
        os.unlink(path)


# ─── 14. train_v15_lgbm --source=tbl missing path → error 2 ──────────────────

def test_train_tbl_missing_path(monkeypatch, tmp_path):
    verdict = str(tmp_path / "v.json")
    out = str(tmp_path / "m.joblib")
    monkeypatch.setenv("V15_TBL_DATASET_PATH", "")
    with monkeypatch.context() as m:
        m.setattr(sys, "argv", [
            "train_v15_lgbm", "--source", "tbl",
            "--verdict-out", verdict, "--out", out,
        ])
        from tools import train_v15_lgbm
        result = train_v15_lgbm.main()
    assert result == 2


# ─── 15. train_v15_lgbm --source=tbl with valid dataset ──────────────────────

def test_train_tbl_loads_samples(monkeypatch, tmp_path):
    # Build a minimal TBL NDJSON with 5 samples (below 200 → REJECT, but samples loaded)
    tbl_path = str(tmp_path / "tbl.ndjson")
    records = []
    for i in range(5):
        records.append({
            "sid": f"BTCUSDT:{i}",
            "ts_ms": 1_700_000_000_000 + i * 1000,
            "symbol": "BTCUSDT",
            "regime": "trending_bull",
            "hit": i % 2,
            "r": 0.0,
            "tbl_outcome": "tp1",
            "tbl_barrier_ms": 30000,
            "tbl_mfe_bps": 10.0,
            "tbl_mae_bps": 3.0,
            "features": {"delta_z": float(i), "ofi_z": float(i) * 0.5},
        })
    _write_ndjson(tbl_path, records)

    verdict = str(tmp_path / "v.json")
    out = str(tmp_path / "m.joblib")
    with monkeypatch.context() as m:
        m.setattr(sys, "argv", [
            "train_v15_lgbm", "--source", "tbl",
            "--tbl-dataset-path", tbl_path,
            "--verdict-out", verdict, "--out", out,
        ])
        from tools import train_v15_lgbm
        result = train_v15_lgbm.main()
    # Expects REJECT (< 200 samples) but NOT error 2 from missing path
    assert result == 2
    assert os.path.exists(verdict)
    with open(verdict) as f:
        verd = json.load(f)
    assert verd["reason"] == "insufficient_data"
    assert verd["n_samples"] == 5


# ─── 16. load_dataset_tbl: skips rows with empty features ─────────────────────

def test_load_dataset_tbl_skips_empty_features(tmp_path):
    tbl_path = str(tmp_path / "tbl.ndjson")
    records = [
        {"sid": "BTCUSDT:1", "ts_ms": 1000, "symbol": "BTC", "regime": "na",
         "hit": 1, "r": 0.0, "features": {}},          # empty → skip
        {"sid": "BTCUSDT:2", "ts_ms": 2000, "symbol": "BTC", "regime": "na",
         "hit": 0, "r": 0.0, "features": {"a": 1.0}},  # OK
    ]
    _write_ndjson(tbl_path, records)
    from tools.train_v15_lgbm import load_dataset_tbl
    samples = load_dataset_tbl(tbl_path)
    assert len(samples) == 1
    assert samples[0].sid == "BTCUSDT:2"


# ─── 17. load_dataset_tbl: skips malformed JSON rows ─────────────────────────

def test_load_dataset_tbl_skips_bad_json(tmp_path):
    tbl_path = str(tmp_path / "tbl.ndjson")
    with open(tbl_path, "w") as f:
        f.write("INVALID JSON\n")
        f.write(json.dumps({
            "sid": "BTCUSDT:1", "ts_ms": 1000, "symbol": "BTC", "regime": "na",
            "hit": 1, "r": 0.0, "features": {"x": 1.0}
        }) + "\n")
    from tools.train_v15_lgbm import load_dataset_tbl
    samples = load_dataset_tbl(tbl_path)
    assert len(samples) == 1


# ─── 18. NaN mfe_bps → None in output (JSON-serializable) ─────────────────────

def test_join_nan_becomes_null():
    from tools.build_dataset_v5_tb_v15of import join_and_write
    import math as _math
    snapshots = {"BTCUSDT:1": _make_snapshot(sid="BTCUSDT:1")}
    tbl_labels = {"BTCUSDT:1": {
        "label": 1, "outcome": "tp1", "barrier_ms": None,
        "mfe_bps": float("nan"), "mae_bps": float("nan"),
    }}
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as f:
        path = f.name
    try:
        join_and_write(snapshots, tbl_labels, path)
        # File must be valid JSON (nan would break this)
        with open(path) as f:
            rec = json.loads(f.readline())
        assert rec["tbl_mfe_bps"] is None
        assert rec["tbl_mae_bps"] is None
        assert rec["tbl_barrier_ms"] is None
    finally:
        os.unlink(path)
