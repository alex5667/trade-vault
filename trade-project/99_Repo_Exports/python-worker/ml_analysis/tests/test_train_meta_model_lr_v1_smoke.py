import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


def _ensure_repo_root_first() -> None:
    # conftest.py puts tick_flow_full/ ahead of the repo root. For production code
    # we want to test imports from the repo-root `core/`.
    repo_root = Path(__file__).resolve().parents[2]
    rp = str(repo_root)
    if sys.path and sys.path[0] != rp:
        sys.path.insert(0, rp)


def test_train_meta_model_lr_from_df_smoke(tmp_path: Path):
    _ensure_repo_root_first()

    from core.meta_model_lr import MetaModelLR
    from ml_analysis.tools.train_meta_model_lr_v1 import train_meta_model_lr_from_df

    n = 240
    ts0 = 1_700_000_000_000
    df = pd.DataFrame(
        {
            "sid": np.arange(n, dtype=np.int64),
            "ts_ms": ts0 + np.arange(n, dtype=np.int64) * 1000,
            "symbol": ["BTCUSDT"] * n,
            # A few typical flattened indicator columns (parquet uses f_*).
            "f_delta_z": np.random.normal(size=n),
            "f_ofi": np.random.normal(size=n),
            "f_spread_bps": np.abs(np.random.normal(size=n) * 2.0),
            "f_liq_score": np.clip(np.random.normal(loc=1.0, scale=0.3, size=n), 0.0, 10.0),
            # One-hot scenarios (trainer decodes scenario_v4_* keys).
            "scenario_v4_trend": [1.0] * n,
            # Label (example: horizon 60s)
            "y_util_pos_60000": (np.arange(n) % 5 == 0).astype(int),
        }
    )

    model, summary = train_meta_model_lr_from_df(
        df,
        schema_name="meta_feat_v8",
        y_col="y_util_pos_60000",
        n_splits=4,
        purge_ms=10_000,
        embargo_ms=10_000,
        C=1.0,
        max_iter=200,
        threshold=0.5,
    )

    assert isinstance(model, MetaModelLR)
    assert len(model.features) > 10  # schema has many features
    assert len(model.coef) == len(model.features)
    assert isinstance(summary, dict)
    assert summary.get("y_col") == "y_util_pos_60000"

    out = tmp_path / "meta_lr.json"
    model.dump(str(out))
    m2 = MetaModelLR.load(str(out))
    assert m2.signature_ok()

    # Deterministic prediction parity after load
    feat = dict.fromkeys(m2.features, 0.0)
    p1 = float(m2.predict_proba(feat))
    p2 = float(MetaModelLR.load(str(out)).predict_proba(feat))
    assert abs(p1 - p2) < 1e-12


def test_confirm_train_v7_missing_y_util_pos_is_derived_from_outcomes(tmp_path: Path):
    _ensure_repo_root_first()

    from ml_analysis.tools.train_meta_model_lr_v1 import (
        _enrich_decision_feature_columns,
        _ensure_label_column,
        _flatten_indicator_columns,
    )

    dataset = tmp_path / "latest_confirm_train_v7.ndjson"
    outcomes = tmp_path / "latest_outcomes.ndjson"
    df = pd.DataFrame(
        {
            "sid": ["s1", "s2", "s3"],
            "ts_ms": [1000, 2000, 3000],
            "symbol": ["BTCUSDT", "BTCUSDT", "ETHUSDT"],
            "indicators_small": [None, None, None],
            "decision_spread_bps": [2.0, 3.0, 4.0],
            "decision_expected_slippage_bps": [0.5, 0.7, 0.9],
            "decision_exec_risk_norm": [0.1, 0.2, 0.3],
            "decision_depth_bid_5": [120.0, 80.0, 50.0],
            "decision_depth_ask_5": [80.0, 120.0, 50.0],
            "decision_book_slope_bid": [0.11, 0.22, 0.33],
            "decision_book_slope_ask": [0.44, 0.55, 0.66],
            "decision_ofi_norm": [1.5, -0.5, 0.1],
            "of_score_final": [0.9, 0.2, 0.0],
            "liqmap_1h_total_usd": [1000.0, 0.0, 500.0],
            "decision_liqmap_gate_rr": [1.2, 0.0, 0.5],
        }
    )
    out = pd.DataFrame(
        {
            "sid": ["s1", "s2", "s3"],
            "pnl": [0.25, -0.1, 0.0],
            "risk_usd": [1.0, 1.0, 0.0],
        }
    )
    df.to_json(dataset, orient="records", lines=True)
    out.to_json(outcomes, orient="records", lines=True)

    labeled = _ensure_label_column(
        df,
        y_col="y_util_pos_60000",
        dataset_path=str(dataset),
        outcomes_path=str(outcomes),
    )
    flattened = _flatten_indicator_columns(labeled)
    enriched = _enrich_decision_feature_columns(flattened, schema_name="meta_feat_v9")

    assert enriched["y_util_pos_60000"].tolist() == [1, 0, 0]
    assert enriched["r_mult"].tolist() == [0.25, -0.1, 0.0]
    assert enriched["f_ofi_ml_norm"].tolist() == [1.5, -0.5, 0.1]
    assert enriched["f_ofi"].tolist() == [1.5, -0.5, 0.1]
    assert enriched["f_spread_bps"].tolist() == [2.0, 3.0, 4.0]
    assert enriched["f_expected_slippage_bps"].tolist() == [0.5, 0.7, 0.9]
    assert enriched["exec_risk_norm"].tolist() == [0.1, 0.2, 0.3]
    assert enriched["f_depth_bid_5"].tolist() == [120.0, 80.0, 50.0]
    assert enriched["f_depth_ask_5"].tolist() == [80.0, 120.0, 50.0]
    assert enriched["f_qimb_wmean"].round(6).tolist() == [0.2, -0.2, 0.0]
    assert enriched["rule_score"].tolist() == [0.9, 0.2, 0.0]
    assert enriched["f_liqmap_1h_total_usd"].tolist() == [1000.0, 0.0, 500.0]
    assert enriched["f_liqmap_gate_rr"].tolist() == [1.2, 0.0, 0.5]
    assert enriched["f_liqmap_5m_total_usd"].tolist() == [0.0, 0.0, 0.0]

    liqmap_cols = [c for c in enriched.columns if c.startswith("f_liqmap_")]
    assert len(liqmap_cols) == 37


def test_train_meta_model_lr_cli_help_smoke(monkeypatch):
    _ensure_repo_root_first()

    import ml_analysis.tools.train_meta_model_lr_v1 as m

    monkeypatch.setattr(sys, "argv", ["train_meta_model_lr_v1", "--help"])
    with pytest.raises(SystemExit) as e:
        m.main()
    assert e.value.code == 0
