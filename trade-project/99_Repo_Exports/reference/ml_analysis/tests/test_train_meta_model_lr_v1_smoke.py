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

    from ml_analysis.tools.train_meta_model_lr_v1 import train_meta_model_lr_from_df
    from core.meta_model_lr import MetaModelLR

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
    feat = {k: 0.0 for k in m2.features}
    p1 = float(m2.predict_proba(feat))
    p2 = float(MetaModelLR.load(str(out)).predict_proba(feat))
    assert abs(p1 - p2) < 1e-12


def test_train_meta_model_lr_cli_help_smoke(monkeypatch):
    _ensure_repo_root_first()

    import ml_analysis.tools.train_meta_model_lr_v1 as m

    monkeypatch.setattr(sys, "argv", ["train_meta_model_lr_v1", "--help"])
    with pytest.raises(SystemExit) as e:
        m.main()
    assert e.value.code == 0
