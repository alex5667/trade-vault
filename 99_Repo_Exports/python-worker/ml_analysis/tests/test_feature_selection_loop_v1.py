import json
import os

import numpy as np
import pandas as pd


def test_feature_selection_loop_v1_runs(tmp_path):
    # Synthetic dataset: one stable strong feature, one regime-unstable, one noise.
    n = 6000
    base_ts = 1700000000000  # fixed epoch ms
    ts_ms = base_ts + np.arange(n, dtype=np.int64) * 60_000  # 1-min steps
    regimes = np.array(["trend", "range", "other"], dtype=object)
    scenario_v4 = regimes[np.arange(n) % 3]

    rng = np.random.default_rng(7)
    good = rng.normal(0, 1, size=n)
    noise = rng.normal(0, 1, size=n)

    # Unstable feature: only useful in trend
    unstable = rng.normal(0, 1, size=n)
    unstable = unstable + (scenario_v4 == "trend") * (0.8 * good)

    # Label: depends mainly on good, slight contribution from unstable in trend
    logits = 2.0 * good + 0.5 * (scenario_v4 == "trend") * unstable + 0.1 * noise
    p = 1.0 / (1.0 + np.exp(-logits))
    y = (rng.random(n) < p).astype(np.int8)

    df = pd.DataFrame(
        {
            "sid": [f"s{i}" for i in range(n)],
            "ts_ms": ts_ms,
            "scenario_v4": scenario_v4,
            "y": y,
            "n_good": good,
            "n_unstable": unstable,
            "n_noise": noise,
        }
    )

    data_path = tmp_path / "ds.csv"
    df.to_csv(data_path, index=False)

    meta = {
        "ver": "v_test",
        "schema_hash": "deadbeef" * 4,
        "feature_names": ["n:good", "n:unstable", "n:noise"],
        "column_names": ["n_good", "n_unstable", "n_noise"],
    }
    meta_path = tmp_path / "ds.csv.meta.json"
    meta_path.write_text(json.dumps(meta), encoding="utf-8")

    out_dir = tmp_path / "out"

    from ml_analysis.tools.feature_selection_loop_v1 import main

    main(
        [
            "--data_path",
            str(data_path),
            "--meta_json",
            str(meta_path),
            "--out_dir",
            str(out_dir),
            "--model",
            "lr",
            "--max_val_rows",
            "4000",
            "--n_repeats",
            "1",
            "--min_group_rows",
            "200",
        ]
    )

    # Outputs exist
    for fn in [
        "summary.json",
        "importance_global.csv",
        "importance_by_regime.csv",
        "importance_by_hour.csv",
        "stability_table.csv",
        "perf_by_regime.csv",
        "perf_by_hour.csv",
        "report.md",
    ]:
        assert (out_dir / fn).exists(), fn

    # Stability sanity: unstable should have higher regime_cv than good (on average).
    stab = pd.read_csv(out_dir / "stability_table.csv")
    cv_good = float(stab.loc[stab["feature"] == "n:good", "regime_cv"].iloc[0])
    cv_unstable = float(stab.loc[stab["feature"] == "n:unstable", "regime_cv"].iloc[0])
    assert cv_unstable >= cv_good
