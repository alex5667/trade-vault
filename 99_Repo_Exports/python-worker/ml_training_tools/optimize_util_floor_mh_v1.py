from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict

import joblib
import numpy as np
import pandas as pd

from core.bucket_utils_v2 import bucket_from_scenario
from core.util_floor_opt_v1 import best_floor_by_sum_util
from core.ml_model_types import UtilMHModelV1


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, help="Input parquet dataset")
    ap.add_argument("--model", required=True, help="Model joblib path")
    ap.add_argument("--out", required=True, help="Output JSON path for util floors")
    ap.add_argument("--time-col", default="ts_ms", help="Time column name")
    ap.add_argument("--horizons", default=os.getenv("TB_HORIZONS_MS", "60000,180000,300000"), help="Comma-separated horizons in ms")
    ap.add_argument("--unc-k", type=float, default=float(os.getenv("UTIL_UNC_K", "0.5")), help="Uncertainty penalty coefficient")
    ap.add_argument("--floor-min", type=float, default=float(os.getenv("UTIL_FLOOR_GRID_MIN", "-0.05")), help="Minimum floor for grid search")
    ap.add_argument("--floor-max", type=float, default=float(os.getenv("UTIL_FLOOR_GRID_MAX", "0.10")), help="Maximum floor for grid search")
    ap.add_argument("--floor-step", type=float, default=float(os.getenv("UTIL_FLOOR_GRID_STEP", "0.005")), help="Floor grid step size")
    ap.add_argument("--min-trades", type=int, default=int(os.getenv("UTIL_MIN_TRADES", "200")), help="Minimum trades required")
    args = ap.parse_args()

    df = pd.read_parquet(args.dataset).sort_values(args.time_col).reset_index(drop=True)
    model = joblib.load(args.model)

    horizons = [int(x) for x in args.horizons.split(",") if x.strip().isdigit()]
    X = df[model.feature_cols].to_numpy(dtype=np.float32)

    # Predict util and uncertainty per horizon
    util_pred = model.predict_util(X)
    unc = model.predict_unc(X)

    # Horizon chooser: argmax(pred - k*unc)
    k = float(args.unc_k)
    scores = np.column_stack([util_pred[h] - k * unc[h] for h in horizons])
    best_idx = np.argmax(scores, axis=1)
    best_score = scores[np.arange(len(df)), best_idx]

    # Realized util for chosen horizon
    util_true = np.zeros(len(df), dtype=float)
    for i, h_i in enumerate(best_idx):
        h = horizons[int(h_i)]
        util_true[i] = float(df.get(f"util_r_{h}", pd.Series([0.0] * len(df))).iloc[i])

    # Buckets from scenario
    if "scenario_v4" in df.columns:
        buckets = np.array([bucket_from_scenario(str(s)) for s in df["scenario_v4"].fillna("").tolist()])
    else:
        buckets = np.array(["other"] * len(df))

    out: Dict[str, Any] = {"global": {}, "by_bucket": {}, "horizons": horizons, "unc_k": k}

    # Global floor optimization
    g = best_floor_by_sum_util(
        score=best_score,
        util_true=util_true,
        floor_min=float(args.floor_min),
        floor_max=float(args.floor_max),
        floor_step=float(args.floor_step),
        min_trades=int(args.min_trades),
    )
    out["global"] = g.__dict__

    # Per-bucket floor optimization
    for b in ("trend", "range", "other"):
        m = (buckets == b)
        if int(m.sum()) < int(args.min_trades):
            continue
        r = best_floor_by_sum_util(
            score=best_score[m],
            util_true=util_true[m],
            floor_min=float(args.floor_min),
            floor_max=float(args.floor_max),
            floor_step=float(args.floor_step),
            min_trades=int(args.min_trades),
        )
        out["by_bucket"][b] = r.__dict__

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(json.dumps({"out": args.out, "global_floor": out["global"].get("floor")}, ensure_ascii=False))


if __name__ == "__main__":
    main()

