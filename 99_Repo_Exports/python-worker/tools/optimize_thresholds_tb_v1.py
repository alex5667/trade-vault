from __future__ import annotations

import argparse
import json
import os
from typing import Any

import joblib
import numpy as np
import pandas as pd

from core.bucket_utils_v2 import bucket_from_scenario
from core.threshold_opt_v1 import best_threshold_by_utility


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--model", required=True)
    ap.add_argument("--out", required=True, help="json with thresholds by bucket")
    ap.add_argument("--time-col", default="ts_ms")
    ap.add_argument("--thr-min", type=float, default=float(os.getenv("THR_GRID_MIN", "0.35")))
    ap.add_argument("--thr-max", type=float, default=float(os.getenv("THR_GRID_MAX", "0.85")))
    ap.add_argument("--thr-step", type=float, default=float(os.getenv("THR_GRID_STEP", "0.01")))
    ap.add_argument("--min-trades", type=int, default=int(os.getenv("THR_MIN_TRADES", "200")))
    ap.add_argument("--label-edge-col", default="y_edge")     # used for diagnostics
    ap.add_argument("--util-col", default="util_r")           # optimize on util_r
    args = ap.parse_args()

    df = pd.read_parquet(args.dataset).sort_values(args.time_col).reset_index(drop=True)

    model = joblib.load(args.model)
    feature_cols = getattr(model, "feature_cols", None)
    if not feature_cols:
        # fallback: try meta
        raise SystemExit("model missing feature_cols; use tb_stack_v2_strict_oof model")

    X = df[feature_cols].to_numpy(dtype=np.float32)
    p = model.predict_proba(X)[:, 1]

    y_edge = df[args.label_edge_col].astype(int).to_numpy() if args.label_edge_col in df.columns else np.zeros(len(df), dtype=int)
    util_r = df[args.util_col].astype(float).to_numpy() if args.util_col in df.columns else np.zeros(len(df), dtype=float)

    # buckets by scenario (one-hot was applied in dataset builder; we keep raw scenario by storing it too in dataset v2? If not, fallback to other)
    if "scenario_v4_range_meanrev" in df.columns or "scenario_v4_" in " ".join(df.columns):
        # If one-hot scenario exists, recover approximate bucket:
        # prefer range if any range scenario column is 1.
        def _bucket_from_onehot(row) -> str:
            # naive: if any scenario_v4_* contains 'range' and is 1 -> range, else trend if 'reversal'/'continuation'
            for c in df.columns:
                if c.startswith("scenario_v4_") and row[c] == 1:
                    name = c[len("scenario_v4_"):]
                    return bucket_from_scenario(name)
            return "other"
        buckets = np.array([_bucket_from_onehot(df.iloc[i]) for i in range(len(df))])
    else:
        buckets = np.array(["other"] * len(df))

    out: dict[str, Any] = {"thresholds": {}, "global": {}}

    # global
    g = best_threshold_by_utility(
        p=p, y_edge=y_edge, util_r=util_r,
        thr_min=float(args.thr_min), thr_max=float(args.thr_max), thr_step=float(args.thr_step),
        min_trades=int(args.min_trades),
    )
    out["global"] = g.__dict__

    for bucket in ("trend","range","other"):
        m = (buckets == bucket)
        if m.sum() < int(args.min_trades):
            continue
        r = best_threshold_by_utility(
            p=p[m], y_edge=y_edge[m], util_r=util_r[m],
            thr_min=float(args.thr_min), thr_max=float(args.thr_max), thr_step=float(args.thr_step),
            min_trades=int(args.min_trades),
        )
        out["thresholds"][bucket] = r.__dict__

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(_safe_json({"out": args.out, "global_thr": out["global"].get("thr", None)}))


if __name__ == "__main__":
    main()

