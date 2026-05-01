from __future__ import annotations
\

import argparse
import json
from typing import Any, Dict, Optional, Tuple

from core.ml_feature_schema import build_features
from core.ml_metrics_utils import brier_score, ece_score

def load_ndjson(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            yield json.loads(line)

def index_closed_by_sid(closed_path: str) -> dict:
    idx = {}
    for row in load_ndjson(closed_path):
        sid = row.get("sid") or row.get("signal_id") or row.get("id")
        if not sid:
            continue
        idx[str(sid)] = row
    return idx

def adverse_proxy_from_row(row: Dict[str, Any]) -> float:
    # Use MAE before MFE if present, else mae_bps as proxy.
    for k in ("mae_bps","mae","mae_r","MAE_R"):
        if k in row:
            try:
                return float(row[k])
            except Exception:
                pass
    return 0.0

def r_net_from_row(row: Dict[str, Any]) -> float:
    for k in ("realized_R","r_mult","R_net","r_net"):
        if k in row:
            try:
                return float(row[k])
            except Exception:
                pass
    return 0.0

def build_y(r_net: float, adverse: float, r_min: float, adv_max: float) -> int:
    return 1 if (r_net >= r_min and adverse <= adv_max) else 0

def build_u(row: Dict[str, Any]) -> float:
    # Utility proxy: pnl_net - costs. If not available, use pnl or r_mult.
    for k in ("pnl_net","pnl","pnl_usd"):
        if k in row:
            try:
                return float(row[k])
            except Exception:
                pass
    return r_net_from_row(row)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True)
    ap.add_argument("--closed", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--r-min", type=float, default=0.5)
    ap.add_argument("--adv-max", type=float, default=1.0)
    args = ap.parse_args()

    closed = index_closed_by_sid(args.closed)

    n_in = 0
    n_join = 0

    with open(args.out, "w", encoding="utf-8") as f:
        for inp in load_ndjson(args.inputs):
            n_in += 1
            sid = inp.get("sid")
            if not sid:
                continue
            c = closed.get(str(sid))
            if not c:
                continue

            r_net = r_net_from_row(c)
            adverse = adverse_proxy_from_row(c)
            y_edge = build_y(r_net, adverse, args.r_min, args.adv_max)
            y_util = build_u(c)

            feat = build_features(inp)

            out = {
                "sid": str(sid),
                "ts_ms": int(inp.get("ts_ms", 0) or 0),
                "symbol": str(inp.get("symbol","")),
                "scenario": str(inp.get("scenario", "none")),
                "direction": str(inp.get("direction","")),
                "y_edge": int(y_edge),
                "y_util": float(y_util),
                "x": feat.x,
                "feature_names": feat.feature_names,
            }
            f.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_join += 1

    print(f"inputs={n_in} joined={n_join} out={args.out}")

if __name__ == "__main__":
    main()
