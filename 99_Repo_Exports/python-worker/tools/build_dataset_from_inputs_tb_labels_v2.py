from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd


def _read_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    """Read NDJSON file line by line."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _get_payload(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Extract payload from object (handle nested payload field)."""
    if "payload" in obj and isinstance(obj["payload"], str) and obj["payload"].strip().startswith("{"):
        try:
            return json.loads(obj["payload"])
        except Exception:
            return obj
    return obj


FEATURE_KEYS = [
    # core OF / microstructure
    "delta_z",
    "ofi",
    "ofi_z",
    "obi",
    "obi_stability_score",
    "obi_age_ms",
    "iceberg_age_ms",
    "iceberg_strict",
    "abs_lvl_ok",
    "fp_edge_absorb",
    "weak_progress",
    "reclaim",
    "sweep",
    # execution / costs
    "spread_bps",
    "expected_slippage_bps",
    "exec_risk_bps",
    "exec_risk_norm",
    "liq_score",
    "liq_regime",
    # pressure / burst
    "pressure_sps",
    "pressure_hi",
    "weak_recent_ratio",
    "weak_recent_count",
]


def _as_num(x: Any) -> float:
    """Convert value to float, handling None, bool, etc."""
    try:
        if x is None:
            return 0.0
        if isinstance(x, bool):
            return 1.0 if x else 0.0
        return float(x)
    except Exception:
        return 0.0


def build_row_from_indicators(ind: Dict[str, Any]) -> Dict[str, float]:
    """Extract features from indicators dict with fixed whitelist."""
    out: Dict[str, float] = {}
    for k in FEATURE_KEYS:
        out[f"f_{k}"] = _as_num(ind.get(k))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="of_inputs ndjson (payload objects)")
    ap.add_argument("--tb", required=True, help="tb labels ndjson (payload objects or direct)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--primary-h-ms", type=int, default=180000)
    ap.add_argument("--drop-no-ticks", type=int, default=1)
    args = ap.parse_args()

    # tb index by sid
    tb: Dict[str, Dict[str, Any]] = {}
    for obj in _read_ndjson(args.tb):
        o = _get_payload(obj)
        sid = str(o.get("sid", "") or "")
        if sid:
            tb[sid] = o

    rows: List[Dict[str, Any]] = []
    miss = 0
    dropped = 0

    for obj in _read_ndjson(args.inputs):
        o = _get_payload(obj)
        sid = str(o.get("sid", "") or "")
        if not sid:
            continue
        t = tb.get(sid)
        if not t:
            miss += 1
            continue

        horizons = t.get("horizons") if isinstance(t.get("horizons"), dict) else {}
        primary = t.get("primary") if isinstance(t.get("primary"), dict) else {}
        if not primary and isinstance(horizons, dict):
            primary = horizons.get(str(args.primary_h_ms), {})

        label = str(primary.get("label", "") or "")
        if int(args.drop_no_ticks) == 1 and label in ("NO_TICKS", "NO_PATH", ""):
            dropped += 1
            continue

        ind = o.get("indicators") if isinstance(o.get("indicators"), dict) else {}

        r: Dict[str, Any] = {
            "sid": sid,
            "ts_ms": int(o.get("ts_ms", o.get("ts", 0)) or 0),
            "symbol": str(o.get("symbol", "") or ""),
            "direction": str(o.get("direction", "") or ""),
            "scenario_v4": str(o.get("scenario_v4", o.get("scenario", "")) or ""),
            # labels (primary horizon)
            "y_edge": int(primary.get("y_edge", 0) or 0),
            "tb_label": label,
            "tb_r_mult": float(primary.get("r_mult", 0.0) or 0.0),
            "tb_ret_bps": float(primary.get("ret_bps", 0.0) or 0.0),
            "tb_mae_bps": float(primary.get("mae_bps", 0.0) or 0.0),
            "tb_mfe_bps": float(primary.get("mfe_bps", 0.0) or 0.0),
            "tb_adverse_proxy": float(primary.get("adverse_proxy", 0.0) or 0.0),
        }

        # util label if present in meta (v10.1 produces util_r)
        meta = t.get("meta") if isinstance(t.get("meta"), dict) else {}
        util_r = float(meta.get("util_r", 0.0) or 0.0)
        r["util_r"] = util_r
        r["y_util_pos"] = 1 if util_r > 0.0 else 0

        # multi-horizon y_edge columns (optional)
        if isinstance(horizons, dict):
            for h in ("60000", "180000", "300000"):
                hh = horizons.get(h, {})
                if isinstance(hh, dict):
                    r[f"y_edge_{h}"] = int(hh.get("y_edge", 0) or 0)

        # features
        r.update(build_row_from_indicators(ind))

        rows.append(r)

    df = pd.DataFrame(rows)

    # one-hot for categorical columns (stable, limited)
    if len(df):
        for col in ("direction", "scenario_v4"):
            if col in df.columns:
                dummies = pd.get_dummies(df[col].fillna(""), prefix=col)
                df = pd.concat([df.drop(columns=[col]), dummies], axis=1)

    df.to_parquet(args.out, index=False)

    summary = {
        "inputs_rows": len(rows) + miss + dropped,
        "joined_rows": len(rows),
        "missing_tb": miss,
        "dropped_no_ticks": dropped,
        "pos_rate_y_edge": float(df["y_edge"].mean()) if len(df) and "y_edge" in df else 0.0,
        "pos_rate_y_util_pos": float(df["y_util_pos"].mean()) if len(df) and "y_util_pos" in df else 0.0,
        "columns": list(df.columns),
    }
    with open(args.out + ".json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

