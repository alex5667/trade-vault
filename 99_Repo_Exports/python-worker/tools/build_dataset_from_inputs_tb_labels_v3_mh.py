from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from typing import Any

import pandas as pd


def _read_ndjson(path: str) -> Iterable[dict[str, Any]]:
    """Read NDJSON file line by line."""
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _get_payload(obj: dict[str, Any]) -> dict[str, Any]:
    """Extract payload from object (handles nested JSON strings)."""
    if "payload" in obj and isinstance(obj["payload"], str) and obj["payload"].strip().startswith("{"):
        try:
            return json.loads(obj["payload"])
        except Exception:
            return obj
    return obj


# Feature keys from OF inputs indicators
FEATURE_KEYS = [
    "delta_z", "ofi", "ofi_z", "obi", "obi_stability_score", "obi_age_ms",
    "iceberg_strict", "abs_lvl_ok", "fp_edge_absorb", "weak_progress",
    "reclaim", "sweep", "spread_bps", "expected_slippage_bps",
    "exec_risk_bps", "exec_risk_norm", "liq_score", "liq_regime",
    "pressure_sps", "pressure_hi",
]


def _as_num(x: Any) -> float:
    """Convert value to float, handling None/bool."""
    try:
        if x is None:
            return 0.0
        if isinstance(x, bool):
            return 1.0 if x else 0.0
        return float(x)
    except Exception:
        return 0.0


def build_feat(ind: dict[str, Any]) -> dict[str, float]:
    """Build feature dict from indicators."""
    return {f"f_{k}": _as_num(ind.get(k)) for k in FEATURE_KEYS}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="OF inputs NDJSON path")
    ap.add_argument("--tb", required=True, help="TB labels NDJSON path")
    ap.add_argument("--out", required=True, help="Output parquet path")
    ap.add_argument("--horizons", default="60000,180000,300000", help="Comma-separated horizons in ms")
    ap.add_argument("--drop-no-ticks", type=int, default=1, help="Drop NO_TICKS/NO_PATH labels")
    ap.add_argument("--keep-scenario-raw", type=int, default=1, help="Keep raw scenario_v4 column")
    args = ap.parse_args()

    horizons = [h.strip() for h in args.horizons.split(",") if h.strip().isdigit()]
    if not horizons:
        raise SystemExit("no horizons")

    # Load TB labels by sid
    tb: dict[str, dict[str, Any]] = {}
    for obj in _read_ndjson(args.tb):
        o = _get_payload(obj)
        sid = (o.get("sid", "") or "")
        if sid:
            tb[sid] = o

    rows: list[dict[str, Any]] = []
    miss = 0
    dropped = 0

    # Join inputs with TB labels
    for obj in _read_ndjson(args.inputs):
        o = _get_payload(obj)
        sid = (o.get("sid", "") or "")
        if not sid:
            continue
        t = tb.get(sid)
        if not t:
            miss += 1
            continue

        horizons_map = t.get("horizons") if isinstance(t.get("horizons"), dict) else {}
        meta = t.get("meta") if isinstance(t.get("meta"), dict) else {}
        exec_cost_r = float(meta.get("exec_cost_r", 0.0) or 0.0)

        # Primary label for drop rule
        primary = t.get("primary") if isinstance(t.get("primary"), dict) else {}
        label0 = (primary.get("label", "") or "")
        if int(args.drop_no_ticks) == 1 and label0 in ("NO_TICKS", "NO_PATH", ""):
            dropped += 1
            continue

        # Features: check 'indicators' nested dict or root level
        ind = o.get("indicators")
        if not isinstance(ind, dict):
            ind = o

        # Explicit mappings for root-level fields used in some producers
        if "sweep_recent" in ind and "sweep" not in ind:
            ind["sweep"] = ind["sweep_recent"]
        if "reclaim_recent" in ind and "reclaim" not in ind:
            ind["reclaim"] = ind["reclaim_recent"]
        if "obi_stable" in ind and "obi_stability_score" not in ind:
            ind["obi_stability_score"] = ind["obi_stable"]

        scenario = (o.get("scenario_v4", o.get("scenario", "")) or "")
        direction = (o.get("direction", "") or "")

        r: dict[str, Any] = {
            "sid": sid,
            "ts_ms": int(o.get("ts_ms", o.get("ts", 0)) or 0),
            "symbol": (o.get("symbol", "") or ""),
            "direction": direction,
        }
        if int(args.keep_scenario_raw) == 1:
            r["scenario_v4"] = scenario

        # Features
        r.update(build_feat(ind))

        # Utility targets per horizon
        for h in horizons:
            hh = horizons_map.get(h, {}) if isinstance(horizons_map, dict) else {}
            lbl = (hh.get("label", "") or "")
            if lbl in ("NO_TICKS", "NO_PATH", ""):
                r[f"y_util_pos_{h}"] = 0
                r[f"util_r_{h}"] = 0.0
                r[f"y_edge_{h}"] = 0
            else:
                util_r = float(hh.get("r_mult", 0.0) or 0.0) - exec_cost_r
                r[f"util_r_{h}"] = util_r
                r[f"y_util_pos_{h}"] = 1 if util_r > 0.0 else 0
                r[f"y_edge_{h}"] = int(hh.get("y_edge", 0) or 0)

        rows.append(r)

    df = pd.DataFrame(rows)
    # One-hot direction for training stability
    if "direction" in df.columns:
        df = pd.concat([df.drop(columns=["direction"]), pd.get_dummies(df["direction"].fillna(""), prefix="direction")], axis=1)

    # Scenario one-hot only if raw kept
    if "scenario_v4" in df.columns:
        df = pd.concat([df, pd.get_dummies(df["scenario_v4"].fillna(""), prefix="scenario_v4")], axis=1)

    df.to_parquet(args.out, index=False)

    summary = {
        "inputs_rows": len(rows) + miss + dropped,
        "joined_rows": len(rows),
        "missing_tb": miss,
        "dropped_no_ticks": dropped,
        "cols": list(df.columns),
    }
    with open(args.out + ".json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

