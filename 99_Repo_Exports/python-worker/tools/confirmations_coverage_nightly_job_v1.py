from __future__ import annotations
"""Confirmations coverage nightly job (v1).

Goal: ensure confirmations (conf_*) are actually present and non-zero in the
offline dataset used for training/AB, and provide a low-cardinality JSON report
for SRE guardrails.

Inputs:
  - Parquet dataset (default: META_AB_DATASET_PARQUET)
Output:
  - JSON report (default: /var/lib/trade/of_reports/confirmations_coverage_report.json)

Exit codes:
  0 => report written (even if reasons present)
  2 => invalid args / unrecoverable IO (still tries to write report if possible)
"""

from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

CONF_COLS: List[str] = [
    "conf_rsi_agree",
    "conf_div_match",
    "conf_sweep_eqh",
    "conf_sweep_eql",
    "conf_sweep_any",
    "conf_iceberg_strict",
    "conf_obi_stable",
    "conf_reclaim",
    "conf_weak_progress",
]

RAW_COLS: List[str] = [
    "rsi_agree",
    "div_match",
    "sweep_eqh",
    "sweep_eql",
    "sweep_any",
    "iceberg_strict",
    "obi_stable",
    "reclaim",
    "weak_progress",
]


def _now_ms() -> int:
    return get_ny_time_millis()


def _write_json_atomic(path: str, obj: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _load_parquet(path: str):
    # Lazy import to keep CLI responsive on minimal images.
    import pandas as pd  # type: ignore

    # Prefer pyarrow when available for robustness.
    try:
        return pd.read_parquet(path, engine="pyarrow")
    except Exception:
        return pd.read_parquet(path)


def _col_stats(df, col: str) -> Dict[str, Any]:
    s = df[col]
    n = int(len(s))
    if n <= 0:
        return {"present": 1, "nonnull_rate": 0.0, "nonzero_rate": 0.0, "mean": 0.0}

    # numeric coercion
    try:
        sc = s.astype("float64")
    except Exception:
        sc = s.apply(lambda v: _safe_float(v, 0.0)).astype("float64")

    nn = int(sc.notnull().sum())
    sc2 = sc.fillna(0.0)

    # treat >0 as true
    nonzero = int((sc2 > 0.0).sum())
    mean = float(sc2.mean()) if n > 0 else 0.0

    return {
        "present": 1,
        "nonnull_rate": float(nn / n),
        "nonzero_rate": float(nonzero / n),
        "mean": float(mean),
    }


def build_report(
    dataset_path: str,
    min_rows: int,
    conf_min_nonzero_rate_warn: float,
) -> Dict[str, Any]:
    rep: Dict[str, Any] = {
        "ts_ms": _now_ms(),
        "dataset_path": dataset_path,
        "counts": {"n_rows": 0},
        "summary": {},
        "features": {},
        "reasons": [],
    }

    if not dataset_path or not os.path.exists(dataset_path):
        rep["reasons"].append("dataset_missing")
        return rep

    try:
        df = _load_parquet(dataset_path)
    except Exception as e:
        rep["reasons"].append("dataset_load_failed")
        rep["summary"]["error"] = str(e)[:500]
        return rep

    n_rows = int(len(df))
    rep["counts"]["n_rows"] = n_rows
    if n_rows < min_rows:
        rep["reasons"].append("n_rows_low")

    cols = set(df.columns)

    # stats for conf and raw cols (if exist)
    conf_present = 0
    conf_min_nonzero = 1.0
    conf_nonzero_rates: Dict[str, float] = {}

    for col in CONF_COLS + RAW_COLS:
        if col in cols:
            st = _col_stats(df, col)
            rep["features"][col] = st
        else:
            rep["features"][col] = {"present": 0, "nonnull_rate": 0.0, "nonzero_rate": 0.0, "mean": 0.0}

        if col in CONF_COLS and rep["features"][col]["present"] == 1:
            conf_present += 1
            r = float(rep["features"][col]["nonzero_rate"])
            conf_nonzero_rates[col] = r
            if r < conf_min_nonzero:
                conf_min_nonzero = r

    if conf_present == 0:
        rep["reasons"].append("conf_cols_missing")
        conf_min_nonzero = 0.0

    # derived "bad" criteria: all conf_* nearly always 0
    bad_all_zero = False
    if conf_present > 0:
        # if every conf_* has nonzero_rate == 0 (or below warn)
        bad_all_zero = all((conf_nonzero_rates.get(c, 0.0) <= 0.0) for c in CONF_COLS if c in conf_nonzero_rates)
        if bad_all_zero:
            rep["reasons"].append("conf_all_zero")
        elif conf_min_nonzero < conf_min_nonzero_rate_warn:
            rep["reasons"].append("conf_low_nonzero_rate")

    rep["summary"] = {
        "conf_present_cols": conf_present,
        "conf_total_cols": len(CONF_COLS),
        "conf_min_nonzero_rate": float(conf_min_nonzero),
        "conf_bad_all_zero": bool(bad_all_zero),
        "conf_min_nonzero_rate_warn": float(conf_min_nonzero_rate_warn),
    }
    return rep


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default=os.getenv("CONFIRMATIONS_COVERAGE_DATASET_PARQUET") or os.getenv("META_AB_DATASET_PARQUET") or "", help="Parquet dataset path")
    ap.add_argument("--out", default=os.getenv("CONFIRMATIONS_COVERAGE_OUT_JSON", "/var/lib/trade/of_reports/confirmations_coverage_report.json"), help="Output JSON path")
    ap.add_argument("--min-rows", type=int, default=int(os.getenv("CONFIRMATIONS_COVERAGE_MIN_ROWS", "1000")))
    ap.add_argument("--min-nonzero-warn", type=float, default=float(os.getenv("CONFIRMATIONS_COVERAGE_MIN_NONZERO_RATE_WARN", "0.005")))
    args = ap.parse_args()

    rep = build_report(args.dataset, args.min_rows, args.min_nonzero_warn)
    try:
        _write_json_atomic(args.out, rep)
    except Exception:
        # last resort (try non-atomic)
        try:
            os.makedirs(os.path.dirname(args.out), exist_ok=True)
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(rep, f, ensure_ascii=False, indent=2, sort_keys=True)
        except Exception:
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
