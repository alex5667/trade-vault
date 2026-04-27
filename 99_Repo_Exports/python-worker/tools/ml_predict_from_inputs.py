#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Predict p_edge on signals:of:inputs NDJSON.

Used for Golden Replay (Step 7): baseline vs new model distributions.
"""

from __future__ import annotations

import argparse
import json
import math
from typing import Any, Dict, Iterator

import joblib


def _read_ndjson(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except Exception:
                continue


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def _flatten(inp: Dict[str, Any]) -> Dict[str, Any]:
    X: Dict[str, Any] = {}
    for k, v in inp.items():
        if k in ("cfg", "raw_ctx", "context", "payload"):
            continue
        if v is None:
            continue
        if isinstance(v, bool):
            X[k] = int(v)
        elif isinstance(v, (int, float)):
            X[k] = v
        elif isinstance(v, str) and len(v) <= 64:
            X[k] = v
        elif isinstance(v, dict):
            for kk, vv in v.items():
                if vv is None:
                    continue
                nk = f"{k}.{kk}"
                if isinstance(vv, bool):
                    X[nk] = int(vv)
                elif isinstance(vv, (int, float)):
                    X[nk] = vv
                elif isinstance(vv, str) and len(vv) <= 64:
                    X[nk] = vv
    if "symbol" in inp:
        X["symbol"] = str(inp.get("symbol")).upper()
    if "direction" in inp:
        X["direction"] = str(inp.get("direction")).upper()
    if "scenario_v4" in inp:
        X["scenario_v4"] = str(inp.get("scenario_v4"))
    elif "scenario" in inp:
        X["scenario_v4"] = str(inp.get("scenario"))
    if "regime_group" in inp:
        X["regime_group"] = str(inp.get("regime_group"))
    elif "regime" in inp:
        X["regime_group"] = str(inp.get("regime"))
    # exec_risk_norm best-effort
    if "exec_risk_norm" not in X:
        spread = _f(inp.get("spread_bps"), 0.0)
        slip = _f(inp.get("expected_slippage_bps"), 0.0)
        ref = _f(inp.get("exec_risk_ref_bps"), 10.0)
        X["exec_risk_norm"] = max(0.0, min(1.0, (spread + slip) / max(1e-9, ref)))
    return X


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True, help="joblib bundle (vectorizer+model)")
    ap.add_argument("--inputs", required=True, help="signals:of:inputs ndjson")
    ap.add_argument("--out", required=True)
    ap.add_argument("--p-min", type=float, default=0.55)
    args = ap.parse_args()

    bundle = joblib.load(args.model)
    vec = bundle["vectorizer"]
    clf = bundle["model"]

    with open(args.out, "w", encoding="utf-8") as f:
        for inp in _read_ndjson(args.inputs):
            sid = str(inp.get("sid", "") or "")
            ts_ms = int(inp.get("ts_ms", inp.get("ts", 0)) or 0)
            sym = str(inp.get("symbol", "") or "").upper()
            X = _flatten(inp)
            A = vec.transform([X])
            p = float(clf.predict_proba(A)[:, 1][0])
            allow = int(p >= float(args.p_min))
            rec = {"ts_ms": ts_ms, "sid": sid, "symbol": sym, "p_edge": float(p), "thr": float(args.p_min), "allow": allow}
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()

