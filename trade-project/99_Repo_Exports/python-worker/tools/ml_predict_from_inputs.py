#!/usr/bin/env python3
from __future__ import annotations

"""Predict p_edge on signals:of:inputs NDJSON.

Used for Golden Replay (Step 7): baseline vs new model distributions.
"""


import argparse
import json
import math
from collections.abc import Iterator
from typing import Any

import joblib


def _read_ndjson(path: str) -> Iterator[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
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


def _flatten(inp: dict[str, Any]) -> dict[str, Any]:
    X: dict[str, Any] = {}
    for k, v in inp.items():
        if k in ("cfg", "raw_ctx", "context", "payload"):
            continue
        if v is None:
            continue
        if isinstance(v, bool):
            X[k] = int(v)
        elif isinstance(v, (int, float)) or isinstance(v, str) and len(v) <= 64:
            X[k] = v
        elif isinstance(v, dict):
            for kk, vv in v.items():
                if vv is None:
                    continue
                nk = f"{k}.{kk}"
                if isinstance(vv, bool):
                    X[nk] = int(vv)
                elif isinstance(vv, (int, float)) or isinstance(vv, str) and len(vv) <= 64:
                    X[nk] = vv
    if "symbol" in inp:
        X["symbol"] = (inp.get("symbol")).upper()
    if "direction" in inp:
        X["direction"] = (inp.get("direction")).upper()
    if "scenario_v4" in inp:
        X["scenario_v4"] = (inp.get("scenario_v4"))
    elif "scenario" in inp:
        X["scenario_v4"] = (inp.get("scenario"))
    if "regime_group" in inp:
        X["regime_group"] = (inp.get("regime_group"))
    elif "regime" in inp:
        X["regime_group"] = (inp.get("regime"))
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
            sid = (inp.get("sid", "") or "")
            ts_ms = int(inp.get("ts_ms", inp.get("ts", 0)) or 0)
            sym = (inp.get("symbol", "") or "").upper()
            X = _flatten(inp)
            A = vec.transform([X])
            p = float(clf.predict_proba(A)[:, 1][0])
            allow = int(p >= float(args.p_min))
            rec = {"ts_ms": ts_ms, "sid": sid, "symbol": sym, "p_edge": float(p), "thr": float(args.p_min), "allow": allow}
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()

