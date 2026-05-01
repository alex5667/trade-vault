#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""Build ML-confirm dataset from NDJSON exports.

Inputs
------
1) signals:of:inputs capture (NDJSON) : each line is JSON (OFInputsV1)
2) events:trades closed export (NDJSON) : each line is JSON (POSITION_CLOSED/CLOSE)

We join by `sid` (stable signal id).

Output
------
NDJSON lines, each:
{
  "ts_ms": ...,
  "sid": "...",
  "symbol": "...",
  "X": {...},        # dict features (numeric + categorical)
  "y_edge": 0/1,     # classification label
  "y_util": float,   # utility target (proxy)
  "meta": {...}      # diagnostic fields
}

This completes Step A (Outcome labels) without relying on a DB.
"""


import argparse
import json
import math
from typing import Any, Dict, Iterator, Optional


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


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _get_sid(obj: Dict[str, Any]) -> str:
    for k in ("sid", "signal_id", "id"):
        v = obj.get(k)
        if isinstance(v, str) and v:
            return v
    return ""


def _get_ts(obj: Dict[str, Any]) -> int:
    for k in ("ts_ms", "ts", "timestamp"):
        v = obj.get(k)
        if v is None:
            continue
        t = _i(v, 0)
        if t > 0:
            return t
    return 0


def _get_symbol(obj: Dict[str, Any]) -> str:
    v = obj.get("symbol")
    if isinstance(v, str):
        return v.upper()
    return ""


def _flatten_features(inp: Dict[str, Any]) -> Dict[str, Any]:
    """Turn OFInputsV1 into a flat feature dict.

    Rules:
      - keep numbers/bools/short strings
      - drop large blobs (cfg/raw_ctx) if present
    """
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
        elif isinstance(v, str):
            if len(v) <= 64:
                X[k] = v
        elif isinstance(v, dict):
            # flatten one level for evidence-like dicts
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
        # ignore lists/huge objects
    # ensure stable categoricals
    if "symbol" in inp:
        X["symbol"] = str(inp.get("symbol")).upper()
    if "direction" in inp:
        X["direction"] = str(inp.get("direction")).upper()
    # allow both scenario and scenario_v4 naming
    if "scenario_v4" in inp:
        X["scenario_v4"] = str(inp.get("scenario_v4"))
    if "scenario" in inp and "scenario_v4" not in X:
        X["scenario_v4"] = str(inp.get("scenario"))
    if "regime_group" in inp:
        X["regime_group"] = str(inp.get("regime_group"))
    if "regime" in inp and "regime_group" not in X:
        X["regime_group"] = str(inp.get("regime"))
    return X


def _extract_outcome(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize closed-trade row to a small set of outcome fields."""
    out: Dict[str, Any] = {}
    out["ts_ms"] = _get_ts(row)
    out["sid"] = _get_sid(row)
    out["symbol"] = _get_symbol(row)
    # R-multiple is the primary outcome in your system
    out["r_mult"] = _f(row.get("r_mult"), 0.0)
    out["pnl"] = _f(row.get("pnl"), _f(row.get("pnl_net"), 0.0))
    out["fees"] = _f(row.get("fees"), _f(row.get("fees_usd"), 0.0))
    out["slippage_bps_real"] = _f(row.get("slippage_bps_real"), _f(row.get("slippage_bps"), 0.0))
    out["spread_bps_at_entry"] = _f(row.get("spread_bps_at_entry"), _f(row.get("spread_bps"), 0.0))
    # adverse proxies (optional)
    out["mae_r"] = row.get("mae_r")
    out["mfe_r"] = row.get("mfe_r")
    out["mae_bps"] = row.get("mae_bps")
    out["mfe_bps"] = row.get("mfe_bps")
    return out


def _adverse_proxy(out: Dict[str, Any]) -> Optional[float]:
    # prefer MAE in R if present
    if out.get("mae_r") is not None:
        return _f(out.get("mae_r"), 0.0)
    if out.get("mae_bps") is not None:
        # scale to pseudo-R by /10000 (bps -> fraction) as a proxy
        return _f(out.get("mae_bps"), 0.0) / 10_000.0
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="ndjson captured from signals:of:inputs")
    ap.add_argument("--closed", required=True, help="ndjson exported from events:trades (POSITION_CLOSED/CLOSE)")
    ap.add_argument("--out", required=True, help="output ndjson path")
    ap.add_argument("--r-min", type=float, default=0.50, help="label edge: R_net >= r-min")
    ap.add_argument("--adv-max", type=float, default=1.00, help="label edge: adverse_proxy <= adv-max (if available)")
    ap.add_argument("--util-exec-pen", type=float, default=0.10, help="utility penalty weight for exec_risk_norm (proxy)")
    args = ap.parse_args()

    # Load outcomes keyed by sid
    outcomes: Dict[str, Dict[str, Any]] = {}
    for row in _read_ndjson(args.closed):
        o = _extract_outcome(row)
        sid = o.get("sid", "")
        if not sid:
            continue
        # keep latest by ts_ms
        if sid not in outcomes or _i(o.get("ts_ms"), 0) >= _i(outcomes[sid].get("ts_ms"), 0):
            outcomes[sid] = o

    written = 0
    skipped_no_outcome = 0
    with open(args.out, "w", encoding="utf-8") as f:
        for inp in _read_ndjson(args.inputs):
            sid = _get_sid(inp)
            if not sid:
                continue
            out = outcomes.get(sid)
            if out is None:
                skipped_no_outcome += 1
                continue

            X = _flatten_features(inp)
            # ensure we carry exec_risk_norm feature if present, else derive best-effort
            exec_risk_norm = _f(X.get("exec_risk_norm", 0.0), 0.0)
            if exec_risk_norm <= 0.0:
                spread_bps = _f(X.get("spread_bps", 0.0), 0.0)
                slip_bps = _f(X.get("expected_slippage_bps", 0.0), 0.0)
                ref = _f(X.get("exec_risk_ref_bps", 10.0), 10.0)
                exec_risk_norm = max(0.0, min(1.0, (spread_bps + slip_bps) / max(1e-9, ref)))
                X["exec_risk_norm"] = exec_risk_norm

            r_mult = _f(out.get("r_mult"), 0.0)
            adv = _adverse_proxy(out)

            y_edge = 1 if r_mult >= float(args.r_min) else 0
            if adv is not None and adv > float(args.adv_max):
                y_edge = 0

            # utility proxy: r_mult - penalty(exec)
            y_util = float(r_mult) - float(args.util_exec_pen) * float(exec_risk_norm)

            rec = {
                "ts_ms": _get_ts(inp) or _get_ts(out),
                "sid": sid,
                "symbol": _get_symbol(inp) or _get_symbol(out),
                "X": X,
                "y_edge": int(y_edge),
                "y_util": float(y_util),
                "meta": {
                    "r_mult": float(r_mult),
                    "adverse_proxy": (None if adv is None else float(adv)),
                    "exec_risk_norm": float(exec_risk_norm),
                },
            }
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            written += 1

    print(f"written={written} skipped_no_outcome={skipped_no_outcome} outcomes={len(outcomes)}")


if __name__ == "__main__":
    main()

