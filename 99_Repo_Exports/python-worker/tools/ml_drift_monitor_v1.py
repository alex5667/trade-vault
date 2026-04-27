#!/usr/bin/env python3
"""ML drift monitor v1.

Reads:
  - metrics:ml_confirm stream (predictions/telemetry; expects JSON in field 'payload')

  - trades:closed stream (outcomes; supports either JSON payload or flat fields)

Computes:
  - PSI + KS drift for p_edge_cal (and exec_risk_norm if present) per bucket

  - Reliability drift for p_edge_cal via binned calibration (ECE + per-bin deltas)

Outputs JSON to stdout. Optionally emits to Redis stream metrics:ml_drift.

Environment:
  REDIS_URL (default redis://localhost:6379/0)

  ML_DRIFT_STREAM_PRED (default metrics:ml_confirm)

  ML_DRIFT_STREAM_TRADES (default trades:closed)

  ML_DRIFT_STREAM_OUT (default metrics:ml_drift)

"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import asyncio
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import redis.asyncio as redis


def _now_ms() -> int:
    return get_ny_time_millis()


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return v
    except Exception:
        return float(default)


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _bucket_from_payload(p: Dict[str, Any]) -> str:
    b = str(p.get("bucket") or p.get("regime_group") or p.get("regime") or "").lower()
    if "range" in b or "chop" in b or "meanrev" in b:
        return "range"
    if "trend" in b:
        return "trend"
    return "other"


def _parse_stream_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    if "payload" in fields:
        try:
            return json.loads(fields.get("payload") or "{}") or {}
        except Exception:
            return {}
    # flat row
    return dict(fields)


def ks_stat(a: List[float], b: List[float]) -> float:
    """Two-sample KS statistic (no p-value)."""
    if not a or not b:
        return 0.0
    a_sorted = sorted(a)
    b_sorted = sorted(b)
    ia = ib = 0
    na = len(a_sorted)
    nb = len(b_sorted)
    d = 0.0
    # merge-walk
    while ia < na and ib < nb:
        va = a_sorted[ia]
        vb = b_sorted[ib]
        if va <= vb:
            ia += 1
        else:
            ib += 1
        fa = ia / na
        fb = ib / nb
        d = max(d, abs(fa - fb))
    return float(d)


def psi(a: List[float], b: List[float], *, bins: int = 10) -> float:
    """Population Stability Index with bins derived from baseline quantiles."""
    if not a or not b:
        return 0.0
    a_sorted = sorted(a)
    # quantile cutpoints
    cuts: List[float] = []
    for k in range(1, bins):
        q_idx = int(round((k / bins) * (len(a_sorted) - 1)))
        cuts.append(a_sorted[q_idx])
    # build hist
    def hist(xs: List[float]) -> List[int]:
        h = [0] * bins
        for v in xs:
            j = 0
            while j < len(cuts) and v > cuts[j]:
                j += 1
            h[j] += 1
        return h

    ha = hist(a)
    hb = hist(b)
    na = sum(ha)
    nb = sum(hb)
    out = 0.0
    eps = 1e-9
    for ca, cb in zip(ha, hb):
        pa = max(eps, ca / max(1, na))
        pb = max(eps, cb / max(1, nb))
        out += (pb - pa) * math.log(pb / pa)
    return float(out)


def calibration_bins(p: List[float], y: List[int], *, n_bins: int = 10) -> Dict[str, Any]:
    """Return per-bin calibration stats and ECE."""
    assert len(p) == len(y)
    bins: List[Dict[str, Any]] = []
    ece = 0.0
    n = len(p)
    if n == 0:
        return {"n": 0, "ece": 0.0, "bins": []}

    for bi in range(n_bins):
        lo = bi / n_bins
        hi = (bi + 1) / n_bins
        idx = [i for i, pv in enumerate(p) if (pv >= lo and (pv < hi if bi < n_bins - 1 else pv <= hi))]
        if not idx:
            bins.append({"lo": lo, "hi": hi, "n": 0, "p_mean": 0.0, "y_rate": 0.0, "abs_gap": 0.0})
            continue
        p_mean = sum(p[i] for i in idx) / len(idx)
        y_rate = sum(y[i] for i in idx) / len(idx)
        gap = abs(p_mean - y_rate)
        ece += (len(idx) / n) * gap
        bins.append({"lo": lo, "hi": hi, "n": len(idx), "p_mean": p_mean, "y_rate": y_rate, "abs_gap": gap})

    return {"n": n, "ece": float(ece), "bins": bins}


async def _read_stream_window(
    r: redis.Redis,
    stream: str,
    *,
    min_ts_ms: int,
    max_scan: int,
) -> List[Dict[str, Any]]:
    """Read newest->oldest, stop when ts_ms < min_ts_ms or scan limit reached."""
    out: List[Dict[str, Any]] = []
    # Read chunks from the tail using XREVRANGE
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        chunk = await r.xrevrange(stream, max=last_id, min="-", count=min(2000, max_scan - scanned))
        if not chunk:
            break
        if len(chunk) == 1 and chunk[0][0] == last_id:
            break
        for msg_id, fields in chunk:
            scanned += 1
            row = _parse_stream_row(fields)
            ts = _i(row.get("ts_ms", 0))
            if ts < min_ts_ms:
                return out
            out.append(row)
            last_id = msg_id
        # move cursor (exclusive): add a tiny suffix to step back
        if last_id == "0-0":
            break
        last_id = f"({last_id}"

    return out


def _join_by_sid(pred_rows: List[Dict[str, Any]], trade_rows: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    trades_by_sid: Dict[str, Dict[str, Any]] = {}
    for tr in trade_rows:
        sid = str(tr.get("sid") or "").strip()
        if not sid:
            continue
        trades_by_sid[sid] = tr
    out: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for pr in pred_rows:
        sid = str(pr.get("sid") or "").strip()
        if not sid:
            continue
        tr = trades_by_sid.get(sid)
        if tr is None:
            continue
        out.append((pr, tr))
    return out


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-hours", type=float, default=float(os.getenv("ML_DRIFT_WINDOW_HOURS", "6")))
    ap.add_argument("--baseline-hours", type=float, default=float(os.getenv("ML_DRIFT_BASELINE_HOURS", "168")))
    ap.add_argument("--max-scan", type=int, default=int(os.getenv("ML_DRIFT_MAX_SCAN", "40000")))
    ap.add_argument("--emit", action="store_true", help="Emit summary to Redis stream (metrics:ml_drift).")
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    stream_pred = os.getenv("ML_DRIFT_STREAM_PRED", "metrics:ml_confirm")
    stream_trades = os.getenv("ML_DRIFT_STREAM_TRADES", "trades:closed")
    stream_out = os.getenv("ML_DRIFT_STREAM_OUT", "metrics:ml_drift")

    now = _now_ms()
    win_ms = int(args.window_hours * 3600 * 1000)
    base_ms = int(args.baseline_hours * 3600 * 1000)

    cur_min = now - win_ms
    base_min = now - base_ms

    r = redis.from_url(redis_url, decode_responses=True)
    try:
        pred_all = await _read_stream_window(r, stream_pred, min_ts_ms=base_min, max_scan=args.max_scan)
        tr_all = await _read_stream_window(r, stream_trades, min_ts_ms=base_min, max_scan=args.max_scan)
    finally:
        await r.aclose()

    pred_cur = [p for p in pred_all if _i(p.get("ts_ms", 0)) >= cur_min]
    pred_base = [p for p in pred_all if base_min <= _i(p.get("ts_ms", 0)) < cur_min]
    tr_cur = [t for t in tr_all if _i(t.get("ts_ms", 0)) >= cur_min]
    tr_base = [t for t in tr_all if base_min <= _i(t.get("ts_ms", 0)) < cur_min]

    # Join for reliability (needs outcomes)
    joined_cur = _join_by_sid(pred_cur, tr_cur)
    joined_base = _join_by_sid(pred_base, tr_base)

    # per-bucket distributions
    buckets = ["trend", "range", "other"]
    out: Dict[str, Any] = {
        "ts_ms": now,
        "window_hours": args.window_hours,
        "baseline_hours": args.baseline_hours,
        "streams": {"pred": stream_pred, "trades": stream_trades},
        "counts": {
            "pred_cur": len(pred_cur),
            "pred_base": len(pred_base),
            "trades_cur": len(tr_cur),
            "trades_base": len(tr_base),
            "joined_cur": len(joined_cur),
            "joined_base": len(joined_base),
        },
        "by_bucket": {},
    }

    for b in buckets:
        # p_edge_cal
        cur_p = [_f(p.get("p_edge_cal", p.get("p_edge", 0.0)), 0.0) for p in pred_cur if _bucket_from_payload(p) == b]
        base_p = [_f(p.get("p_edge_cal", p.get("p_edge", 0.0)), 0.0) for p in pred_base if _bucket_from_payload(p) == b]
        cur_exec = [_f(p.get("exec_risk_norm", 0.0), 0.0) for p in pred_cur if _bucket_from_payload(p) == b and "exec_risk_norm" in p]
        base_exec = [_f(p.get("exec_risk_norm", 0.0), 0.0) for p in pred_base if _bucket_from_payload(p) == b and "exec_risk_norm" in p]

        joined_cur_b = [(p, t) for (p, t) in joined_cur if _bucket_from_payload(p) == b]
        joined_base_b = [(p, t) for (p, t) in joined_base if _bucket_from_payload(p) == b]

        # label: r_mult > 0
        cur_y = [1 if _f(t.get("r_mult", 0.0), 0.0) > 0.0 else 0 for (p, t) in joined_cur_b]
        cur_pp = [_f(p.get("p_edge_cal", p.get("p_edge", 0.0)), 0.0) for (p, t) in joined_cur_b]

        base_y = [1 if _f(t.get("r_mult", 0.0), 0.0) > 0.0 else 0 for (p, t) in joined_base_b]
        base_pp = [_f(p.get("p_edge_cal", p.get("p_edge", 0.0)), 0.0) for (p, t) in joined_base_b]

        out["by_bucket"][b] = {
            "n_pred_cur": len(cur_p),
            "n_pred_base": len(base_p),
            "p_edge_cal": {
                "psi": psi(base_p, cur_p) if base_p and cur_p else 0.0,
                "ks": ks_stat(base_p, cur_p) if base_p and cur_p else 0.0,
                "cur_mean": sum(cur_p) / len(cur_p) if cur_p else 0.0,
                "base_mean": sum(base_p) / len(base_p) if base_p else 0.0,
            },
            "exec_risk_norm": {
                "psi": psi(base_exec, cur_exec) if base_exec and cur_exec else 0.0,
                "ks": ks_stat(base_exec, cur_exec) if base_exec and cur_exec else 0.0,
                "cur_mean": sum(cur_exec) / len(cur_exec) if cur_exec else 0.0,
                "base_mean": sum(base_exec) / len(base_exec) if base_exec else 0.0,
                "present": bool(cur_exec or base_exec),
            },
            "reliability": {
                "cur": calibration_bins(cur_pp, cur_y, n_bins=10) if cur_pp else {"n": 0, "ece": 0.0, "bins": []},
                "base": calibration_bins(base_pp, base_y, n_bins=10) if base_pp else {"n": 0, "ece": 0.0, "bins": []},
            },
        }
        out["by_bucket"][b]["reliability"]["ece_drift"] = float(
            out["by_bucket"][b]["reliability"]["cur"]["ece"] - out["by_bucket"][b]["reliability"]["base"]["ece"]
        )

    print(json.dumps(out, ensure_ascii=False))

    if args.emit:
        r2 = redis.from_url(redis_url, decode_responses=True)
        try:
            await r2.xadd(stream_out, {"payload": json.dumps(out, ensure_ascii=False)}, maxlen=2000, approximate=True)
        finally:
            await r2.aclose()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
