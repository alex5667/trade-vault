from __future__ import annotations

"""
Calculates recent ML metrics (Precision@TopK, ECE, Expectancy) for the last N hours.
Used by nightly_meta_enforce_ramp_bundle.py to gate ramp-up.

Usage:
    import ml_calculate_recent_metrics
    stats = ml_calculate_recent_metrics.calculate(window_hours=24)
"""

import argparse
import json
import logging
import os
from typing import Any

import numpy as np
import redis

# Import labeling logic
from tools.label_triple_barrier_from_redis_ticks_v10 import label_one, load_ticks_for_symbol
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("ml_calculate_metrics")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None: return d
        return float(x)
    except Exception:
        return d


def _ece(y_true: np.ndarray, p: np.ndarray, n_bins: int = 20) -> float:
    if len(y_true) == 0:
        return 0.0
    y = y_true.astype(float)
    p = np.clip(p.astype(float), 1e-9, 1 - 1e-9)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        m = (p >= lo) & (p < hi) if i < n_bins - 1 else (p >= lo) & (p <= hi)
        if not np.any(m):
            continue
        conf = float(np.mean(p[m]))
        acc = float(np.mean(y[m]))
        w = float(np.mean(m))
        ece += w * abs(acc - conf)
    return float(ece)


def calculate(
    redis_url: str | None = None,
    ticks_redis_url: str | None = None,
    window_hours: float = 24.0,
    top_k_pct: float = 0.05,
    min_samples: int = 100
) -> dict[str, Any]:
    """
    Fetches recent predictions, labels them via ticks, and computes metrics.
    Returns dict with keys: precision_top_k, ece, expectancy, n_samples.
    """
    r_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    t_url = ticks_redis_url or os.getenv("TICKS_REDIS_URL", r_url)

    r = redis.Redis.from_url(r_url, decode_responses=True)
    r_ticks = redis.Redis.from_url(t_url, decode_responses=True)

    stream = os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm")

    now_ms = get_ny_time_millis()
    start_ms = now_ms - int(window_hours * 3600 * 1000)

    # 1. Fetch relevant predictions from metrics stream
    # We need: symbol, ts_ms, direction (if available), p_edge, entry_px (maybe)
    # Note: metrics:ml_confirm might not have direction/entry_px.
    # If direction is missing, we can't label.
    # Let's check if direction is in metrics:ml_confirm.
    # Based on check_ml_confirm_metrics.py, keys include 'direction'.

    rows = []
    # Read stream from start_ms
    start_id = f"{start_ms}-0"

    # Read in batches
    cur_id = start_id
    while True:
        batch = r.xrange(stream, min=cur_id, max="+", count=1000)
        if not batch:
            break

        for xid, fields in batch:
            cur_id = xid

            # Filter for valid predictions
            if _i(fields.get("ok_rule"), 0) != 1:
                continue # Only interested in valid signals

            sym = (fields.get("symbol", "")).upper()
            d = (fields.get("direction", "")).upper()
            ts = _i(fields.get("ts_ms"), 0)
            p = _f(fields.get("p_edge"), 0.0)

            if not sym or not d or ts <= 0 or p <= 0:
                continue

            rows.append({
                "symbol": sym,
                "ts_ms": ts,
                "direction": d,
                "p_edge": p,
                # We need entry_px. metrics stream might not have it.
                # If not present, we will pick from ticks (mid/last) at ts_ms
                "entry_px": _f(fields.get("entry_px"), 0.0),

                # Indicators for TP/SL inference
                # metrics stream usually doesn't have full indicators.
                # We might need to use fallbacks if not present.
                # Or assume standard TP/SL if we can't find them.
                # For valid evaluation, we should ideally use the *actual* TP/SL used.
                # But if that info is lost, we use standard params.
                "indicators": {
                    "stop_bps": _f(fields.get("stop_bps"), 0.0),
                    "atr_bps": _f(fields.get("atr_bps"), 0.0)
                }
            })

        if len(batch) < 1000:
            break

        # increment cur_id
        try:
            ts_p, seq_p = cur_id.split("-")
            cur_id = f"{ts_p}-{int(seq_p) + 1}"
        except Exception:
            break

    if len(rows) < min_samples:
        logger.warning(f"Insufficient samples: {len(rows)} < {min_samples}")
        return {
            "n": len(rows),
            "precision_top_k": 0.0,
            "ece": 0.0,
            "expectancy": 0.0,
            "insufficient_data": True
        }

    # 2. Fetch ticks and label
    # Group by symbol
    by_sym: dict[str, list[dict]] = {}
    for row in rows:
        by_sym.setdefault(row["symbol"], []).append(row)

    labeled_rows = []

    # Params for labeling (standard v10 params)
    h_ms = 180_000 # 3m
    tp_k = 1.0
    sl_k = 1.0
    fallback_bps = 30.0

    for sym, sym_rows in by_sym.items():
        ts_vals = [r["ts_ms"] for r in sym_rows]
        ts_min = min(ts_vals)
        ts_max = max(ts_vals)

        # Load ticks
        # We need ticks from [min - small, max + horizon]
        t_start = max(start_ms, ts_min - 5000)
        t_end = ts_max + h_ms + 5000
        stream_key = f"stream:tick_{sym}"

        ticks = load_ticks_for_symbol(
            r_ticks,
            stream=stream_key,
            start_ms=t_start,
            end_ms=t_end,
            max_rows=200_000
        )

        for row in sym_rows:
            # We construct a minimal input dict for label_one
            inp = {
                "symbol": row["symbol"],
                "ts_ms": row["ts_ms"],
                "direction": row["direction"],
                "entry_px": row["entry_px"],
                "indicators": row["indicators"]
            }

            res = label_one(
                inp,
                ticks,
                h_ms=h_ms,
                tp_k_atr=tp_k,
                sl_k_atr=sl_k,
                fallback_tp_bps=fallback_bps,
                fallback_sl_bps=fallback_bps
            )

            row["tb_label"] = res["tb_label"]
            row["tb_r_mult"] = res["tb_r_mult"]
            row["tb_y_edge"] = res["tb_y_edge"]

            # If entry_px was missing and we found it in ticks, keep it?
            # Actually label_one handles it.

            if row["tb_label"] != "NO_TICKS":
                labeled_rows.append(row)

    if len(labeled_rows) < min_samples:
        return {
            "n": len(labeled_rows),
            "precision_top_k": 0.0,
            "ece": 1.0,
            "expectancy": 0.0,
            "insufficient_data": True
        }

    # 3. Compute Metrics

    # Sort by p_edge desc
    labeled_rows.sort(key=lambda x: x["p_edge"], reverse=True)

    # Top K%
    n_top = max(1, int(len(labeled_rows) * top_k_pct))
    top_k_rows = labeled_rows[:n_top]

    tp_count = sum(1 for r in top_k_rows if r["tb_y_edge"] == 1)
    precision_top_k = tp_count / n_top

    # ECE (on all data)
    y_true = np.array([r["tb_y_edge"] for r in labeled_rows])
    p_pred = np.array([r["p_edge"] for r in labeled_rows])
    ece_val = _ece(y_true, p_pred)

    # Expectancy (avg R-mult)
    # We should probably look at Top K expectancy too, but global is safer proxy for general quality
    r_mults = [r["tb_r_mult"] for r in top_k_rows] # Expectancy of the "traded" portion
    expectancy = sum(r_mults) / len(r_mults) if r_mults else 0.0

    return {
        "n": len(labeled_rows),
        "precision_top_k": precision_top_k,
        "ece": ece_val,
        "expectancy": expectancy,
        "insufficient_data": False
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=float, default=24.0)
    ap.add_argument("--top-k", type=float, default=0.05)
    args = ap.parse_args()

    stats = calculate(window_hours=args.window, top_k_pct=args.top_k)
    print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    main()
