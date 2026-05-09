from utils.time_utils import get_ny_time_millis

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
train_confidence_calibrator_v2.py

Trains confidence calibration models (Platt, Isotonic, Beta, Identity) on labeled signal data.
Supports granular bucketing (Symbol, Session, Regime).

Input: 
  - File: JSONL with keys y, confidence, symbol, session, regime
  - DB: PostgreSQL (signal_performance)

Output: JSON with calibration parameters per bucket.
"""

import argparse
import json
import logging
import os
import time
from typing import Any

import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
import contextlib

# Check if we can import session_utc
try:
    from services.orderflow.utils import session_utc
except ImportError:
    # Minimal fallback if not in path
    def session_utc(ts_ms: int) -> str:
        h = int((ts_ms / 1000 / 3600) % 24)
        if 0 <= h < 8: return "Asia"
        if 8 <= h < 16: return "London"
        return "NY"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("conf_calib_v2")

def now_ms() -> int:
    return get_ny_time_millis()

def _get(d: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d: return d[k]
    # search indicators if dict
    ind = d.get("indicators")
    if isinstance(ind, dict):
        for k in keys:
            if k in ind: return ind[k]
    return None

def _bucket_key(row: dict[str, Any]) -> str:
    sym = str(_get(row, "symbol", "sym") or "global").strip().upper()
    sess = str(_get(row, "session", "sess") or "any").strip().lower()
    reg = str(_get(row, "regime", "market_mode", "liq_regime") or "any").strip().lower()
    return f"{sym}|{sess}|{reg}"

class Calibrator:
    def fit(self, probs: np.ndarray, labels: np.ndarray) -> dict[str, Any]:
        raise NotImplementedError

class IdentityCalibrator(Calibrator):
    def fit(self, probs: np.ndarray, labels: np.ndarray) -> dict[str, Any]:
        return {"method": "identity"}

class PlattCalibrator(Calibrator):
    def fit(self, probs: np.ndarray, labels: np.ndarray) -> dict[str, Any]:
        # Platt/Sigmoid scaling: LogReg on log-odds
        # To avoid log(0), clip probs
        eps = 1e-6
        p_clipped = np.clip(probs, eps, 1 - eps)
        log_odds = np.log(p_clipped / (1 - p_clipped)).reshape(-1, 1)

        lr = LogisticRegression(C=1e5, solver='lbfgs')
        lr.fit(log_odds, labels)

        return {
            "method": "platt",
            "slope": float(lr.coef_[0][0]),
            "intercept": float(lr.intercept_[0])
        }

class IsotonicCalibrator(Calibrator):
    def fit(self, probs: np.ndarray, labels: np.ndarray) -> dict[str, Any]:
        iso = IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1)
        # Isotonic expects sorted X usually, but fit handles it?
        # Actually sklearn handles it.
        iso.fit(probs, labels)

        # Serialize boundaries and values
        return {
            "method": "isotonic",
            "x_min": float(iso.X_min_),
            "x_max": float(iso.X_max_),
            "f_x": [float(x) for x in iso.f_.x],
            "f_y": [float(y) for y in iso.f_.y]
        }

class BetaCalibratorResult(Calibrator):
     def fit(self, probs: np.ndarray, labels: np.ndarray) -> dict[str, Any]:
        # Placeholder: Beta calibration often reduces to Platt-like on log-log scales.
        # We will use Platt as fallback for robustness in this draft.
        logger.warning("Beta calibration requested but simplified to Platt for stability.")
        pc = PlattCalibrator()
        res = pc.fit(probs, labels)
        res["method"] = "beta_simplified"
        return res

FACTORIES = {
    "identity": IdentityCalibrator,
    "platt": PlattCalibrator,
    "isotonic": IsotonicCalibrator,
    "beta": BetaCalibratorResult,
}

def load_data_jsonl(path: str) -> list[dict[str, Any]]:
    data = []
    with open(path) as f:
        for line in f:
            if not line.strip(): continue
            with contextlib.suppress(Exception):
                data.append(json.loads(line))
    return data

def load_data_db(dsn: str, since_days: int) -> list[dict[str, Any]]:
    # Connect and fetch
    logger.info(f"Connecting to DB dsn=... len={len(dsn)}...")
    conn = psycopg2.connect(dsn)
    data = []
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Fetch extra to get indicators -> regime
        # signal_performance has: ts_signal, symbol, final_score, outcome, realized_R, extra
        sql = """
        SELECT 
            EXTRACT(EPOCH FROM ts_signal) * 1000 as ts_ms,
            symbol,
            final_score as confidence,
            outcome,
            "realized_R",
            extra
        FROM signal_performance
        WHERE ts_signal > NOW() - INTERVAL '%s days'
          AND final_score IS NOT NULL
        """
        cur.execute(sql, (since_days,))
        rows = cur.fetchall()
        logger.info(f"Fetched {len(rows)} rows from DB")

        for r in rows:
            # Parse label
            label = None
            out = str(r["outcome"] or "")
            rr = float(r["realized_R"]) if r["realized_R"] is not None else None

            if out == "target_hit": label = 1
            elif out == "stop_hit": label = 0
            elif out in ("manual_exit", "expired_no_target", "breakeven"):
                if rr is not None: label = 1 if rr > 0 else 0
                else: label = 0

            if label is None:
                continue

            # Parse extra for regime
            regime = "any"
            extra = r.get("extra")
            if isinstance(extra, dict):
                # check indicators
                # extra often contains "indicators" key if it was dumped via json
                ind = extra.get("indicators")
                if isinstance(ind, dict):
                    regime = str(ind.get("market_mode") or ind.get("liq_regime") or "any")
                elif "market_mode" in extra:
                    regime = str(extra["market_mode"])

            # Session
            ts_ms = int(r["ts_ms"])
            sess = session_utc(ts_ms)

            data.append({
                "y": label,
                "confidence": float(r["confidence"]),
                "symbol": r["symbol"],
                "session": sess,
                "regime": regime
            })

    finally:
        conn.close()

    return data

def run_once(args):
    start_ts = now_ms()
    logger.info(f"Starting run (method={args.method})...")

    if args.dsn:
        rows = load_data_db(args.dsn, args.days)
    elif args.data_jsonl:
        rows = load_data_jsonl(args.data_jsonl)
    else:
        logger.error("Must provide --dsn or --data_jsonl")
        return

    logger.info(f"Processing {len(rows)} labeled samples.")

    # Group by bucket
    buckets: dict[str, list[tuple[float, int]]] = {}

    for r in rows:
        y = _get(r, "y", "label", "success")
        p = _get(r, "confidence", "conf", "score")

        if y is None or p is None:
            continue

        try:
            val_y = int(y),
            val_p = float(p),
            # if 0/1 labels only
            if val_y not in (0, 1): continue
        except Exception:
            continue

        bk = _bucket_key(r),
        if bk not in buckets: buckets[bk] = [],
        buckets[bk].append((val_p, val_y)),

        # Global
        if "GLOBAL" not in buckets: buckets["GLOBAL"] = [],
        buckets["GLOBAL"].append((val_p, val_y)),

    # Train
    out_map = {
        "meta": {
            "method": args.method,
            "train_ts": now_ms(),
            "source": "db" if args.dsn else args.data_jsonl,
            "rows": len(rows),
        },
        "calibrations": {}
    }

    cal_cls = FACTORIES[args.method]
    cal_inst = cal_cls()

    count_ok = 0
    for bk, items in buckets.items():
        if len(items) < args.min_bucket_n:
            continue

        probs = np.array([x[0] for x in items])
        labels = np.array([x[1] for x in items])

        if len(np.unique(labels)) < 2:
            continue

        try:
            res = cal_inst.fit(probs, labels)
            res["n"] = len(items)
            res["mean_prob"] = float(np.mean(probs))
            res["mean_y"] = float(np.mean(labels))
            out_map["calibrations"][bk] = res
            count_ok += 1
        except Exception as e:
            logger.error(f"Failed to calibrate {bk}: {e}")

    # Write output
    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)

    # Atomic write
    tmp_path = args.out_json + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(out_map, f, indent=2)
    os.replace(tmp_path, args.out_json)

    logger.info(f"Wrote {count_ok} calibrations to {args.out_json} (took {(now_ms() - start_ts)/1000.0:.3f}s)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_jsonl", help="Input JSONL file")
    ap.add_argument("--dsn", help="PostgreSQL DSN (PERF_PG_DSN)")
    ap.add_argument("--out_json", required=True, help="Output JSON path")
    ap.add_argument("--method", choices=FACTORIES.keys(), default="platt")
    ap.add_argument("--min_bucket_n", type=int, default=50)
    ap.add_argument("--days", type=int, default=30, help="Days of history to fetch from DB")
    ap.add_argument("--loop", action="store_true", help="Run in a loop")
    ap.add_argument("--interval", type=int, default=3600, help="Loop interval seconds")

    args = ap.parse_args()

    # ENV fallback
    if not args.dsn and os.getenv("ANALYTICS_DB_DSN"):
        args.dsn = os.getenv("ANALYTICS_DB_DSN")

    if args.loop:
        logger.info(f"Starting loop mode (interval={args.interval}s)...")
        while True:
            try:
                run_once(args)
            except Exception as e:
                logger.error(f"Error in loop: {e}", exc_info=True)
            time.sleep(args.interval)
    else:
        run_once(args)

if __name__ == "__main__":
    main()
