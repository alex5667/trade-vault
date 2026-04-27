from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Dict, List, Optional

from core.skew_stats import calculate_proportion_skew
from core.confirmations_schema_v1 import CONF_KEYS_V1


def iter_ndjson(path: str):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except Exception:
                continue


def collect_stats(path: str, keys: List[str]) -> Dict[str, Dict[str, float]]:
    """Collect sample size and positive count for each key."""
    counts = {k: 0 for k in keys}
    n = 0
    symbols = set()
    
    for row in iter_ndjson(path):
        n += 1
        symbols.add(row.get("symbol", "unknown"))
        for k in keys:
            # support both "conf_key" and "key"
            val = row.get(f"conf_{k}", row.get(k, 0))
            if float(val or 0) > 0:
                counts[k] += 1
                
    return {
        "n": n,
        "counts": counts,
        "symbols": list(symbols),
        "proportions": {k: (counts[k] / n if n > 0 else 0.0) for k in keys}
    }


def main():
    ap = argparse.ArgumentParser(description="Audit v7 confirmation feature skew between Train and Serve.")
    ap.add_argument("--train", required=True, help="Path to train-side NDJSON (from build_of_dataset.py)")
    ap.add_argument("--serve", required=True, help="Path to serve-side NDJSON (signals raw)")
    ap.add_argument("--alpha", type=float, default=0.01, help="Significance level (default 0.01)")
    ap.add_argument("--min-n", type=int, default=100, help="Minimum sample size for both sides")
    ap.add_argument("--out-prom", help="Path to write Prometheus textfile metrics")
    ap.add_argument("--out-json", help="Path to write machine-readable JSON report")
    args = ap.parse_args()

    print(f"--- Skew Audit v7 ---")
    print(f"Training data: {args.train}")
    print(f"Serving data:  {args.serve}")

    train_stats = collect_stats(args.train, list(CONF_KEYS_V1))
    serve_stats = collect_stats(args.serve, list(CONF_KEYS_V1))

    tn = train_stats["n"]
    sn = serve_stats["n"]

    print(f"Samples: Train={tn}, Serve={sn}")
    
    if tn < args.min_n or sn < args.min_n:
        print(f"ERROR: Insufficient data (min_n={args.min_n})")
        sys.exit(1)

    bad_features = []
    
    results = {}
    for k in CONF_KEYS_V1:
        tp = train_stats["proportions"][k]
        sp = serve_stats["proportions"][k]
        
        res = calculate_proportion_skew(tn, tp, sn, sp, alpha=args.alpha)
        results[k] = res
        
        status = "OK"
        if res.is_significant:
            status = "WARN" if res.drift_score < 0.1 else "BAD"
            bad_features.append(k)
            
        print(f"Feature: {k:15} | Train: {tp:.4f} | Serve: {sp:.4f} | Diff: {res.drift_score:+.4f} | Z: {res.z_score:6.2f} | P: {res.p_value:.4f} | {status}")

    # Write Prometheus metrics if requested
    if args.out_prom:
        with open(args.out_prom, "w") as f:
            f.write("# HELP conf_skew_z_score Z-score for confirmation feature skew\n")
            f.write("# TYPE conf_skew_z_score gauge\n")
            for k, res in results.items():
                f.write(f'conf_skew_z_score{{feature="{k}"}} {res.z_score}\n')
                
            f.write("# HELP conf_skew_drift Absolute proportion difference\n")
            f.write("# TYPE conf_skew_drift gauge\n")
            for k, res in results.items():
                f.write(f'conf_skew_drift{{feature="{k}"}} {res.drift_score}\n')
                
            f.write("# HELP conf_skew_significant 1 if skew is statistically significant\n")
            f.write("# TYPE conf_skew_significant gauge\n")
            for k, res in results.items():
                val = 1 if res.is_significant else 0
                f.write(f'conf_skew_significant{{feature="{k}"}} {val}\n')

    # Write JSON report if requested
    if args.out_json:
        report = {
            "ts": time.time(),
            "train_path": args.train,
            "serve_path": args.serve,
            "train_n": tn,
            "serve_n": sn,
            "results": {k: {
                "drift": res.drift_score,
                "z": res.z_score,
                "p": res.p_value,
                "sig": res.is_significant
            } for k, res in results.items()},
            "bad_features": bad_features,
            "critical": any(results[k].drift_score >= 0.1 for k in bad_features)
        }
        with open(args.out_json, "w") as f:
            json.dump(report, f, indent=2)

    if bad_features:
        print(f"\n--- FAILED FEATURES: {', '.join(bad_features)} ---")
        if any(results[k].drift_score >= 0.1 for k in bad_features):
             print("CRITICAL SKEW DETECTED! (>10% DRIFT)")
             sys.exit(2)
        sys.exit(0)  # Exit 0 if only WARN, or maybe exit 1? Let's say exit 0 for now unless it's critical.
    else:
        print("\n--- ALL FEATURES OK ---")
        sys.exit(0)


if __name__ == "__main__":
    main()
