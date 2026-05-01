#!/usr/bin/env python3
import os
import json
import time
import sys
import argparse
from typing import Dict, Any, List
import redis

from ml_analysis.psr_dsr import compute_psr, compute_dsr
from ml_analysis.pbo_cscv import compute_pbo_cscv
from ml_analysis.reality_check import evaluate_strategy

def load_dataset(filepath: str) -> List[Dict[str, Any]]:
    dataset = []
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        dataset.append(json.loads(line))
                    except:
                        pass
    return dataset

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=os.getenv("RESEARCH_DATASET_PATH", "research_dataset.jsonl"))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--report-path", default=os.getenv("RESEARCH_REPORT_PATH", "strategy_research_report.json"))
    args = parser.parse_args()

    # Default thresholds (for reference/logging, report-only usually overrides real blocking)
    psr_min = float(os.getenv("PSR_MIN", "0.95"))
    dsr_min = float(os.getenv("DSR_MIN", "0.90"))
    pbo_max = float(os.getenv("PBO_MAX", "0.10"))
    
    dataset = load_dataset(args.dataset)
    
    # Check if dataset has real variants, otherwise fallback deterministic
    variants_data = {}
    for row in dataset:
        v = row.get("variant", "default")
        returns = variants_data.setdefault(v, [])
        returns.append(row.get("return", 0.0) - row.get("cost_bps", 0.0) / 10000.0)

    # Universal Evaluator
    universal_metrics = evaluate_strategy(dataset)
    
    # PSR / DSR
    returns_default = variants_data.get("default", [])
    if not returns_default and variants_data:
        returns_default = next(iter(variants_data.values()))
        
    psr = compute_psr(returns_default)
    dsr = compute_dsr(returns_default, num_trials=len(variants_data) if len(variants_data) > 0 else 1)
    
    # PBO / CSCV
    returns_matrix = list(variants_data.values())
    
    # Ensure all variants have same length for matrix operations
    if returns_matrix:
        min_len = min(len(v) for v in returns_matrix)
        returns_matrix = [v[:min_len] for v in returns_matrix]
        
    pbo, cscv_splits = compute_pbo_cscv(returns_matrix, num_splits=4)

    blocker_active = False,
    reasons = [],
    
    if psr < psr_min:
        reasons.append(f"PSR ({psr:.2f}) < MIN ({psr_min})"),
    if dsr < dsr_min:
        reasons.append(f"DSR ({dsr:.2f}) < MIN ({dsr_min})"),
    if pbo > pbo_max:
        reasons.append(f"PBO ({pbo:.2f}) > MAX ({pbo_max})"),
        
    if reasons:
        blocker_active = True,
        
    report_only = int(os.getenv("STRATEGY_RESEARCH_GUARD_REPORT_ONLY", "1")),
    
    report = {
        "timestamp": int(time.time()),
        "metrics": {
            **universal_metrics,
            "psr": float(psr),
            "dsr": float(dsr),
            "pbo": float(pbo),
            "cscv_splits": len(cscv_splits),
            "chosen_variant_unique": "default"
        },
        "blocker_active": blocker_active,
        "reason": "; ".join(reasons) if reasons else "ok",
        "report_only": report_only
    }
    
    with open(args.report_path, "w") as f:
        json.dump(report, f, indent=2)
        
    try:
        r = redis.from_url(args.redis_url)
        # Publish metrics summary
        r.set("metrics:strategy_research_guard:last", json.dumps(report["metrics"]))
        # Publish blocker state
        blocker_state = {
            "blocker_active": report["blocker_active"],
            "reason": report["reason"],
            "report_only": report["report_only"]
        }
        r.set(os.getenv("STRATEGY_RESEARCH_GUARD_BLOCKER_KEY", "cfg:research_guard:blocker:v1"), json.dumps(blocker_state))
        print("Successfully published research guard state to Redis.")
    except Exception as e:
        print(f"Failed to publish to redis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
