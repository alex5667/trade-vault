#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from typing import Any

import redis

from ml_analysis.pbo_cscv import compute_pbo
from ml_analysis.psr_dsr import deflated_sharpe_ratio as compute_dsr
from ml_analysis.psr_dsr import probabilistic_sharpe_ratio as compute_psr
from ml_analysis.reality_check import evaluate_rows as evaluate_strategy
import contextlib


def load_dataset(filepath: str) -> list[dict[str, Any]]:
    dataset = []
    if os.path.exists(filepath):
        with open(filepath) as f:
            for line in f:
                if line.strip():
                    with contextlib.suppress(Exception):
                        dataset.append(json.loads(line))
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

    psr = compute_psr(returns_default) if returns_default else 0.0
    dsr = compute_dsr(returns_default, n_trials=len(variants_data) if len(variants_data) > 0 else 1) if returns_default else 0.0

    # PBO / CSCV
    returns_matrix = {k: v for k, v in variants_data.items()}

    # Ensure all variants have same length for matrix operations
    if returns_matrix:
        min_len = min(len(v) for v in returns_matrix.values())
        returns_matrix = {k: v[:min_len] for k, v in returns_matrix.items() if len(v) > 0}

    if returns_matrix and len(returns_matrix) > 0 and len(next(iter(returns_matrix.values()))) > 0:
        try:
            pbo_result = compute_pbo(returns_matrix, n_folds=4)
        except ValueError:
            pbo_result = {"pbo": 1.0, "cscv_splits": 0}
    else:
        pbo_result = {"pbo": 1.0, "cscv_splits": 0}

    pbo=pbo_result.get("pbo", 0.0)
    cscv_splits=pbo_result.get("cscv_splits", 0.0)

    blocker_active = False
    reasons = []

    if psr < psr_min:
        reasons.append(f"PSR ({psr:.2f}) < MIN ({psr_min})")
    if dsr < dsr_min:
        reasons.append(f"DSR ({dsr:.2f}) < MIN ({dsr_min})")
    if pbo > pbo_max:
        reasons.append(f"PBO ({pbo:.2f}) > MAX ({pbo_max})")

    if reasons:
        blocker_active = True

    report_only = int(os.getenv("STRATEGY_RESEARCH_GUARD_REPORT_ONLY", "1"))

    report = {
        "timestamp": int(time.time()),
        "metrics": {
            **universal_metrics,
            "psr": float(psr),
            "dsr": float(dsr),
            "pbo": float(pbo),
            "cscv_splits": int(cscv_splits),
            "chosen_variant_unique": "default"
        },
        "blocker_active": blocker_active,
        "reason": "; ".join(reasons) if reasons else "ok",
        "report_only": report_only
    }

    with open(args.report_path, "w") as f:
        json.dump(report, f, indent=2)

    try:
        r = redis.from_url(args.redis_url, decode_responses=True)
        now_ms = int(time.time() * 1000)
        summary_key = "metrics:strategy_research_guard:last"
        blocker_key = os.getenv("STRATEGY_RESEARCH_GUARD_BLOCKER_KEY", "cfg:research_guard:blocker:v1")

        # Write summary as Redis hash — consumed by state exporter (HGETALL) and calibrator (HGETALL)
        summary_fields: dict[str, str] = {
            k: str(v) for k, v in report["metrics"].items()
        }
        summary_fields.update({
            "updated_ts_ms": str(now_ms),
            "ts_ms": str(now_ms),
            "success": "1",
        })
        r.hset(summary_key, mapping=summary_fields)

        # Write blocker as Redis hash — consumed by evaluate_research_guard_gate (HGETALL)
        blocker_fields: dict[str, str] = {
            "blocked": "1" if report["blocker_active"] else "0",
            "blocker_active": "1" if report["blocker_active"] else "0",
            "reason": report["reason"],
            "report_only": str(report["report_only"]),
            "updated_ts_ms": str(now_ms),
        }
        r.hset(blocker_key, mapping=blocker_fields)
        print("Successfully published research guard state to Redis.")
    except Exception as e:
        print(f"Failed to publish to redis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
