#!/usr/bin/env python3
"""
Nightly Strategy Research Guard Bundle Generator.

Loads research dataset, computes PSR/DSR/PBO metrics, writes to Redis as HASHes.
If dataset is missing, blocker_active defaults to True (fail-safe).
"""
import argparse
import contextlib
import json
import logging
import os
import sys
import time
from typing import Any

import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("research_guard_bundle")


def load_dataset(filepath: str) -> list[dict[str, Any]]:
    """Load JSONL dataset."""
    dataset = []
    if os.path.exists(filepath):
        try:
            with open(filepath) as f:
                for line in f:
                    if line.strip():
                        with contextlib.suppress(Exception):
                            dataset.append(json.loads(line))
            logger.info("✓ Loaded %d rows from %s", len(dataset), filepath)
        except Exception as e:
            logger.warning("Failed to load dataset: %s", e)
    else:
        logger.warning("⚠️ Dataset not found: %s (blocker will be active)", filepath)
    return dataset


def compute_stub_metrics(dataset: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute stub metrics when dataset is missing or empty."""
    if not dataset:
        return {
            "rows": 0,
            "psr": 0.0,
            "dsr": 0.0,
            "pbo": 1.0,
            "ece": 0.0,
            "brier": 0.0,
            "net_expectancy": 0.0,
            "mean_r": 0.0,
            "precision_at_top_x": 0.0,
        }
    # TODO: integrate real PSR/DSR/PBO computation from ml_analysis when dataset is populated
    return {
        "rows": len(dataset),
        "psr": 0.0,
        "dsr": 0.0,
        "pbo": 1.0,
        "ece": 0.0,
        "brier": 0.0,
        "net_expectancy": 0.0,
        "mean_r": 0.0,
        "precision_at_top_x": 0.0,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate nightly research guard bundle")
    parser.add_argument("--dataset", default=os.getenv("RESEARCH_DATASET_PATH", "research_dataset.jsonl"))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--report-path", default=os.getenv("RESEARCH_REPORT_PATH", "strategy_research_report.json"))
    args = parser.parse_args()

    psr_min = float(os.getenv("RG_BUNDLE_PSR_MIN", "0.95"))
    dsr_min = float(os.getenv("RG_BUNDLE_DSR_MIN", "0.90"))
    pbo_max = float(os.getenv("RG_BUNDLE_PBO_MAX", "0.10"))

    dataset = load_dataset(args.dataset)
    metrics = compute_stub_metrics(dataset)

    psr = metrics.get("psr", 0.0)
    dsr = metrics.get("dsr", 0.0)
    pbo = metrics.get("pbo", 1.0)

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
        "metrics": metrics,
        "blocker_active": blocker_active,
        "reason": "; ".join(reasons) if reasons else "ok",
        "report_only": report_only
    }

    with open(args.report_path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info("✓ Wrote report to %s", args.report_path)

    try:
        r = redis.from_url(args.redis_url, decode_responses=True)
        now_ms = int(time.time() * 1000)
        summary_key = "metrics:strategy_research_guard:last"
        blocker_key = os.getenv("STRATEGY_RESEARCH_GUARD_BLOCKER_KEY", "cfg:research_guard:blocker:v1")

        # Write summary as Redis HASH (required by calibrator HGETALL)
        summary_fields: dict[str, str] = {k: str(v) for k, v in metrics.items()}
        summary_fields.update({
            "updated_ts_ms": str(now_ms),
            "ts_ms": str(now_ms),
            "success": "1",
        })
        r.hset(summary_key, mapping=summary_fields)
        logger.info("✓ Wrote summary hash: %s", summary_key)

        # Write blocker as Redis HASH (required by blocker_v1 HGETALL)
        blocker_fields: dict[str, str] = {
            "blocked": "1" if report["blocker_active"] else "0",
            "blocker_active": "1" if report["blocker_active"] else "0",
            "reason": report["reason"],
            "report_only": str(report["report_only"]),
            "updated_ts_ms": str(now_ms),
        }
        r.hset(blocker_key, mapping=blocker_fields)
        logger.info("✓ Wrote blocker hash: %s (active=%s)", blocker_key, report["blocker_active"])

        logger.info("✅ Successfully published research guard state to Redis (HASH format)")
    except Exception as e:
        logger.error("❌ Failed to publish to redis: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
