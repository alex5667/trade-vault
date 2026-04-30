#!/usr/bin/env python3
"""
Analyze Reliability Calibration Data

Extracts and analyzes confidence -> hit-rate curves from Redis.
Shows patterns across different outcomes and dimensions.
"""

from __future__ import annotations

import os
import sys
import math
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from services.reliability_calibrator import _bucket_conf_pct
from core.redis_client import get_redis


def get_all_relcal_keys(redis_client) -> List[str]:
    """Get all reliability calibration keys."""
    keys = []
    cursor = 0
    while True:
        cursor, batch = redis_client.scan(cursor, match="relcal:*", count=10000)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


def parse_relcal_key(key: str) -> Optional[Dict[str, str]]:
    """Parse relcal key into components."""
    parts = key.split(":")
    if len(parts) != 8 or parts[0] != "relcal":
        return None

    _, outcome, kind, symbol, venue, session, tf, regime = parts
    return {
        "outcome": outcome
        "kind": kind
        "symbol": symbol
        "venue": venue
        "session": session
        "tf": tf
        "regime": regime
        "key": key
    }


def get_relcal_data(redis_client, key: str) -> Dict[str, Any]:
    """Extract reliability data from Redis key."""
    h = redis_client.hgetall(key)
    if not h:
        return {}

    data = {}
    # Global counters
    data["samples_total"] = int(h.get("samples_total", 0))
    data["hits_total"] = int(h.get("hits_total", 0))

    # Bucket data (confidence 0-100 in 5% steps)
    buckets = {}
    for conf_pct in range(0, 101, 5):
        bucket = f"b{conf_pct}"
        n = int(h.get(f"{bucket}:n", 0))
        hits = int(h.get(f"{bucket}:h", 0))
        if n > 0:
            buckets[conf_pct] = {"samples": n, "hits": hits, "hit_rate": hits / n}

    data["buckets"] = buckets
    data["last_ts_ms"] = int(h.get("last_ts_ms", 0))

    return data


def analyze_outcome_performance(data_by_key: Dict[str, Dict]) -> Dict[str, Any]:
    """Analyze performance across outcomes."""
    outcome_stats = defaultdict(lambda: {"total_samples": 0, "total_hits": 0, "buckets": defaultdict(list)})

    for key_info, data in data_by_key.items():
        outcome = key_info["outcome"]
        stats = outcome_stats[outcome]

        stats["total_samples"] += data.get("samples_total", 0)
        stats["total_hits"] += data.get("hits_total", 0)

        # Aggregate bucket data across all keys for this outcome
        for conf, bucket_data in data.get("buckets", {}).items():
            stats["buckets"][conf].append(bucket_data)

    # Calculate averages
    for outcome, stats in outcome_stats.items():
        if stats["total_samples"] > 0:
            stats["overall_hit_rate"] = stats["total_hits"] / stats["total_samples"]

        # Average hit rates per confidence bucket
        avg_buckets = {}
        for conf, bucket_list in stats["buckets"].items():
            if bucket_list:
                avg_hit_rate = sum(b["hit_rate"] for b in bucket_list) / len(bucket_list)
                avg_samples = sum(b["samples"] for b in bucket_list) / len(bucket_list)
                avg_buckets[conf] = {"avg_hit_rate": avg_hit_rate, "avg_samples": avg_samples}

        stats["avg_buckets"] = dict(sorted(avg_buckets.items()))

    return dict(outcome_stats)


def analyze_symbol_performance(data_by_key: Dict[str, Dict]) -> Dict[str, Any]:
    """Analyze performance by symbol."""
    symbol_stats = defaultdict(lambda: {"outcomes": defaultdict(lambda: {"samples": 0, "hits": 0})})

    for key_info, data in data_by_key.items():
        symbol = key_info["symbol"]
        outcome = key_info["outcome"]

        symbol_stats[symbol]["outcomes"][outcome]["samples"] += data.get("samples_total", 0)
        symbol_stats[symbol]["outcomes"][outcome]["hits"] += data.get("hits_total", 0)

    # Calculate hit rates
    for symbol, stats in symbol_stats.items():
        for outcome, outcome_data in stats["outcomes"].items():
            if outcome_data["samples"] > 0:
                outcome_data["hit_rate"] = outcome_data["hits"] / outcome_data["samples"]

    return dict(symbol_stats)


def find_best_performing_configs(data_by_key: Dict[str, Dict], min_samples: int = 50) -> List[Dict]:
    """Find configurations with best performance."""
    configs = []

    for key_info, data in data_by_key.items():
        samples = data.get("samples_total", 0)
        hits = data.get("hits_total", 0)

        if samples >= min_samples and hits > 0:
            hit_rate = hits / samples
            configs.append({
                "config": key_info
                "samples": samples
                "hit_rate": hit_rate
                "score": hit_rate * math.log(samples)  # Reward both high hit_rate and sample count
            })

    # Sort by score (hit_rate * log(samples))
    configs.sort(key=lambda x: x["score"], reverse=True)
    return configs[:20]  # Top 20


def print_analysis_report(outcome_analysis: Dict, symbol_analysis: Dict, top_configs: List):
    """Print comprehensive analysis report."""

    print("=" * 80)
    print("RELIABILITY CALIBRATION ANALYSIS REPORT")
    print("=" * 80)

    print("\n📊 OUTCOME PERFORMANCE:")
    print("-" * 50)
    for outcome, stats in outcome_analysis.items():
        total_samples = stats["total_samples"]
        overall_hit_rate = stats.get("overall_hit_rate", 0)

        print(f"\n{outcome.upper()}:")
        print(".1f")
        print(f"  Buckets with data: {len(stats['avg_buckets'])}")

        # Show confidence curve
        if stats['avg_buckets']:
            print("  Confidence -> Hit Rate curve:")
            for conf in sorted(stats['avg_buckets'].keys()):
                bucket = stats['avg_buckets'][conf]
                print(".1f")

    print("\n💰 SYMBOL PERFORMANCE:")
    print("-" * 50)
    for symbol, stats in sorted(symbol_analysis.items()):
        print(f"\n{symbol}:")
        for outcome, outcome_data in stats["outcomes"].items():
            samples = outcome_data["samples"]
            hit_rate = outcome_data.get("hit_rate", 0)
            if samples > 0:
                print(".1f")

    print("\n🏆 TOP PERFORMING CONFIGURATIONS:")
    print("-" * 50)
    for i, config in enumerate(top_configs[:10], 1):
        print(f"{i}. {config['config']['symbol']} | {config['config']['outcome']} | {config['config']['kind']}")
        print(".1f")

    print("\n💡 INSIGHTS:")
    print("-" * 50)

    # Find outcome with best overall performance
    if outcome_analysis:
        best_outcome = max(outcome_analysis.items(), key=lambda x: x[1].get("overall_hit_rate", 0))
        print(f"• Best overall outcome: {best_outcome[0]} ({best_outcome[1].get('overall_hit_rate', 0):.1%})")

    # Check if strict outcomes have lower hit rates (expected)
    if "nosl_after_tp1_t500" in outcome_analysis and "nosl_after_tp1" in outcome_analysis:
        strict_rate = outcome_analysis["nosl_after_tp1_t500"].get("overall_hit_rate", 0)
        relaxed_rate = outcome_analysis["nosl_after_tp1"].get("overall_hit_rate", 0)
        diff = relaxed_rate - strict_rate
        print(f"• Strict horizon penalty (500ms): {diff:.1%} lower hit rate")

    print("• Use tp2 for entry quality assessment")
    print("• Use nosl_after_tp1 for management quality")
    print("• Strict outcomes show true hold quality but need more data")


def main():
    """Main analysis function."""
    print("🔍 Analyzing Reliability Calibration Data...")

    redis_client = get_redis()

    # Get all relcal keys
    keys = get_all_relcal_keys(redis_client)
    print(f"Found {len(keys)} reliability calibration keys")

    if not keys:
        print("❌ No reliability calibration data found in Redis")
        print("Make sure REL_CAL_ENABLED=1 and trades have been processed")
        return

    # Parse keys and get data
    data_by_key = {}
    for key in keys[:1000]:  # Limit for performance
        key_info = parse_relcal_key(key)
        if key_info:
            data = get_relcal_data(redis_client, key)
            if data:
                data_by_key[key_info] = data

    print(f"Loaded data for {len(data_by_key)} configurations")

    # Analyze
    outcome_analysis = analyze_outcome_performance(data_by_key)
    symbol_analysis = analyze_symbol_performance(data_by_key)
    top_configs = find_best_performing_configs(data_by_key)

    # Report
    print_analysis_report(outcome_analysis, symbol_analysis, top_configs)


if __name__ == "__main__":
    main()
