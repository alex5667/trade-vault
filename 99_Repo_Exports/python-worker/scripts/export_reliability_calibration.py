#!/usr/bin/env python3
from __future__ import annotations
"""
Export Reliability Calibration Data to CSV

Exports all reliability calibration data to CSV files for external analysis.
Creates separate files for:
- Summary statistics
- Bucket-level data
- Configuration performance
"""


import os
import sys
import csv
from collections import defaultdict
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scripts.analyze_reliability_calibration import (
    get_all_relcal_keys,
    parse_relcal_key,
    get_relcal_data
)
from core.redis_client import get_redis


def export_summary_csv(data_by_key: Dict[str, Dict], filename: str):
    """Export summary statistics to CSV."""
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = [
            'outcome', 'kind', 'symbol', 'venue', 'session', 'tf', 'regime',
            'samples_total', 'hits_total', 'hit_rate', 'last_ts_ms'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for key_info, data in data_by_key.items():
            row = dict(key_info)
            row.update({
                'samples_total': data.get('samples_total', 0),
                'hits_total': data.get('hits_total', 0),
                'hit_rate': data.get('hits_total', 0) / max(1, data.get('samples_total', 0)),
                'last_ts_ms': data.get('last_ts_ms', 0)
            })
            writer.writerow(row)

    print(f"✅ Exported {len(data_by_key)} configurations to {filename}")


def export_bucket_csv(data_by_key: Dict[str, Dict], filename: str):
    """Export bucket-level data to CSV."""
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = [
            'outcome', 'kind', 'symbol', 'venue', 'session', 'tf', 'regime',
            'confidence_bucket', 'samples', 'hits', 'hit_rate'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for key_info, data in data_by_key.items():
            for conf_pct, bucket_data in data.get('buckets', {}).items():
                row = dict(key_info)
                row.update({
                    'confidence_bucket': conf_pct,
                    'samples': bucket_data['samples'],
                    'hits': bucket_data['hits'],
                    'hit_rate': bucket_data['hit_rate']
                })
                writer.writerow(row)

    total_buckets = sum(len(data.get('buckets', {})) for data in data_by_key.values())
    print(f"✅ Exported {total_buckets} bucket data points to {filename}")


def export_outcome_analysis(data_by_key: Dict[str, Dict], filename: str):
    """Export outcome-level aggregated analysis."""
    from scripts.analyze_reliability_calibration import analyze_outcome_performance

    outcome_analysis = analyze_outcome_performance(data_by_key)

    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['outcome', 'total_samples', 'total_hits', 'overall_hit_rate']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for outcome, stats in outcome_analysis.items():
            writer.writerow({
                'outcome': outcome,
                'total_samples': stats['total_samples'],
                'total_hits': stats['total_hits'],
                'overall_hit_rate': stats.get('overall_hit_rate', 0)
            })

    print(f"✅ Exported {len(outcome_analysis)} outcome analyses to {filename}")


def create_calibration_curves(data_by_key: Dict[str, Dict], output_dir: str):
    """Create calibration curve data for plotting."""
    outcome_curves = defaultdict(lambda: defaultdict(list))

    # Aggregate by outcome
    for key_info, data in data_by_key.items():
        outcome = key_info['outcome']
        for conf_pct, bucket_data in data.get('buckets', {}).items():
            outcome_curves[outcome][conf_pct].append(bucket_data['hit_rate'])

    # Create curve files
    for outcome, conf_data in outcome_curves.items():
        filename = os.path.join(output_dir, f'calibration_curve_{outcome}.csv')

        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['confidence_pct', 'avg_hit_rate', 'sample_count'])

            for conf_pct in sorted(conf_data.keys()):
                hit_rates = conf_data[conf_pct]
                avg_hit_rate = sum(hit_rates) / len(hit_rates)
                writer.writerow([conf_pct, avg_hit_rate, len(hit_rates)])

        print(f"✅ Created calibration curve for {outcome}: {filename}")


def main():
    """Main export function."""
    print("📤 Exporting Reliability Calibration Data...")

    # Create output directory
    output_dir = "reliability_calibration_export"
    os.makedirs(output_dir, exist_ok=True)

    redis_client = get_redis()

    # Get all relcal keys
    keys = get_all_relcal_keys(redis_client)
    print(f"Found {len(keys)} reliability calibration keys")

    if not keys:
        print("❌ No reliability calibration data found in Redis")
        return

    # Load data (limit for performance)
    data_by_key = {}
    for key in keys[:5000]:  # Reasonable limit
        key_info = parse_relcal_key(key)
        if key_info:
            data = get_relcal_data(redis_client, key)
            if data and data.get('samples_total', 0) > 0:  # Only export configs with data
                data_by_key[key_info] = data

    print(f"Loaded data for {len(data_by_key)} configurations with samples")

    # Export different views
    export_summary_csv(data_by_key, os.path.join(output_dir, "relcal_summary.csv"))
    export_bucket_csv(data_by_key, os.path.join(output_dir, "relcal_buckets.csv"))
    export_outcome_analysis(data_by_key, os.path.join(output_dir, "relcal_outcomes.csv"))
    create_calibration_curves(data_by_key, output_dir)

    print("\n📁 Export complete! Files created:")
    print(f"   • {output_dir}/relcal_summary.csv - Configuration summaries")
    print(f"   • {output_dir}/relcal_buckets.csv - Bucket-level data")
    print(f"   • {output_dir}/relcal_outcomes.csv - Outcome analysis")
    print(f"   • {output_dir}/calibration_curve_*.csv - Calibration curves for plotting")

    print("\n💡 Next steps:")
    print("   1. Import CSV files into Python/R/Excel for analysis")
    print("   2. Plot calibration curves to visualize confidence vs actual hit rate")
    print("   3. Compare outcomes: tp2 vs nosl_after_tp1 vs strict variants")
    print("   4. Identify best performing configurations")


if __name__ == "__main__":
    main()
