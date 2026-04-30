#!/usr/bin/env python3
"""
Plot Reliability Calibration Curves

Creates visualizations of confidence vs hit-rate calibration curves.
Requires matplotlib and pandas.
"""

from __future__ import annotations

import os
import sys
import csv
from collections import defaultdict
from typing import Dict, List

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

try:
    import matplotlib.pyplot as plt
    import pandas as pd
    HAS_PLOTTING = True
except ImportError:
    HAS_PLOTTING = False
    print("⚠️  matplotlib and pandas not available. Install with:")
    print("   pip install matplotlib pandas")


def load_calibration_curves(data_dir: str) -> Dict[str, pd.DataFrame]:
    """Load calibration curve data from CSV files."""
    curves = {}

    for filename in os.listdir(data_dir):
        if filename.startswith('calibration_curve_') and filename.endswith('.csv'):
            outcome = filename.replace('calibration_curve_', '').replace('.csv', '')
            filepath = os.path.join(data_dir, filename)

            try:
                df = pd.read_csv(filepath)
                curves[outcome] = df
            except Exception as e:
                print(f"Error loading {filename}: {e}")

    return curves


def plot_calibration_curves(curves: Dict[str, pd.DataFrame], output_file: str = None):
    """Plot calibration curves for all outcomes."""
    if not HAS_PLOTTING:
        return

    plt.figure(figsize=(12, 8))

    colors = ['blue', 'green', 'red', 'orange', 'purple', 'brown']
    markers = ['o', 's', '^', 'D', 'v', '*']

    for i, (outcome, df) in enumerate(curves.items()):
        color = colors[i % len(colors)]
        marker = markers[i % len(markers)]

        plt.plot(df['confidence_pct'], df['avg_hit_rate']
                marker=marker, color=color, linewidth=2, markersize=6
                label=f'{outcome} (n={df["sample_count"].sum()})')

        # Add sample size annotations for buckets with significant data
        for _, row in df.iterrows():
            if row['sample_count'] >= 10:  # Only annotate buckets with decent samples
                plt.annotate(f'{int(row["sample_count"])}'
                           (row['confidence_pct'], row['avg_hit_rate'])
                           xytext=(5, 5), textcoords='offset points'
                           fontsize=8, alpha=0.7)

    # Perfect calibration line
    plt.plot([0, 100], [0, 1], 'k--', alpha=0.5, label='Perfect Calibration')

    plt.xlabel('Predicted Confidence (%)')
    plt.ylabel('Actual Hit Rate')
    plt.title('Reliability Calibration Curves\nConfidence vs Actual Hit Rate by Outcome')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xlim(0, 100)
    plt.ylim(0, 1)

    # Add interpretation zones
    plt.axhline(y=0.5, color='red', linestyle=':', alpha=0.5)
    plt.text(5, 0.52, 'Overconfident zone', fontsize=10, color='red', alpha=0.7)
    plt.text(5, 0.45, 'Underconfident zone', fontsize=10, color='blue', alpha=0.7)

    plt.tight_layout()

    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✅ Saved plot to {output_file}")
    else:
        plt.show()


def plot_outcome_comparison(curves: Dict[str, pd.DataFrame], output_file: str = None):
    """Create comparison plot showing differences between outcomes."""
    if not HAS_PLOTTING:
        return

    if len(curves) < 2:
        print("Need at least 2 outcomes for comparison plot")
        return

    plt.figure(figsize=(15, 10))

    # Subplot 1: All curves together
    plt.subplot(2, 2, 1)
    for outcome, df in curves.items():
        plt.plot(df['confidence_pct'], df['avg_hit_rate'], marker='o', label=outcome, linewidth=2)

    plt.plot([0, 100], [0, 1], 'k--', alpha=0.5)
    plt.xlabel('Confidence (%)')
    plt.ylabel('Hit Rate')
    plt.title('All Outcomes Comparison')
    plt.legend()
    plt.grid(True, alpha=0.3)

    # Subplot 2: Differences from tp2 (if available)
    if 'tp2' in curves:
        plt.subplot(2, 2, 2)
        tp2_df = curves['tp2'].set_index('confidence_pct')['avg_hit_rate']

        for outcome, df in curves.items():
            if outcome == 'tp2':
                continue

            df_indexed = df.set_index('confidence_pct')['avg_hit_rate']
            # Align indices
            common_conf = sorted(set(tp2_df.index) & set(df_indexed.index))

            if common_conf:
                diff = df_indexed.loc[common_conf] - tp2_df.loc[common_conf]
                plt.plot(common_conf, diff, marker='o', label=f'{outcome} - tp2', linewidth=2)

        plt.axhline(y=0, color='black', linestyle='-', alpha=0.8)
        plt.xlabel('Confidence (%)')
        plt.ylabel('Hit Rate Difference from tp2')
        plt.title('Outcome Differences vs tp2 Baseline')
        plt.legend()
        plt.grid(True, alpha=0.3)

    # Subplot 3: Sample sizes
    plt.subplot(2, 2, 3)
    outcomes = list(curves.keys())
    total_samples = [curves[outcome]['sample_count'].sum() for outcome in outcomes]

    bars = plt.bar(outcomes, total_samples)
    plt.ylabel('Total Samples')
    plt.title('Sample Size by Outcome')
    plt.xticks(rotation=45)

    # Add value labels on bars
    for bar, samples in zip(bars, total_samples):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height()
                f'{int(samples)}', ha='center', va='bottom')

    # Subplot 4: Average hit rates
    plt.subplot(2, 2, 4)
    avg_hit_rates = []
    for outcome in outcomes:
        df = curves[outcome]
        # Weighted average by sample count
        weights = df['sample_count']
        avg_rate = (df['avg_hit_rate'] * weights).sum() / weights.sum()
        avg_hit_rates.append(avg_rate)

    bars = plt.bar(outcomes, avg_hit_rates)
    plt.ylabel('Average Hit Rate')
    plt.title('Average Hit Rate by Outcome')
    plt.xticks(rotation=45)
    plt.ylim(0, 1)

    # Add value labels
    for bar, rate in zip(bars, avg_hit_rates):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height()
                f'{rate:.1%}', ha='center', va='bottom')

    plt.tight_layout()

    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✅ Saved comparison plot to {output_file}")
    else:
        plt.show()


def main():
    """Main plotting function."""
    if not HAS_PLOTTING:
        print("❌ Cannot create plots without matplotlib and pandas")
        print("Install with: pip install matplotlib pandas")
        return

    data_dir = "reliability_calibration_export"

    if not os.path.exists(data_dir):
        print(f"❌ Data directory {data_dir} not found")
        print("Run export_reliability_calibration.py first")
        return

    print("📊 Loading calibration curve data...")
    curves = load_calibration_curves(data_dir)

    if not curves:
        print("❌ No calibration curve data found")
        return

    print(f"Loaded curves for outcomes: {list(curves.keys())}")

    # Create plots
    plot_calibration_curves(curves, "calibration_curves.png")
    plot_outcome_comparison(curves, "outcome_comparison.png")

    print("\n📈 Analysis Insights:")
    print("-" * 50)

    for outcome, df in curves.items():
        avg_rate = (df['avg_hit_rate'] * df['sample_count']).sum() / df['sample_count'].sum()
        total_samples = df['sample_count'].sum()
        print(".1%")

    print("\n💡 Interpretation:")
    print("• Curves above diagonal = underconfident (conservative predictions)")
    print("• Curves below diagonal = overconfident (optimistic predictions)")
    print("• Strict outcomes (t500, t2000) should have lower hit rates due to time filtering")
    print("• Use tp2 for entry quality, nosl_after_tp1* for management quality")


if __name__ == "__main__":
    main()
