#!/usr/bin/env python3
# scripts/setup_sample_experiment.py

"""
Sample script to create and manage experiments.

This script demonstrates how to:
1. Create a new experiment
2. Update experiment status
3. Query experiment results
"""

import os
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

PG_DSN = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))

def create_sample_experiment():
    """Create a sample experiment for testing confidence threshold boost"""

    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            # Create experiment
            experiment_data = {
                "experiment_id": "confidence_threshold_boost_v1"
                "name": "Confidence Threshold Boost Experiment"
                "filter_name": "confidence_boost"
                "signal_family": "orderflow"
                "direction": 0,  # All directions
                "status": "running"
                "target_metric": "expectancy_r"
                "config": json.dumps({
                    "confidence_threshold": 70.0,  # Boost threshold from default 30-60
                    "z_threshold_multiplier": 1.2,  # Slightly relaxed z-threshold
                })
            }

            cur.execute("""
                INSERT INTO signal_experiment (
                    experiment_id, name, filter_name, signal_family, direction
                    status, target_metric, config, start_at
                ) VALUES (
                    %(experiment_id)s, %(name)s, %(filter_name)s, %(signal_family)s, %(direction)s
                    %(status)s, %(target_metric)s, %(config)s, NOW()
                )
                ON CONFLICT (experiment_id) DO UPDATE SET
                    name = EXCLUDED.name
                    filter_name = EXCLUDED.filter_name
                    signal_family = EXCLUDED.signal_family
                    status = EXCLUDED.status
                    target_metric = EXCLUDED.target_metric
                    config = EXCLUDED.config
            """, experiment_data)

        conn.commit()
        print("✅ Created/Updated experiment: confidence_threshold_boost_v1")

    finally:
        conn.close()


def create_weak_progress_experiment():
    """Create experiment for testing weak progress filter"""

    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            experiment_data = {
                "experiment_id": "weak_progress_filter_v1"
                "name": "Weak Progress Absorption Filter"
                "filter_name": "weak_progress_filter"
                "signal_family": "orderflow"
                "direction": 0
                "status": "draft",  # Start as draft, can be activated later
                "target_metric": "sharpe_r"
                "config": json.dumps({
                    "require_weak_progress": True
                    "weak_progress_threshold": 0.25
                })
            }

            cur.execute("""
                INSERT INTO signal_experiment (
                    experiment_id, name, filter_name, signal_family, direction
                    status, target_metric, config, created_at
                ) VALUES (
                    %(experiment_id)s, %(name)s, %(filter_name)s, %(signal_family)s, %(direction)s
                    %(status)s, %(target_metric)s, %(config)s, NOW()
                )
                ON CONFLICT (experiment_id) DO NOTHING
            """, experiment_data)

        conn.commit()
        print("✅ Created experiment: weak_progress_filter_v1 (status: draft)")

    finally:
        conn.close()


def activate_experiment(experiment_id: str):
    """Activate a draft experiment"""

    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE signal_experiment
                SET status = 'running', start_at = NOW()
                WHERE experiment_id = %s AND status = 'draft'
            """, (experiment_id,))

            if cur.rowcount > 0:
                print(f"✅ Activated experiment: {experiment_id}")
            else:
                print(f"⚠️  Experiment {experiment_id} not found or not in draft status")

        conn.commit()

    finally:
        conn.close()


def stop_experiment(experiment_id: str):
    """Stop a running experiment"""

    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE signal_experiment
                SET status = 'stopped', end_at = NOW()
                WHERE experiment_id = %s AND status = 'running'
            """, (experiment_id,))

            if cur.rowcount > 0:
                print(f"✅ Stopped experiment: {experiment_id}")
            else:
                print(f"⚠️  Experiment {experiment_id} not found or not running")

        conn.commit()

    finally:
        conn.close()


def list_experiments():
    """List all experiments with their status"""

    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT experiment_id, name, status, target_metric
                       start_at, end_at, created_at
                FROM signal_experiment
                ORDER BY created_at DESC
            """)

            experiments = cur.fetchall()

            if not experiments:
                print("No experiments found")
                return

            print("📊 Experiments:")
            print("-" * 80)
            for exp in experiments:
                status_icon = {
                    "draft": "📝"
                    "running": "🟢"
                    "stopped": "🟡"
                    "completed": "✅"
                }.get(exp["status"], "❓")

                print(f"{status_icon} {exp['experiment_id']}")
                print(f"   Name: {exp['name']}")
                print(f"   Status: {exp['status']}")
                print(f"   Target: {exp['target_metric']}")
                print(f"   Created: {exp['created_at']}")
                if exp["start_at"]:
                    print(f"   Started: {exp['start_at']}")
                if exp["end_at"]:
                    print(f"   Ended: {exp['end_at']}")
                print()

    finally:
        conn.close()


def show_experiment_results(experiment_id: str):
    """Show results for a specific experiment"""

    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get latest snapshots for each variant
            cur.execute("""
                SELECT variant, signals_total, traded_total, winners_total, losers_total
                       expectancy_r, sharpe_r, max_dd_r, cl_ratio, winrate
                       precision, recall, f1, as_of
                FROM signal_experiment_snapshot
                WHERE experiment_id = %s
                AND (variant, as_of) IN (
                    SELECT variant, MAX(as_of)
                    FROM signal_experiment_snapshot
                    WHERE experiment_id = %s
                    GROUP BY variant
                )
                ORDER BY variant
            """, (experiment_id, experiment_id))

            results = cur.fetchall()

            if not results:
                print(f"No results found for experiment: {experiment_id}")
                return

            print(f"📈 Results for experiment: {experiment_id}")
            print("-" * 60)

            for result in results:
                print(f"Variant: {result['variant']}")
                print(f"  Signals: {result['signals_total']}")
                print(f"  Traded: {result['traded_total']}")
                print(".3f")
                print(".3f")
                print(".3f")
                print(".3f")
                print(".3f")
                print(".3f")
                print(".3f")
                print(f"  As of: {result['as_of']}")
                print()

    finally:
        conn.close()


def main():
    """Main function with command line interface"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python setup_sample_experiment.py <command>")
        print("Commands:")
        print("  create_sample        - Create sample confidence boost experiment")
        print("  create_weak_progress - Create weak progress filter experiment")
        print("  activate <exp_id>    - Activate a draft experiment")
        print("  stop <exp_id>        - Stop a running experiment")
        print("  list                 - List all experiments")
        print("  results <exp_id>     - Show results for experiment")
        return

    command = sys.argv[1]

    try:
        if command == "create_sample":
            create_sample_experiment()
        elif command == "create_weak_progress":
            create_weak_progress_experiment()
        elif command == "activate" and len(sys.argv) > 2:
            activate_experiment(sys.argv[2])
        elif command == "stop" and len(sys.argv) > 2:
            stop_experiment(sys.argv[2])
        elif command == "list":
            list_experiments()
        elif command == "results" and len(sys.argv) > 2:
            show_experiment_results(sys.argv[2])
        else:
            print(f"Unknown command: {command}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()























































