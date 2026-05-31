#!/usr/bin/env python3
"""
Execution Reports Aggregator - PnL & Win-rate analysis by signal rule.

Reads orders:exec stream, joins with signal snapshots, calculates
performance metrics by signal type/rule.

Usage:
    python3 aggregate_exec.py --start "2025-10-25T00:00:00Z" \
                               --end "2025-10-26T00:00:00Z" \
                               --out reports/exec_2025-10-25.parquet
"""

import argparse
import json
import os
import time
from datetime import datetime

import pandas as pd
import redis

from core.redis_client import get_redis, wait_for_redis

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
EXEC_STREAM = os.getenv("EXEC_STREAM", "orders:exec")
SNAP_PREFIX = os.getenv("SNAP_PREFIX", "signal:snap:")


def to_ms(s: str) -> str:
    """Convert ISO or epoch-ms to stream ID format."""
    if s.isdigit():
        return f"{int(s)}-0"
    dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
    return f"{int(dt.timestamp() * 1000)}-0"


def load_exec(r: redis.Redis, start_id: str, end_id: str) -> pd.DataFrame:
    """
    Load execution records from Redis stream.
    
    Args:
        r: Redis client
        start_id: Start stream ID
        end_id: End stream ID
        
    Returns:
        DataFrame with execution records
    """
    rows = []
    last = start_id

    print(f"📊 Reading {EXEC_STREAM} from {start_id} to {end_id}...")

    while True:
        try:
            chunk = r.xrange(EXEC_STREAM, min=last, max=end_id, count=1000)
        except redis.exceptions.BusyLoadingError:
            print("⚠️  Redis is loading dataset, retrying in 10 seconds...")
            time.sleep(10)
            continue
        except redis.exceptions.ConnectionError as e:
            print(f"❌ Redis connection error: {e}")
            raise

        if not chunk:
            break

        for mid, fields in chunk:
            d = dict(fields)
            d["stream_id"] = mid
            rows.append(d)

        # Advance
        ms, seq = last.split("-")
        last = f"{ms}-{int(seq) + 1}"

    print(f"✅ Loaded {len(rows)} execution records")
    return pd.DataFrame(rows)


def attach_snapshots(df: pd.DataFrame, r: redis.Redis) -> pd.DataFrame:
    """
    Attach signal snapshots to execution records.
    
    Args:
        df: DataFrame with execution records
        r: Redis client
        
    Returns:
        DataFrame with attached snapshot data
    """
    if "sid" not in df.columns or df["sid"].isna().all():
        print("⚠️  No 'sid' column or all NaN, skipping snapshots")
        return df

    unique_sids = sorted(set(df["sid"].dropna().astype(str)))
    print(f"📸 Loading {len(unique_sids)} signal snapshots...")

    notes = {}
    for sid in unique_sids:
        try:
            snap = r.get(SNAP_PREFIX + sid)
        except redis.exceptions.BusyLoadingError:
            print("⚠️  Redis is loading dataset during snapshot loading, skipping...")
            break
        except redis.exceptions.ConnectionError as e:
            print(f"❌ Redis connection error during snapshot loading: {e}")
            raise

        if snap:
            try:
                j = json.loads(snap)
                notes[sid] = {
                    "note": j.get("note", ""),
                    "side": j.get("side", ""),
                    "entry": float(j.get("price") or 0.0),
                    "atr": j.get("risk", {}).get("atr", 0.0)
                }
            except Exception as e:
                print(f"⚠️  Failed to parse snapshot for {sid}: {e}")

    print(f"✅ Loaded {len(notes)} snapshots")

    if notes:
        ndf = pd.DataFrame([{"sid": k, **v} for k, v in notes.items()])
        df = df.merge(ndf, on="sid", how="left", suffixes=("", "_snap"))

    return df


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="Aggregate execution reports with PnL analysis"
    )
    ap.add_argument("--start", required=True, help="Start time (ISO or epoch-ms)")
    ap.add_argument("--end", required=True, help="End time (ISO or epoch-ms)")
    ap.add_argument("--out", required=True, help="Output file (*.csv or *.parquet)")
    args = ap.parse_args()

    print("=" * 80)
    print("📊 XAUUSD Execution Reports Aggregator v7")
    print("=" * 80)
    print()

    # Connect to Redis with retry on BusyLoadingError
    print("🔌 Connecting to Redis...")
    try:
        # Increase retry attempts to give Redis more time to load
        r = get_redis(retry_attempts=20, retry_delay=2)
        # Wait for Redis to be fully ready (handles BusyLoading)
        print("⏳ Waiting for Redis to be ready...")
        if not wait_for_redis(r, max_retries=30, delay=10.0):
            print("❌ Redis is still loading after maximum wait time")
            raise RuntimeError("Redis is not ready after waiting")
        print("✅ Redis connected and ready")
    except redis.exceptions.BusyLoadingError:
        # If get_redis() exhausted retries, wait explicitly
        print("⚠️ Redis still loading after get_redis() retries, waiting explicitly...")
        # Try to get a connection one more time with more patience
        try:
            r = get_redis(retry_attempts=30, retry_delay=3)
            if not wait_for_redis(r, max_retries=30, delay=10.0):
                print("❌ Redis is still loading after maximum wait time")
                raise RuntimeError("Redis is not ready after waiting")
            print("✅ Redis connected and ready after extended wait")
        except Exception as e2:
            print(f"❌ Failed to connect to Redis after extended wait: {e2}")
            raise
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        raise

    # Parse time range
    start_id = to_ms(args.start)
    end_id = to_ms(args.end).replace("-0", "-999999")

    # Load execution records
    df = load_exec(r, start_id, end_id)

    if df.empty:
        print("⚠️  No execution records found")
        pd.DataFrame().to_csv(args.out, index=False)
        return

    # Parse JSON fields if present
    if "json" in df.columns:
        print("📦 Parsing JSON fields...")
        extra = df["json"].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x.startswith("{") else {}
        )
        df = pd.concat([df.drop(columns=["json"]), extra.apply(pd.Series)], axis=1)

    # Attach signal snapshots
    df = attach_snapshots(df, r)

    # Coerce numeric columns
    numeric_cols = ["price", "exec_price", "profit", "volume", "lot", "sl", "tp"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Calculate metrics by rule
    group_key = "note" if "note" in df.columns else "status"

    if "profit" in df.columns and not df["profit"].isna().all():
        print()
        print("=" * 80)
        print(f"📈 Performance Summary by {group_key.upper()}")
        print("=" * 80)

        grouped = df.groupby(group_key)

        summary = pd.DataFrame({
            "trades": grouped.size(),
            "avg_profit": grouped["profit"].mean(),
            "median_profit": grouped["profit"].median(),
            "total_profit": grouped["profit"].sum(),
            "win_rate": grouped["profit"].apply(lambda s: (s > 0).mean()),
            "max_profit": grouped["profit"].max(),
            "max_loss": grouped["profit"].min(),
            "std_profit": grouped["profit"].std()
        })

        # Calculate Sharpe-like metric
        summary["sharpe"] = summary["avg_profit"] / (summary["std_profit"] + 1e-9)

        # Sort by total profit
        summary = summary.sort_values("total_profit", ascending=False)

        print(summary.to_string())
        print()

        # Overall summary
        total_trades = len(df)
        total_profit = df["profit"].sum()
        overall_wr = (df["profit"] > 0).mean()
        avg_profit = df["profit"].mean()

        print("=" * 80)
        print("🎯 OVERALL SUMMARY")
        print("=" * 80)
        print(f"Total trades:    {total_trades}")
        print(f"Total profit:    ${total_profit:.2f}")
        print(f"Average profit:  ${avg_profit:.2f}")
        print(f"Win rate:        {overall_wr:.1%}")
        print()
    else:
        print("⚠️  No 'profit' column found or all NaN")
        print("   Make sure MT5 executor sends profit in /orders/confirm payloads")

    # Export
    if args.out.endswith(".parquet"):
        df.to_parquet(args.out, index=False)
    else:
        df.to_csv(args.out, index=False)

    print(f"✅ Exported {len(df)} records to {args.out}")
    print()


if __name__ == "__main__":
    main()

