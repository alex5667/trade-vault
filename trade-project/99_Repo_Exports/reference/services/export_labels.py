#!/usr/bin/env python3
"""
Export labels from Redis Stream to Parquet/CSV.

Exports trade labels from labels:trades stream for offline analysis,
supervised learning, and calibration.

Usage:
    python3 export_labels.py \\
        --start "2025-10-25T00:00:00Z" \\
        --end "2025-10-26T00:00:00Z" \\
        --out labels.parquet
"""

import os
import argparse
import redis
from datetime import datetime
from typing import List, Dict

try:
    import pandas as pd
except ImportError:
    print("Error: pandas not installed. Run: pip install pandas pyarrow")
    exit(1)


# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
STREAM = os.getenv("LABELS_STREAM", "labels:trades")


def parse_time(s: str) -> str:
    """
    Parse time string to Redis Stream ID format.
    
    Args:
        s: Time string (epoch ms or ISO 8601)
        
    Returns:
        Redis Stream ID (e.g., "1729854000000-0")
    """
    try:
        # Try as epoch ms first
        if s.isdigit():
            return f"{int(s)}-0"
        
        # Try ISO format
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        ms = int(dt.timestamp() * 1000)
        return f"{ms}-0"
    except Exception as e:
        raise SystemExit(f"Invalid time format '{s}': {e}")


def export_labels(
    r: redis.Redis,
    start_id: str,
    end_id: str
) -> List[Dict]:
    """
    Export labels from Redis Stream.
    
    Args:
        r: Redis client
        start_id: Start stream ID
        end_id: End stream ID
        
    Returns:
        List of label dictionaries
    """
    rows = []
    last = start_id
    
    print(f"📂 Exporting from {STREAM}...")
    print(f"   Range: {start_id} to {end_id}")
    
    while True:
        chunk = r.xrange(STREAM, min=last, max=end_id, count=1000)
        if not chunk:
            break
        
        for mid, fields in chunk:
            # Convert fields to dict
            d = dict(fields)
            d["stream_id"] = mid
            rows.append(d)
        
        # Advance to avoid reading same message
        ms, seq = last.split("-")
        last = f"{ms}-{int(seq) + 1}"
        
        if len(rows) % 10000 == 0:
            print(f"   Exported {len(rows)} rows...")
    
    print(f"✅ Exported {len(rows)} total labels")
    return rows


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="Export labels from Redis Stream to Parquet/CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export day's labels
  python3 export_labels.py \\
      --start "2025-10-25T00:00:00Z" \\
      --end "2025-10-26T00:00:00Z" \\
      --out labels.parquet
  
  # Export using epoch timestamps
  python3 export_labels.py \\
      --start 1729814400000 \\
      --end 1729900800000 \\
      --out labels.csv
        """
    )
    ap.add_argument("--start", required=True, help="Start time (ms epoch or ISO)")
    ap.add_argument("--end", required=True, help="End time (ms epoch or ISO)")
    ap.add_argument("--out", required=True, help="Output file (.parquet or .csv)")
    args = ap.parse_args()
    
    # Parse timestamps to Redis IDs
    start_id = parse_time(args.start)
    end_id = parse_time(args.end).replace("-0", "-999999")
    
    # Connect to Redis
    print(f"🔌 Connecting to Redis: {REDIS_URL}")
    r = redis.from_url(REDIS_URL, decode_responses=True)
    
    # Export labels
    rows = export_labels(r, start_id, end_id)
    
    if not rows:
        print("⚠️  No labels found in time range")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(rows)
    
    # Save
    print(f"💾 Saving to {args.out}...")
    if args.out.endswith(".parquet"):
        df.to_parquet(args.out, index=False)
    else:
        df.to_csv(args.out, index=False)
    
    print(f"✅ Wrote {len(df)} rows to {args.out}")
    print(f"   Columns: {', '.join(df.columns)}")
    print(f"   Size: {os.path.getsize(args.out) / 1024:.1f} KB")
    
    # Summary statistics
    if "action" in df.columns:
        print("\n📊 Label Summary:")
        print(df["action"].value_counts())
    
    if "status" in df.columns:
        print("\n📊 Status Summary:")
        print(df["status"].value_counts())


if __name__ == "__main__":
    main()

