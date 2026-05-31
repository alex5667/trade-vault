#!/usr/bin/env python3
"""
export_trade_closed_ndjson.py

Exports "trades:closed" stream to NDJSON for offline analysis.
"""

import os
import sys
import json
import time
import argparse
import logging
import redis
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("export_trade_closed")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--stream", default="trades:closed")
    parser.add_argument("--out", required=True, help="Output file path (NDJSON)")
    parser.add_argument("--max", type=int, default=100000, help="Max items to export")
    parser.add_argument("--hours", type=float, default=24, help="Max age in hours (approx via check)")
    args = parser.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    
    # Check stream info
    try:
        info = r.xinfo_stream(args.stream)
        logger.info(f"Stream {args.stream} length: {info['length']}")
    except Exception as e:
        logger.warning(f"Stream info failed (might use 0): {e}")

    # Read range
    # We want last X hours?
    # Or just all?
    # Stream is capped. Let's just read from beginning or specific range.
    # "--hours" implies we filter by ID timestamp.
    
    start_ms = int((time.time() - args.hours * 3600) * 1000)
    start_id = f"{start_ms}-0"
    
    logger.info(f"Reading from {start_id} (last {args.hours}h)")
    
    count = 0
    with open(args.out, "w") as f:
        # XRANGE in chunks
        last_id = start_id
        while count < args.max:
            items = r.xrange(args.stream, min=last_id, count=1000)
            if not items:
                break
                
            chunk_count = 0
            for eid, fields in items:
                if eid == last_id and count > 0: # skip stored last_id if repeated (xrange is inclusive)
                    continue
                
                # Check duplication if we strictly iterate?
                # Actually xrange inclusive min. 
                # Better: use '(' prefix in next call? standard redis py might not support it in older ver.
                # Let's just verify ID > last_id if we loop.
                # Actually standard pattern:
                
                obj = {"id": eid}
                obj.update(fields)
                f.write(json.dumps(obj) + "\n")
                
                last_id = eid
                chunk_count += 1
                count += 1
                if count >= args.max:
                    break
            
            if chunk_count == 0:
                 # Should not happen unless empty?
                 # If items returned only 1 item equal to last_id
                 if len(items) == 1 and items[0][0] == last_id:
                     break
            
            # Prepare next ID: increment last_id slightly? 
            # Xrange min is inclusive. We can use exclusive range syntax if redis supports `(`
            # Or just parse ID.
            ts, seq = map(int, last_id.split("-"))
            last_id = f"{ts}-{seq+1}"

    logger.info(f"Exported {count} items to {args.out}")

if __name__ == "__main__":
    main()
