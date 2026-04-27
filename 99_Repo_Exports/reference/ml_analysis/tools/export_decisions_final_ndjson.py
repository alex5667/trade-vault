#!/usr/bin/env python3
"""
export_decisions_final_ndjson.py

Exports "decisions:final" stream to NDJSON for offline analysis.
"""

import os
import sys
import json
import time
import argparse
import logging
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("export_decisions")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--stream", default="decisions:final")
    parser.add_argument("--out", required=True, help="Output file path (NDJSON)")
    parser.add_argument("--max", type=int, default=100000, help="Max items to export")
    parser.add_argument("--hours", type=float, default=24, help="Max age in hours")
    args = parser.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    
    start_ms = int((time.time() - args.hours * 3600) * 1000)
    start_id = f"{start_ms}-0"
    
    logger.info(f"Reading {args.stream} from {start_id} (last {args.hours}h)")
    
    count = 0
    with open(args.out, "w") as f:
        last_id = start_id
        while count < args.max:
            items = r.xrange(args.stream, min=last_id, count=1000)
            if not items:
                break
                
            chunk_count = 0
            for eid, fields in items:
                # "payload" field usually contains the full JSON record
                # We can write just the payload, or the stream fields + payload nested.
                # Let's write the parsed payload if available, else fields.
                
                record = {}
                if "payload" in fields:
                    try:
                        record = json.loads(fields["payload"])
                    except:
                        record = fields
                else:
                    record = fields
                
                # Ensure ID is preserved
                if "stream_id" not in record:
                    record["stream_id"] = eid
                
                f.write(json.dumps(record) + "\n")
                
                last_id = eid
                chunk_count += 1
                count += 1
                if count >= args.max:
                    break
            
            # Advance ID
            part = last_id.split("-")
            if len(part) == 2:
                ts, seq = map(int, part)
                last_id = f"{ts}-{seq+1}"
            else:
                break # weird id
            
            if chunk_count == 0:
                break

    logger.info(f"Exported {count} items to {args.out}")

if __name__ == "__main__":
    main()
