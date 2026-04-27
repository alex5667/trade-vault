#!/usr/bin/env python3
"""
ack_sre_alert.py

Tool to ACK SRE alerts (suppress notifications) or manually set delivery receipts.

Usage:
  # ACK by kind/scope
  python ack_sre_alert.py --kind meta_freeze --scope ALL --ttl_sec 21600

  # ACK by direct key
  python ack_sre_alert.py --ack_key "sre:ack:cfg_sugg:meta_freeze:ALL" --ttl_sec 21600

  # Set receipt (stop retries)
  python ack_sre_alert.py --receipt_id "rcpt:xxxxxxxxxxxxxxxx" --ttl_sec 3600
"""
import os
import sys
import argparse
import logging
from redis import Redis

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("ack_sre_alert")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--kind", help="Suggestion kind (e.g. meta_freeze)")
    parser.add_argument("--scope", help="Suggestion scope (e.g. ALL)")
    parser.add_argument("--ack_key", help="Direct ACK key override")
    parser.add_argument("--receipt_id", help="Receipt ID to mark as delivered (e.g. rcpt:...)")
    parser.add_argument("--ttl_sec", type=int, default=3600, help="TTL in seconds (default 1h)")
    parser.add_argument("--prefix", default="sre:ack:cfg_sugg:", help="Prefix for constructed ACK keys")
    parser.add_argument("--receipt_prefix", default="notify:receipt:", help="Prefix for receipt keys")

    args = parser.parse_args()

    redis = Redis.from_url(args.redis_url, decode_responses=True)

    # Mode 1: Receipt
    if args.receipt_id:
        key = args.receipt_prefix + args.receipt_id if not args.receipt_id.startswith(args.receipt_prefix) else args.receipt_id
        # For receipts, we just need existence, value doesn't strictly matter but timestamp is good
        import time
        val = str(int(time.time()))
        redis.setex(key, args.ttl_sec, val)
        logger.info(f"Set RECEIPT {key} = {val} (ttl={args.ttl_sec}s)")
        return

    # Mode 2: ACK
    key = args.ack_key
    if not key:
        if not args.kind or not args.scope:
            logger.error("Must provide either --ack_key OR (--kind AND --scope) OR --receipt_id")
            sys.exit(1)
        key = f"{args.prefix}{args.kind}:{args.scope}"

    # Set the ACK
    # Value can be user info or timestamp
    import time
    val = f"acked_at_{int(time.time())}"
    redis.setex(key, args.ttl_sec, val)
    logger.info(f"Set ACK {key} = {val} (ttl={args.ttl_sec}s)")

if __name__ == "__main__":
    main()
