"""One-time processing script for TB labeler to process pending/old data."""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from typing import Any, Dict

import redis

from services.tb_labeler_worker_v10_1 import TBLabelerWorker, _safe_loads

# Config (same as worker)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
OF_INPUTS_STREAM = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
OF_INPUTS_FIELD = os.getenv("OF_INPUTS_FIELD", "payload")
TB_INPUTS_GROUP = os.getenv("TB_INPUTS_GROUP", "tb-labeler")
TB_INPUTS_CONSUMER = os.getenv("TB_INPUTS_CONSUMER", "c1")


def process_pending(r: redis.Redis, limit: int = 1000) -> int:
    """Process pending messages from consumer group."""
    group = os.getenv("TB_INPUTS_GROUP", "tb-labeler")
    consumer = os.getenv("TB_INPUTS_CONSUMER", "c1")
    
    processed = 0
    worker = TBLabelerWorker()
    
    # Check pending
    try:
        pending = r.xpending_range(OF_INPUTS_STREAM, group, min="-", max="+", count=limit)
        if pending:
            print(f"📋 Found {len(pending)} pending messages")
            for p in pending:
                msg_id = p["message_id"]
                # Claim and process
                try:
                    claimed = r.xclaim(OF_INPUTS_STREAM, group, consumer, 0, [msg_id])
                    if claimed:
                        for _stream, msgs in [(OF_INPUTS_STREAM, claimed)]:
                            for _msg_id, fields in msgs:
                                raw = fields.get(OF_INPUTS_FIELD)
                                inp = _safe_loads(raw)
                                worker.enqueue_job(inp)
                                r.xack(OF_INPUTS_STREAM, group, _msg_id)
                                processed += 1
                except Exception as e:
                    print(f"⚠️  Error claiming {msg_id}: {e}")
    except Exception as e:
        print(f"⚠️  Error checking pending: {e}")
    
    return processed


def process_new(r: redis.Redis, limit: int = 1000) -> int:
    """Process new messages from stream."""
    group = os.getenv("TB_INPUTS_GROUP", "tb-labeler")
    consumer = os.getenv("TB_INPUTS_CONSUMER", "c1")
    
    processed = 0
    worker = TBLabelerWorker()
    
    # Ensure group exists
    try:
        r.xgroup_create(OF_INPUTS_STREAM, group, id="0", mkstream=True)
    except Exception:
        pass
    
    # Read new messages
    try:
        resp = r.xreadgroup(group, consumer, {OF_INPUTS_STREAM: ">"}, count=limit, block=100)
        if resp:
            for _stream, msgs in resp:
                for msg_id, fields in msgs:
                    raw = fields.get(OF_INPUTS_FIELD)
                    inp = _safe_loads(raw)
                    worker.enqueue_job(inp)
                    r.xack(OF_INPUTS_STREAM, group, msg_id)
                    processed += 1
    except Exception as e:
        print(f"⚠️  Error reading new messages: {e}")
    
    return processed


def process_from_stream_direct(r: redis.Redis, since_hours: float = 24.0, limit: int = 5000) -> int:
    """Process messages directly from stream (bypass consumer group)."""
    import time
    processed = 0
    skipped_done = 0
    skipped_invalid = 0
    worker = TBLabelerWorker()
    
    since_ms = get_ny_time_millis() - int(since_hours * 3600_000)
    
    try:
        # Read from stream starting from since_ms
        last_id = f"{since_ms}-0"
        batch = r.xrange(OF_INPUTS_STREAM, min=last_id, max="+", count=limit)
        
        if batch:
            print(f"📥 Found {len(batch)} messages in stream (since {since_hours}h ago)")
            sample_checked = False
            for msg_id, fields in batch:
                raw = fields.get(OF_INPUTS_FIELD)
                inp = _safe_loads(raw)
                
                # Debug first few messages
                if not sample_checked and processed + skipped_done + skipped_invalid < 3:
                    print(f"   🔍 Sample message: msg_id={msg_id}, has_payload={raw is not None}, parsed_keys={list(inp.keys())[:5] if inp else 'None'}")
                    sample_checked = True
                
                if not inp:
                    skipped_invalid += 1
                    continue
                
                # Check required fields
                sid = str(inp.get("sid", "") or "")
                symbol = str(inp.get("symbol", "") or "").upper()
                ts_ms = int(inp.get("ts_ms", inp.get("ts", 0)) or 0)
                direction = str(inp.get("direction", "") or "").upper()
                
                # Generate sid if missing (from symbol + ts_ms + direction)
                if not sid and symbol and ts_ms > 0 and direction:
                    import hashlib
                    sid_raw = f"{symbol}:{ts_ms}:{direction}"
                    sid = hashlib.sha256(sid_raw.encode()).hexdigest()[:16]
                    inp["sid"] = sid
                
                if not sid or not symbol or ts_ms <= 0 or direction not in ("LONG", "SHORT"):
                    skipped_invalid += 1
                    if skipped_invalid <= 3:
                        print(f"   ⚠️  Invalid: sid={sid}, symbol={symbol}, ts_ms={ts_ms}, direction={direction}")
                    continue
                
                # Check if already done
                sid = str(inp.get("sid", ""))
                done_key = f"tb:done:{sid}"
                if r.exists(done_key):
                    skipped_done += 1
                    continue
                
                # Try to enqueue
                try:
                    worker.enqueue_job(inp)
                    processed += 1
                except Exception as e:
                    skipped_invalid += 1
                    if processed + skipped_done + skipped_invalid <= 10:
                        print(f"   ⚠️  Skipped {sid}: {e}")
            
            if skipped_done > 0:
                print(f"   ⏭️  Skipped {skipped_done} already processed (tb:done)")
            if skipped_invalid > 0:
                print(f"   ⚠️  Skipped {skipped_invalid} invalid/missing fields")
    except Exception as e:
        print(f"⚠️  Error reading from stream: {e}")
    
    return processed


def process_due_jobs(worker: TBLabelerWorker, limit: int = 200) -> int:
    """Process due jobs."""
    return worker.process_due(limit=limit)


def main() -> None:
    ap = argparse.ArgumentParser(description="One-time TB labeler processing")
    ap.add_argument("--pending", action="store_true", help="Process pending messages")
    ap.add_argument("--new", action="store_true", help="Process new messages")
    ap.add_argument("--due", action="store_true", help="Process due jobs")
    ap.add_argument("--direct", action="store_true", help="Process directly from stream (bypass consumer group)")
    ap.add_argument("--since-hours", type=float, default=24.0, help="Hours to look back for direct processing")
    ap.add_argument("--all", action="store_true", help="Process all (pending + new + due + direct)")
    ap.add_argument("--limit", type=int, default=1000, help="Limit for processing")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", REDIS_URL))
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    worker = TBLabelerWorker()

    if args.all:
        args.pending = True
        args.new = True
        args.due = True
        args.direct = True

    total = 0

    if args.pending:
        print("🔄 Processing pending messages...")
        p = process_pending(r, limit=args.limit)
        total += p
        print(f"   Processed {p} pending messages")

    if args.new:
        print("🔄 Processing new messages...")
        n = process_new(r, limit=args.limit)
        total += n
        print(f"   Processed {n} new messages")

    if args.direct:
        print(f"🔄 Processing directly from stream (since {args.since_hours}h)...")
        d = process_from_stream_direct(r, since_hours=args.since_hours, limit=args.limit)
        total += d
        print(f"   Enqueued {d} jobs from stream")

    if args.due:
        print("🔄 Processing due jobs...")
        d = process_due_jobs(worker, limit=args.limit)
        total += d
        print(f"   Processed {d} due jobs")

    if not (args.pending or args.new or args.due or args.direct):
        print("❌ No action specified. Use --pending, --new, --due, --direct, or --all")
        return

    print(f"\n✅ Total processed: {total}")
    
    # Check results
    tb_len = r.xlen("labels:tb")
    jobs_len = r.zcard("tb:jobs:due")
    print(f"📊 Current state: labels:tb={tb_len}, tb:jobs:due={jobs_len}")


if __name__ == "__main__":
    main()

