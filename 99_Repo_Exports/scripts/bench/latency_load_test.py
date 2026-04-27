#!/usr/bin/env python3
import time
import json
import redis
import argparse
from datetime import datetime
import os
import sys

def get_redis_url_worker1():
    return os.getenv("REDIS_URL", "redis://localhost:63791/0")

def main():
    parser = argparse.ArgumentParser(description="Latency Audit Benchmark (P4.1 Integration)")
    parser.add_argument("--stream", default="signals:crypto:raw", help="Signal stream to audit")
    parser.add_argument("--count", type=int, default=10, help="Number of signals to analyze")
    parser.add_argument("--url", default=get_redis_url_worker1(), help="Redis URL (worker-1)")
    
    args = parser.parse_args()
    
    try:
        r = redis.from_url(args.url, decode_responses=True)
        r.ping()
    except Exception as e:
        print(f"❌ Cannot connect to Redis at {args.url}: {e}")
        sys.exit(1)

    print(f"📊 Analyzing last {args.count} signals and P4.1 states...")
    
    # Check P4.1 Summary
    slo_summary_key = "metrics:latency_contract:slo:last"
    summary = r.hgetall(slo_summary_key)
    if summary:
        print("\n=== P4.1 SLO Summary ===")
        print(f"Gate OK: {'✅' if summary.get('gate_ok') == '1' else '❌'}")
        print(f"Required Stages: {summary.get('required_total')}")
        print(f"Present Stages:  {summary.get('present_total')}")
        print(f"Missing Stages:  {summary.get('missing_total')}")
        print(f"Stale Stages:    {summary.get('stale_total')}")
        print(f"Budget Breaches: {summary.get('budget_breach_total')}")
        print("-" * 40)
    else:
        print("\n⚠️ P4.1 SLO Summary not found.")

    # Show some specific stages for BTCUSDT
    print("\n=== P4.1 Stage Latencies (Last Observed) ===")
    stages = [
        ("go_ingest", "ingest_source_to_redis"),
        ("python_worker", "redis_to_feature"),
        ("python_worker", "feature_to_emit"),
        ("nest_gateway", "end_to_end_event"),
    ]
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    
    print(f"{'Service':<15} | {'Stage':<22} | {'Symbol':<10} | {'Latency (ms)'}")
    print("-" * 70)
    for svc, stage in stages:
        for sym in symbols:
            key = f"metrics:latency_contract:last:{svc}:{stage}:{sym}"
            data = r.hgetall(key)
            if data:
                lat = data.get("last_duration_ms", "N/A")
                print(f"{svc:<15} | {stage:<22} | {sym:<10} | {lat}ms")
            else:
                pass # skip missing

    # Signals
    print("\n=== Recent Signals (Stream Audit) ===")
    try:
        messages = r.xrevrange(args.stream, count=args.count)
    except Exception as e:
        print(f"❌ Error reading stream {args.stream}: {e}")
        return

    if not messages:
        print("⚠️ No signals found in stream.")
        return

    print("-" * 100)
    print(f"{'Symbol':<10} | {'Event Time (Wall)':<20} | {'E2E (ms)':<8}")
    print("-" * 100)

    for msg_id, data in messages:
        payload_raw = data.get("payload")
        if not payload_raw: continue
        try: payload = json.loads(payload_raw)
        except: continue

        symbol = payload.get("symbol", "N/A")
        ts_event_ms = payload.get("ts_event_ms")
        if not ts_event_ms: continue
            
        ts_event = int(ts_event_ms)
        ts_msg = int(msg_id.split("-")[0]) # Redis ID is a good proxy for write time
        e2e = ts_msg - ts_event
        
        event_dt = datetime.fromtimestamp(ts_event/1000).strftime('%H:%M:%S.%f')[:-3]
        print(f"{symbol:<10} | {event_dt:<22} | {e2e:<8}")

if __name__ == "__main__":
    main()
