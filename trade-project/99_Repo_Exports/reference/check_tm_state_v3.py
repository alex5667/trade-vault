import os
import sys
import redis
import json
import time

# Add project root to sys.path
sys.path.append("/app/python-worker")
sys.path.append("/app")

print("STARTING DIAGNOSTIC SCRIPT V3", flush=True)

from services.trade_monitor import TradeMonitorService

def check():
    redis_url = os.getenv("REDIS_URL")
    print(f"Initializing TradeMonitorService with {redis_url}...", flush=True)
    try:
        # This will call _recover_open_positions()
        monitor = TradeMonitorService(redis_url=redis_url)
        print("TradeMonitorService INITIALIZED", flush=True)
        
        print(f"monitor.open_positions count: {len(monitor.open_positions)}", flush=True)
        print(f"monitor.open_by_symbol keys: {list(monitor.open_by_symbol.keys())}", flush=True)
        
        for sym, ids in monitor.open_by_symbol.items():
            print(f"Symbol {sym}: {len(ids)} positions", flush=True)
            for oid in list(ids)[:3]:
                pos = monitor.open_positions.get(oid)
                print(f"  - {oid}: symbol={pos.symbol if pos else 'N/A'}", flush=True)

        if not monitor.open_positions:
            print("CRITICAL: monitor.open_positions IS EMPTY after init!", flush=True)
            
    except Exception as e:
        print(f"TradeMonitorService init FAILED: {e}", flush=True)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check()
