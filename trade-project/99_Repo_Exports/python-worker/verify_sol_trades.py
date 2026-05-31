import os
import redis
import time
from datetime import datetime, timezone

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
SYMBOL = "SOLUSDT"
SOURCE = "CryptoOrderFlow"
WINDOW_END_STR = "2026-01-15 05:00:00"
WINDOW_SECONDS = 3600

def check_trades():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    
    # Calculate timestamps
    end_dt = datetime.strptime(WINDOW_END_STR, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    end_ts_ms = int(end_dt.timestamp() * 1000)
    start_ts_ms = end_ts_ms - (WINDOW_SECONDS * 1000)
    
    print(f"Checking trades for {SYMBOL} ({SOURCE})")
    print(f"Window: {WINDOW_SECONDS}s")
    print(f"Start: {start_ts_ms} ({datetime.fromtimestamp(start_ts_ms/1000, tz=timezone.utc)})")
    print(f"End:   {end_ts_ms} ({datetime.fromtimestamp(end_ts_ms/1000, tz=timezone.utc)})")
    
    # 1. Check Stream
    print("\n--- Checking trades:closed Stream ---")
    # xrevrange: max, min.  We want from End down to Start.
    # Note: Stream IDs are timestamp-seq.
    max_id = f"{end_ts_ms}-99999"
    min_id = f"{start_ts_ms}-0"
    
    try:
        # Read all (loop if needed, but for 370 trades one call might be enough, let's use count 10000)
        entries = r.xrevrange("trades:closed", max=max_id, min=min_id, count=10000)
        print(f"Raw entries in window: {len(entries)}")
        
        matched_trades = []
        for _id, fields in entries:
            s_source = fields.get("source") or fields.get("strategy") or ""
            s_symbol = fields.get("symbol") or ""
            
            # Simple normalization
            if "SOLUSDT" not in s_symbol.upper():
                continue
            if SOURCE.lower() not in s_source.lower() and "orderflow" not in s_source.lower():
                continue
                
            matched_trades.append(fields)
            
        print(f"Matched {SYMBOL} trades: {len(matched_trades)}")
        
        if matched_trades:
            durations = []
            for t in matched_trades:
                entry_ts = float(t.get("entry_ts_ms") or t.get("entry_time") or 0)
                exit_ts = float(t.get("exit_ts_ms") or t.get("closed_time") or t.get("close_time") or 0)
                
                # Normalize seconds to ms if needed (simple heuristic)
                if entry_ts < 10000000000: entry_ts *= 1000
                if exit_ts < 10000000000: exit_ts *= 1000
                
                if exit_ts > entry_ts > 0:
                    durations.append((exit_ts - entry_ts) / 1000.0)
            
            if durations:
                avg_dur = sum(durations) / len(durations)
                print(f"Avg Duration: {avg_dur:.2f}s")
                print(f"Min Duration: {min(durations):.2f}s")
                print(f"Max Duration: {max(durations):.2f}s")
            else:
                print("No valid durations found.")
                
    except Exception as e:
        print(f"Error reading stream: {e}")

    # 2. Check ZSET if exists
    zkey = f"closed_z:cryptoorderflow:solusdt:tick:{SOURCE.lower()}" 
    # Try generic too
    zkey_gen = f"closed_z:cryptoorderflow:solusdt:tick"
    
    print(f"\n--- Checking ZSET {zkey} ---")
    try:
        count = r.zcount(zkey, start_ts_ms, end_ts_ms)
        print(f"Count in ZSET: {count}")
    except:
        print("ZSET not found or error")

    print(f"\n--- Checking ZSET {zkey_gen} ---")
    try:
        count = r.zcount(zkey_gen, start_ts_ms, end_ts_ms)
        print(f"Count in ZSET (generic): {count}")
    except:
        print("ZSET generic not found or error")

if __name__ == "__main__":
    check_trades()
