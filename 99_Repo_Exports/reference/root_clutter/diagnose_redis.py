import redis
import time
import json
import os

def diagnose():
    # Inside a container on scanner-network, we use 'redis-worker-1'
    r = redis.Redis(host='redis-worker-1', port=6379, decode_responses=True)
    
    stream = 'metrics:of_gate'
    now_ms = int(time.time() * 1000)
    window_ms = 60 * 60 * 1000
    start_ms = now_ms - window_ms
    
    print(f"Current Time: {now_ms} ({time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(now_ms/1000))} UTC)")
    print(f"Window Start: {start_ms} ({time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(start_ms/1000))} UTC)")
    
    try:
        # Read last 2000 messages
        batch = r.xrevrange(stream, count=2000)
    except Exception as e:
        print(f"Error reading stream: {e}")
        return

    if not batch:
        print("Stream is empty!")
        return

    symbols = {}
    for msg_id, fields in batch:
        sym = fields.get('symbol', 'unknown')
        ts_val = fields.get('ts_ms', fields.get('ts', 0))
        try:
            ts = int(float(ts_val))
        except:
            ts = 0
        
        if sym not in symbols:
            symbols[sym] = {'count': 0, 'min_ts': ts, 'max_ts': ts, 'latest_msg_id': msg_id}
        
        symbols[sym]['count'] += 1
        symbols[sym]['min_ts'] = min(symbols[sym]['min_ts'], ts)
        symbols[sym]['max_ts'] = max(symbols[sym]['max_ts'], ts)
        
    print("\nSymbol Stats (last 2000 msgs):")
    for sym, stats in symbols.items():
        lag_ms = now_ms - stats['max_ts']
        print(f"  {sym:12}: n={stats['count']:3} | max_ts={stats['max_ts']} (lag {lag_ms/1000:.1f}s) | min_ts={stats['min_ts']}")
        if stats['max_ts'] < start_ms:
            print(f"    WARNING: Latest msg for {sym} is BEFORE window start!")

    # Check for the break condition in the monitor
    print("\nSimulating monitor scan (start_ms={}):".format(start_ms))
    scanned = 0
    picked = 0
    for msg_id, fields in batch:
        scanned += 1
        ts_val = fields.get('ts_ms', fields.get('ts', 0))
        try:
            ts = int(float(ts_val))
        except:
            ts = 0
            
        if ts < start_ms:
            lag = now_ms - ts
            print(f"  STOP at msg {scanned}: ts={ts} < start_ms={start_ms} (lag {lag/1000:.1f}s) | sym={fields.get('symbol')}")
            break
        picked += 1
    
    print(f"\nScan Results: scanned={scanned}, picked={picked}")

if __name__ == "__main__":
    diagnose()
