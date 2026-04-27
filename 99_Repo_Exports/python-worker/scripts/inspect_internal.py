
import redis
import json
import os

def check():
    url = "redis://redis-worker-1:6379/0"
    try:
        r = redis.from_url(url, decode_responses=True)
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print("--- SCAN closed_z:* ---")
    keys = []
    cursor = "0"
    while True:
        cursor, partial = r.scan(cursor, match="closed_z:*", count=10000)
        keys.extend(partial)
        if cursor == "0":
            break
    print(f"Found {len(keys)} keys: {keys[:10]}")

    print("\n--- STREAM trades:closed (Last 5) ---")
    try:
        entries = r.xrevrange("trades:closed", max="+", count=5)
        for mid, data in entries:
            print(f"ID: {mid} Sym: {data.get('symbol')} Src: {data.get('source')} PnL: {data.get('pnl')}")
            sp = data.get('signal_payload')
            print(f"  Payload len: {len(sp) if sp else 0}")
            if sp:
                try:
                    js = json.loads(sp)
                    print(f"  Payload keys: {list(js.keys())}")
                    if 'indicators' in js:
                         print(f"  Indicators: {list(js['indicators'].keys())}")
                except:
                    print("  Invalid JSON payload")
    except Exception as e:
        print(f"Stream error: {e}")

    print("\n--- STREAM signals:cryptoorderflow:ETHUSDT (Last 5) ---")
    try:
        entries = r.xrevrange("signals:cryptoorderflow:ETHUSDT", max="+", count=5)
        for mid, data in entries:
             print(f"ID: {mid}")
             payload = data.get('payload') or data.get('data')
             if payload:
                 try:
                     js = json.loads(payload)
                     print(f"  Val: {js.get('validation_status')} OF_OK: {js.get('indicators',{}).get('of_confirm_ok')}")
                 except:
                     pass
    except Exception as e:
        print(f"Signal stream error: {e}")

if __name__ == "__main__":
    check()
