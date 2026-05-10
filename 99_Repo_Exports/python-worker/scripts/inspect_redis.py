
import json
import os

import redis
from core.redis_keys import RedisStreams as RS

# Default to localhost if not set
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

def connect_redis():
    try:
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        return r
    except Exception as e:
        print(f"Failed to connect to Redis at {REDIS_URL}: {e}")
        return None

def inspect_trades_stream(r):
    print("\n--- Inspecting trades:closed STREAM (Last 5) ---")
    try:
        entries = r.xrevrange(RS.TRADES_CLOSED, max="+", count=5)
        if not entries:
            print("No entries found in trades:closed.")
            return

        for msg_id, data in entries:
            print(f"\nID: {msg_id}")
            print(f"Symbol: {data.get('symbol')}, Source: {data.get('source')}, PnL: {data.get('pnl')}")
            # ... (details omitted for brevity unless found)

    except Exception as e:
        print(f"Error reading trades:closed: {e}")

def inspect_trades_zset(r, strategy="CryptoOrderFlow", symbol="ETHUSDT", tf="tick", source="CryptoOrderFlow"):
    # closed_z:{strategy}:{symbol}:{tf}:{source}
    zkey = f"closed_z:{strategy}:{symbol}:{tf}:{source}"
    print(f"\n--- Inspecting ZSET {zkey} (Last 5) ---")
    try:
        # Get last 5 timestamps (scores)
        # zrevrange returns list of members
        members = r.zrevrange(zkey, 0, 4)
        if not members:
            # Try without source suffix just in case
            zkey_short = f"closed_z:{strategy}:{symbol}:{tf}"
            print(f"No members in {zkey}. Trying {zkey_short}...")
            members = r.zrevrange(zkey_short, 0, 4)
            if not members:
                print(f"No members found in {zkey_short} either.")
                return
            zkey = zkey_short

        print(f"Found {len(members)} order IDs in {zkey}")

        for oid in members:
            order_key = f"order:{oid}"
            data = r.hgetall(order_key)
            if not data:
                print(f"Order {oid} not found in Redis (hash {order_key} missing)")
                continue

            print(f"\nOrder ID: {oid}")
            print(f"Symbol: {data.get('symbol')}, Source: {data.get('source')}, PnL: {data.get('pnl')}")

            sp_str = data.get('signal_payload')
            if sp_str and sp_str != "{}":
                try:
                    sp = json.loads(sp_str)
                    print("signal_payload keys:", list(sp.keys()))
                    indicators = sp.get('indicators', {})
                    print("indicators keys:", list(indicators.keys()))

                    if 'of_confirm' in indicators:
                        of_c = indicators['of_confirm']
                        print(f"of_confirm found: {type(of_c)}")
                    else:
                        print("of_confirm found: NO")

                    if 'ml_stats' in sp:
                         print("ml_stats found: Yes")
                    else:
                         print("ml_stats found: NO")

                except Exception as e:
                    print(f"Error parsing signal_payload: {e}")
            else:
                print("signal_payload: MISSING or EMPTY")

    except Exception as e:
        print(f"Error reading ZSET {zkey}: {e}")

def inspect_signals(r, symbol="ETHUSDT"):
    stream_key = f"signals:cryptoorderflow:{symbol}"
    print(f"\n--- Inspecting {stream_key} (Last 5) ---")
    try:
        entries = r.xrevrange(stream_key, max="+", count=5)
        if not entries:
            print(f"No entries found in {stream_key}.")
            return

        for msg_id, data in entries:
            print(f"\nID: {msg_id}")
            payload_str = data.get('payload') or data.get('data')

            if payload_str:
                try:
                    payload = json.loads(payload_str)
                    val_status = payload.get('validation_status')
                    print(f"validation_status: {val_status}")

                    indicators = payload.get('indicators', {})
                    of_ok = indicators.get('of_confirm_ok')
                    print(f"of_confirm_ok: {of_ok}")

                except Exception as e:
                    print(f"Error parsing payload: {e}")
            else:
                print("payload: MISSING")

    except Exception as e:
        print(f"Error reading signals stream {stream_key}: {e}")

def main():
    r = connect_redis()
    if r:
        inspect_trades_stream(r)
        inspect_trades_zset(r, "CryptoOrderFlow", "ETHUSDT", "tick", "CryptoOrderFlow")
        inspect_signals(r, "ETHUSDT")

if __name__ == "__main__":
    main()
