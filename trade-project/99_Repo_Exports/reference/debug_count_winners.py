
import os
import redis
import json
from collections import defaultdict

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

def main():
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        stream = "trades:closed"
        
        print(f"Connecting to {REDIS_URL}...")
        print(f"Reading stream {stream}...")
        
        all_trades = r.xrange(stream, min="-", max="+")
        print(f"Total trades found: {len(all_trades)}")
        
        stats = defaultdict(lambda: {"total": 0, "win": 0, "loss": 0, "breakeven": 0, "valid_win": 0})
        
        for _id, fields in all_trades:
            # Handle flat fields (no 'data' JSON wrapper)
            symbol = fields.get("symbol", "UNKNOWN")
            source = fields.get("source", "UNKNOWN")
            pnl_str = fields.get("pnl_net", "0.0")
            one_r_str = fields.get("one_r_money", "0.0")
            
            try:
                pnl = float(pnl_str)
                one_r = float(one_r_str)
            except ValueError:
                pnl = 0.0
                one_r = 0.0
            
            key = f"{source}:{symbol}"
            stats[key]["total"] += 1
            if pnl > 0:
                stats[key]["win"] += 1
                if one_r > 0.000001:
                    stats[key]["valid_win"] += 1
            elif pnl < 0:
                stats[key]["loss"] += 1
            else:
                stats[key]["breakeven"] += 1

        print("\n--- Trade Statistics per Symbol ---")
        for sym, s in stats.items():
            print(f"{sym}: Total={s['total']}, Win={s['win']} ({s['win']/s['total']*100:.1f}%), ValidWin(one_r>0)={s['valid_win']}, Loss={s['loss']}, BreakEven={s['breakeven']}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
