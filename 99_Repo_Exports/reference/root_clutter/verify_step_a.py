import os
import redis
from collections import defaultdict, Counter

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

def get_redis_client():
    return redis.from_url(REDIS_URL, decode_responses=True)

def norm_map(data):
    return {k: v for k, v in data.items()}

def analyze_trades():
    try:
        r = get_redis_client()
        r.ping()
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return

    entries = r.xrevrange("trades:closed", count=300)
    trades = []
    for _id, fields in entries:
        t = norm_map(fields)
        trades.append(t)
        
    print(f"Fetched {len(trades)} trades.")
    
    # 1. Inspect Raw Reasons
    raw_counts = Counter()
    norm_counts = Counter()
    
    # 2. Inspect TP Losses
    tp_losses = []
    
    for t in trades:
        raw = t.get("close_reason_raw", "N/A")
        norm = t.get("close_reason", "N/A")
        pnl = float(t.get("pnl_net") or 0.0)
        
        raw_counts[raw] += 1
        norm_counts[norm] += 1
        
        # Check TP loss
        if "TP" in raw.upper() and pnl < -1e-9:
            tp_losses.append({
                "id": t.get("order_id"),
                "raw": raw,
                "pnl": pnl,
                "fees": float(t.get("fees") or 0.0), 
                "gross": float(t.get("pnl_gross") or 0.0)
            })

    print("\n=== Unique Close Reasons Raw ===")
    for k, v in raw_counts.most_common():
        print(f"{k:<40} : {v}")
        
    print("\n=== Unique Close Reasons Norm ===")
    for k, v in norm_counts.most_common():
        print(f"{k:<40} : {v}")
        
    print(f"\n=== TP Losses Analysis (Count: {len(tp_losses)}) ===")
    for x in tp_losses[:5]:
        print(f"ID: {x['id']} | Raw: {x['raw']} | PnL: {x['pnl']:.4f} (Gross: {x['gross']:.4f}, Fees: {x['fees']:.4f})")
        
if __name__ == "__main__":
    analyze_trades()
