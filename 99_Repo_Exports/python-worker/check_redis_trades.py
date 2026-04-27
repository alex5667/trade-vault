import redis
import json

def run():
    r = redis.Redis(host="redis-worker-1", port=6379, decode_responses=False)
    res = r.xrevrange("trades:closed", count=30)
    for _id, fields in res:
        # Decode fields and print
        f = {k.decode('utf-8'): v.decode('utf-8') for k, v in fields.items()}
        print({
            "order_id": f.get("order_id"), 
            "status": f.get("status"),
            "close_reason": f.get("close_reason"),
            "is_virtual": f.get("is_virtual"),
            "pnl_net": f.get("pnl_net"),
            "tp1_hit": f.get("tp1_hit"),
            "tp1_touched": f.get("tp1_touched"),
            "tp2_hit": f.get("tp2_hit")
        })

if __name__ == "__main__":
    run()
