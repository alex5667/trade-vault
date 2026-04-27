
import redis
import os
import json

def debug_stream():
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    
    print(f"Reading from {stream}...")
    try:
        data = r.xrevrange(stream, count=50)
    except Exception as e:
        print(f"Error reading stream: {e}")
        return

    print(f"Found {len(data)} records.")
    
    cnt_ok = 0
    cnt_soft = 0
    
    for msg_id, fields in data:
        ok = int(fields.get("ok", 0))
        ok_soft = int(fields.get("ok_soft", 0))
        have = fields.get("have")
        need = fields.get("need")
        score = fields.get("score")
        exec_risk = fields.get("exec_risk_norm")
        
        cnt_ok += ok
        cnt_soft += ok_soft
        
        print(f"ID={msg_id} OK={ok} SOFT={ok_soft} H/N={have}/{need} SCR={score} Risk={exec_risk}")
        
    print(f"Total OK: {cnt_ok}")
    print(f"Total SOFT: {cnt_soft}")

if __name__ == "__main__":
    debug_stream()
