import redis
import os
import json

def main():
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        stream = "metrics:of_gate"
        
        print(f"Reading from {stream} (filtering for non-dn_veto)...")
        # Read last 100 messages
        data = r.xrevrange(stream, count=100)
        found = 0
        for msg_id, fields in data:
            if "type" not in fields:
                print(f"MsgID: {msg_id}")
                print(f"Top-level keys: {list(fields.keys())}")
                ok = fields.get("ok", "MISSING")
                ok_soft = fields.get("ok_soft", "MISSING")
                meta_veto = fields.get("meta_veto", "MISSING")
                latency = fields.get("latency_us", "MISSING")
                print(f"ok={ok}, ok_soft={ok_soft}, meta_veto={meta_veto}, latency={latency}")
                if "payload" in fields:
                     print("Payload present")
                print("-" * 20)
                found += 1
                if found >= 5:
                    break
        if found == 0:
             print("No non-dn_veto messages found in last 100 entries")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
