import redis
import json
import sys

try:
    r = redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True)
    msg = r.xrevrange('signals:of:inputs', count=1)
    if msg:
        payload = msg[0][1].get('payload', '{}')
        data = json.loads(payload)
        
        print("Is stop_bps in root?", "stop_bps" in data)
        print("Is atr_bps in root?", "atr_bps" in data)
        print("Is indicators in root?", "indicators" in data)
        inds = data.get("indicators", {})
        print("Value of stop_bps in indicators:", inds.get("stop_bps"))
        print("Value of atr_bps in indicators:", inds.get("atr_bps"))
    else:
        print("No messages found")
except Exception as e:
    print(f"Error: {e}")
