import redis
import json
import time

r = redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True)
stream_name = 'metrics:ml_confirm'

print("Checking recent ML Confirm Gate decisions...")
try:
    messages = r.xrevrange(stream_name, max='+', min='-', count=10)
    
    if not messages:
        print("No recent messages in metrics:ml_confirm.")
    
    for msg_id, msg_data in messages:
        try:
            raw_data = msg_data.get('data') or msg_data.get(b'data')
            if not raw_data:
                continue
            if isinstance(raw_data, bytes):
                raw_data = raw_data.decode('utf-8')
                
            data = json.loads(raw_data)
            
            ts_ms = int(msg_id.split('-')[0])
            age_sec = (time.time() * 1000 - ts_ms) / 1000
            
            p_edge = data.get('p_edge')
            kind = data.get('kind')
            mode = data.get('mode')
            symbol = data.get('symbol')
            
            print(f"[{age_sec:.1f}s ago] {symbol} | kind: {kind} | mode: {mode} | p_edge: {p_edge}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            
except Exception as e:
    import traceback
    traceback.print_exc()
