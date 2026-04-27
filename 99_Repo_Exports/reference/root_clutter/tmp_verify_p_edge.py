import redis
import json

r = redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True)
stream_name = 'trades:closed'

print("Fetching last 50 closed trades to verify p_edge...")
try:
    messages = r.xrevrange(stream_name, max='+', min='-', count=50)
    
    count_05 = 0
    count_other = 0
    
    for msg_id, msg_data in messages:
        try:
            raw_data = msg_data.get('data') or msg_data.get(b'data')
            if not raw_data:
                continue
            if isinstance(raw_data, bytes):
                raw_data = raw_data.decode('utf-8')
                
            trade_data = json.loads(raw_data)
            
            # Navigate nested structures
            inds = trade_data.get('indicators', {})
            # ML confirm can be directly in indicators or under ml_confirm_gate
            ml_metrics = inds.get('ml_confirm_gate', inds) 
            
            p_edge = ml_metrics.get('p_edge')
            if p_edge is not None:
                p_edge = float(p_edge)
                if abs(p_edge - 0.5) < 0.0001:
                    count_05 += 1
                else:
                    count_other += 1
                    print(f"[{msg_id}] symbol: {trade_data.get('symbol')}, p_edge: {p_edge:.4f}")
        except Exception as e:
            pass
    
    print(f"\nSummary:")
    print(f"Trades with p_edge == 0.50: {count_05}")
    print(f"Trades with normal p_edge: {count_other}")
    
except Exception as e:
    print(f"Error fetching from stream: {e}")
