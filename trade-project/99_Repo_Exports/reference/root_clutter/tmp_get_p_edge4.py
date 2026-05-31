import redis, json
r = redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True)
events = r.xrevrange('trades:closed', max='+', min='-', count=500)
evs = [e[1] for e in events]

for d in evs:
    try:
        sp_raw = d.get('signal_payload', '{}')
        sp = json.loads(sp_raw) if isinstance(sp_raw, str) else sp_raw
        
        ind = sp.get('indicators', {})
        of_confirm = ind.get('of_confirm', {})
        evd = of_confirm.get('evidence', {})
        ml_dec = evd.get('ml_decision') or evd.get('ml') or sp.get('ml') or {}
        
        if isinstance(ml_dec, str):
            ml_dec = json.loads(ml_dec)
            
        p = float(ml_dec.get('p_edge', 0.0))
        if p > 0.49 and p < 0.51:
            print("FOUND 0.5 IN TRADES:CLOSED!")
            print("ML_DEC:")
            print(json.dumps(ml_dec, indent=2))
            break
    except Exception as e:
        pass
