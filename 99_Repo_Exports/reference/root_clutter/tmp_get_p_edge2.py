import redis, json
r = redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True)
keys = r.keys('trades:closed*')
if keys:
    k = keys[0]
    if r.type(k) == 'zset':
        evs = r.zrevrange(k, 0, 500)
    elif r.type(k) == 'list':
        evs = r.lrange(k, 0, 500)
    elif r.type(k) == 'stream':
        events = r.xrevrange(k, max='+', min='-', count=500)
        evs = [e[1].get('payload', '{}') for e in events if 'payload' in e[1]]
    else:
        evs = []
        
    for x in evs:
        try:
            d = json.loads(x)
            sp = d.get('signal_payload', {})
            if isinstance(sp, str):
                sp = json.loads(sp)
            ind = sp.get('indicators', {})
            of_confirm = ind.get('of_confirm', {})
            evd = of_confirm.get('evidence', {})
            ml_dec = evd.get('ml_decision') or evd.get('ml') or sp.get("ml") or {}
            
            if isinstance(ml_dec, str):
                ml_dec = json.loads(ml_dec)
                
            p_edge = float(ml_dec.get('p_edge', 0.0))
            if abs(p_edge - 0.5) < 0.01:
                print("FOUND 0.5:")
                print(json.dumps(d, indent=2))
                break
        except Exception as e:
            pass
