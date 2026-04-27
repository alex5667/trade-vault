import redis, json
r = redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True)
keys = r.keys('trades:closed*')
if keys:
    res = []
    for k in keys:
        typ = r.type(k)
        evs = []
        try:
            if typ == 'zset':
                evs = r.zrevrange(k, 0, 500)
            elif typ == 'list':
                evs = r.lrange(k, 0, 500)
            elif typ == 'stream':
                events = r.xrevrange(k, max='+', min='-', count=500)
                evs = [e[1].get('payload', '{}') for e in events if 'payload' in e[1]]
                if not evs:
                    evs = [json.dumps(e[1]) for e in events]
        except Exception as e:
            print("Error reading key", k, e)
            
        for x in evs:
            try:
                d = json.loads(x)
                sp = d.get('signal_payload', {})
                if isinstance(sp, str):
                    sp = json.loads(sp)
                ind = sp.get('indicators', {})
                of_confirm = ind.get('of_confirm', {})
                evd = of_confirm.get('evidence', {})
                ml_dec = evd.get('ml_decision') or evd.get('ml') or {}
                
                if isinstance(ml_dec, str):
                    ml_dec = json.loads(ml_dec)
                    
                p_edge = float(ml_dec.get('p_edge', 0.0))
                # Store all p_edge instead of only > 0.0
                res.append(p_edge)
            except Exception as e:
                pass
    if res:
        avg = sum(res)/max(1, len(res))
        print("Found", len(res), "avg:", avg, "p_edges:", res[:10])
    else:
        print("Found no entries at all.")
else:
    print("No trades:closed keys")
