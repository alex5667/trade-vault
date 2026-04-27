import redis, json
r = redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True)
events = r.xrevrange('trades:closed', max='+', min='-', count=500)
evs = [e[1] for e in events]

found = False
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
        if p == 0.5:
            print("FOUND 0.5 IN TRADES:CLOSED!")
            orig = sp.get('ml', sp.get('indicators', {}).get('of_confirm', {}).get('evidence', {}).get('ml_decision'))
            print("ORIGINAL ML DECISION:")
            print(json.dumps(orig if not isinstance(orig, str) else json.loads(orig), indent=2))
            found = True
            break
    except Exception as e:
        pass

if not found:
    print("Could not find p_edge=0.5 in the last 500 trades.")
