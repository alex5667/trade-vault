import redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
msgs = r.xrevrange('metrics:of_gate', max='+', min='-', count=100)
for _, data in msgs:
    if str(data.get('ok')) == '0':
        print(f"telemetry_ok=0 | reason={data.get('reason')} score={data.get('score')} have={data.get('have')} need={data.get('need')}")
