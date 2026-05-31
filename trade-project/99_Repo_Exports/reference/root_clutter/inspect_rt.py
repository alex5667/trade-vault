import redis, json

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
keys = r.keys("of:state:*")
print(f"Found {len(keys)} state keys")
for k in keys[:3]:
    data = r.get(k)
    try:
        obj = json.loads(data)
        print(f"--- {k} ---")
        print("last_obi_event:", obj.get("last_obi_event"))
        print("last_div:", obj.get("last_div"))
        print("cont_ctx_ts_ms:", obj.get("cont_ctx_ts_ms"))
    except Exception as e:
        print("Error", e)
