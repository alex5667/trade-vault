import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

while True:
    msgs = r.xrevrange("metrics:of_gate", "+", "-", count=20)
    for m_id, fields in msgs:
        if fields.get("symbol") == "1000PEPEUSDT":
            print(f"TS: {fields.get('ts_ms')} | Symbol: {fields.get('symbol')} | OK: {fields.get('ok')} | Need: {fields.get('need')} | Have: {fields.get('have')} | Score: {fields.get('score')} | Risk: {fields.get('exec_risk_bps')} | Reason: {fields.get('reason')}")
            break
    time.sleep(1)
