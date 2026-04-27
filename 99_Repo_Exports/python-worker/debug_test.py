import json
import time
import fakeredis
from utils.atr_cache import ATRCache, _now_ms

def debug_test():
    r = fakeredis.FakeRedis(decode_responses=True)
    c = ATRCache()
    c.redis_client = r
    c.max_age_ms = 60_000
    sym = "ETHUSDT"
    nm = _now_ms()

    # atr:json is stale
    ts = nm - 120_000
    r.set(f"atr:json:{sym}:1m", json.dumps({"atr": 5.0, "ts": ts}))
    # tracker is present (no ts, accepted)
    r.hset(f"ATR:{sym}:M1", mapping={"atr": "4.8"})

    print(f"DEBUG: now={nm}, ts={ts}, age={nm-ts}, max_age={c.max_age_ms}")

    # Manually check candidates
    cands = c._candidates_for_tf(sym=sym, tf_norm="M1", tf_raw="1m", nm=nm)
    for ca in cands:
        print(f"CAND: src={ca.source} atr={ca.atr} ts={ca.ts_ms} age={ca.age_ms} fresh={ca.fresh_ok}")

    pick = c._select_best(cands)
    if pick:
        print(f"PICK: {pick.source} {pick.atr}")
    else:
        print("PICK: None")

    atr, meta = c.get_with_meta(sym, "1m", now_ms=nm)
    print(f"RESULT: atr={atr} src={meta.get('source')}")

if __name__ == "__main__":
    debug_test()
