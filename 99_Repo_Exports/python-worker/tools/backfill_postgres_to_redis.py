from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
import psycopg2
import redis
import json
import time

def now_ms(): return get_ny_time_millis()

print("Connecting to Redis...")
r = redis.Redis(host='redis-worker-1', port=6379, db=0)

import os

print("Connecting to Postgres...")
pg_dsn = os.getenv("PG_DSN", f"postgresql://trading:{os.getenv('TRADING_PASSWORD', 'trading_password')}@scanner-postgres:5432/scanner_analytics")
conn = psycopg2.connect(pg_dsn)
cur = conn.cursor()

print("Fetching last 20000 trades from trades_closed...")
cur.execute("""
    SELECT sid, symbol, tf, strategy, exit_ts_ms, r_multiple, 
           config_json, meta_enforce_cov_bucket, meta_enforce_applied
    FROM trades_closed 
    WHERE sid IS NOT NULL
    ORDER BY id DESC LIMIT 20000
""")
rows = cur.fetchall()
rows.reverse()

print(f"Fetched {len(rows)} trades. Publishing to Redis...")
count = 0
for row in rows:
    sid, symbol, tf, strategy, exit_ts_ms, r_mult, config_json, cov_bucket, applied = row

    if not config_json:
        config_json = {}

    import random
    random.seed(sid)
    cov_bucket = cov_bucket or random.choice(["a", "b", "c", "d"])
    applied = applied if applied is not None else random.choice([0, 1])

    y = 1 if r_mult is not None and r_mult >= 0.0 else 0
    p = 0.5
    
    # Emulate decision
    decision = {
        "sid": sid,
        "symbol": symbol,
        "tf": tf,
        "strategy": strategy,
        "ts_ms": exit_ts_ms - 60000 if exit_ts_ms else now_ms(),
        "p_cal": p,
        "p_raw": p,
        "ml_state": "ok",
        "dq_state": "ok",
        "drift_state": "ok",
        "rule_score": 100,
        "meta_enforce_cov_bucket": cov_bucket,
        "config": config_json.get("config", {}),
        "indicators": config_json.get("indicators", {})
    }

    # Emulate close event
    close_event = {
        "event_type": "POSITION_CLOSED",
        "sid": sid,
        "symbol": symbol,
        "tf": tf,
        "close_ts_ms": exit_ts_ms,
        "r_mult": r_mult,
        "meta_enforce_cov_bucket": cov_bucket,
        "meta_enforce_applied": applied,
    }

    # Emulate trades:closed
    trades_closed_payload = {
        "ver": "p55",
        "event_type": "POSITION_CLOSED",
        "sid": sid,
        "symbol": symbol,
        "tf": tf,
        "close_ts_ms": exit_ts_ms,
        "decision_ts_ms": exit_ts_ms - 60000 if exit_ts_ms else now_ms(),
        "r_mult": r_mult,
        "y": y,
        "ml_state": "ok",
        "dq_state": "ok",
        "drift_state": "ok",
        "meta_enforce_cov_bucket": cov_bucket,
        "meta_enforce_applied": applied,
        "ml_p": p,
        "ml_p_cal": p,
        "rule_score": 100,
        "source": "postgres_backfill"
    }

    # Emulate ml_replay_inputs_v1
    replay_payload = {
        "ver": "p55",
        "sid": sid,
        "close": close_event,
        "decision": decision,
        "ts_ms": now_ms(),
        "source": "postgres_backfill"
    }

    r.xadd("trades:closed", {"payload": json.dumps(trades_closed_payload)}, maxlen=50000)
    r.xadd("ml_replay_inputs_v1", {"payload": json.dumps(replay_payload)}, maxlen=50000)
    count += 1

print(f"Successfully published {count} items to trades:closed and ml_replay_inputs_v1.")
