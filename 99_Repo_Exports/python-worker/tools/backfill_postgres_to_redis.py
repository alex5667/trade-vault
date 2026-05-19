#!/usr/bin/env python3
"""Backfill last N postgres trades into Redis streams.

trades:closed  — flat fields matching calibrator schema (ml_prob, result, r_multiple, …)
ml_replay_inputs_v1 — nested {"payload": JSON} for replay consumers
"""
import json
import os
import random

import psycopg2
import redis

from core.redis_keys import RedisStreams as RS, STREAM_RETENTION
from utils.time_utils import get_ny_time_millis


def now_ms() -> int:
    return get_ny_time_millis()


print("Connecting to Redis...")
r = redis.Redis(host="redis-worker-1", port=6379, db=0)

print("Connecting to Postgres...")
pg_dsn = os.getenv(
    "PG_DSN",
    f"postgresql://trading:{os.getenv('TRADING_PASSWORD', 'trading_password')}"
    "@scanner-postgres:5432/scanner_analytics",
)
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

_maxlen_closed = STREAM_RETENTION.get(RS.TRADES_CLOSED, 10_000)
_maxlen_replay = STREAM_RETENTION.get(RS.ML_REPLAY_INPUTS, 50_000)

print(f"Fetched {len(rows)} trades. Publishing to Redis...")
count = 0
for row in rows:
    sid, symbol, tf, strategy, exit_ts_ms, r_mult, config_json, cov_bucket, applied = row

    if not config_json:
        config_json = {}

    random.seed(sid)
    cov_bucket = cov_bucket or random.choice(["a", "b", "c", "d"])
    applied = applied if applied is not None else random.choice([0, 1])

    r_mult_val: float = float(r_mult) if r_mult is not None else 0.0
    p: float = 0.5  # no ml_prob in postgres backfill — use neutral default
    ts_close: int = int(exit_ts_ms) if exit_ts_ms else now_ms()

    if r_mult_val > 0:
        result = "WIN"
    elif r_mult_val < 0:
        result = "LOSS"
    else:
        result = "BE"

    # ── trades:closed: flat dict — calibrator (p_edge, ml_outcome_tracker) reads these
    r.xadd(
        RS.TRADES_CLOSED,
        {
            "sid": str(sid),
            "symbol": str(symbol),
            "result": result,
            "r_multiple": str(r_mult_val),
            "ml_prob": str(p),
            "ts_close": str(ts_close),
            "ts_decision": str(ts_close - 60_000),
            "market_regime": "*",  # not available from postgres backfill
            "kind": "*",           # not available from postgres backfill
            "source": "postgres_backfill",
        },
        maxlen=_maxlen_closed,
        approximate=True,
    )

    # ── ml_replay_inputs_v1: nested payload (replay consumers expect this format)
    decision = {
        "sid": sid,
        "symbol": symbol,
        "tf": tf,
        "strategy": strategy,
        "ts_ms": ts_close - 60_000,
        "p_cal": p,
        "p_raw": p,
        "ml_state": "ok",
        "dq_state": "ok",
        "drift_state": "ok",
        "rule_score": 100,
        "meta_enforce_cov_bucket": cov_bucket,
        "config": config_json.get("config", {}),
        "indicators": config_json.get("indicators", {}),
    }
    close_event = {
        "event_type": "POSITION_CLOSED",
        "sid": sid,
        "symbol": symbol,
        "tf": tf,
        "close_ts_ms": ts_close,
        "r_mult": r_mult_val,
        "meta_enforce_cov_bucket": cov_bucket,
        "meta_enforce_applied": applied,
    }
    r.xadd(
        RS.ML_REPLAY_INPUTS,
        {"payload": json.dumps({"ver": "p55", "sid": sid, "close": close_event,
                                "decision": decision, "ts_ms": now_ms(),
                                "source": "postgres_backfill"})},
        maxlen=_maxlen_replay,
        approximate=True,
    )
    count += 1

print(f"Successfully published {count} items to trades:closed and ml_replay_inputs_v1.")
