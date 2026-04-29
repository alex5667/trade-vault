import json
import redis
from typing import Any
from utils.time_utils import get_ny_time_millis

def cache_ml_decision(
    r: redis.Redis,
    *,
    sid: str,
    symbol: str,
    bucket: str,
    p_edge: float,
    enforce: int,
    ok_rule: int,
    missing: int,
    model_ver: str,
    ttl_sec: int = 7 * 24 * 3600,
) -> None:
    """
    Cache ML decision for outcome emitter join.
    Writes to ml:dec:{sid} key with TTL (default 7 days).
    """
    if not r or not sid:
        return

    key = f"ml:dec:{sid}"
    payload = {
        "sid": sid,
        "symbol": str(symbol).upper(),
        "bucket": str(bucket).lower(),
        "p_edge": float(p_edge),
        "enforce": int(enforce),
        "ok_rule": int(ok_rule),
        "missing": int(missing),
        "model_ver": str(model_ver),
        "ts_ms": int(get_ny_time_millis()),
    }
    try:
        r.set(key, json.dumps(payload, separators=(",", ":")), ex=ttl_sec)
    except Exception:
        pass
