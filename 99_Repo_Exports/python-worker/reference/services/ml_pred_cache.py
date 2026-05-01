from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import redis

from common.redis_errors import retry_redis_operation


def _pred_key(sid: str) -> str:
    """Generate Redis key for prediction cache."""
    return f"ml:pred:{sid}"


def cache_pred(
    r: redis.Redis,
    *,
    sid: str,
    payload: Dict[str, Any],
    ttl_sec: Optional[int] = None,
) -> None:
    """Cache ML prediction for outcome joiner.
    
    Args:
        r: Redis client (decode_responses=True)
        sid: Signal ID
        payload: Prediction payload (sid, ts_ms, symbol, scenario_v4, p_edge, p_edge_chal, model_ver, chal_ver, enforce, mode)
        ttl_sec: TTL in seconds (default: ML_PRED_TTL_SEC env or 14 days)
    """
    if ttl_sec is None:
        ttl_sec = int(os.getenv("ML_PRED_TTL_SEC", "1209600") or 1209600)  # 14d
    retry_redis_operation(
        lambda: r.set(_pred_key(sid), json.dumps(payload, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec),
        operation_name="cache_pred set",
    )


def get_pred(r: redis.Redis, sid: str) -> Optional[Dict[str, Any]]:
    """Retrieve cached ML prediction.
    
    Args:
        r: Redis client (decode_responses=True)
        sid: Signal ID
        
    Returns:
        Prediction payload dict or None if not found
    """
    raw = retry_redis_operation(
        lambda: r.get(_pred_key(sid)),
        operation_name="get_pred get",
    )
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None

