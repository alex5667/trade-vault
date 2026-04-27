from __future__ import annotations

import json
import os
import redis
from typing import Dict, Any

_redis_client = None

def _get_redis():
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    return _redis_client

def get_active_policy(source: str, symbol: str, scenario: str, regime: str, risk_horizon_bucket: str) -> Dict[str, Any]:
    """Fetch active policy with fallback chain."""
    r = _get_redis()
    
    source = str(source or "unknown")
    symbol = str(symbol or "unknown")
    scenario = str(scenario or "unknown")
    regime = str(regime or "unknown")
    risk_horizon_bucket = str(risk_horizon_bucket or "unknown")
    
    try:
        from services.atr_promotion_policy_metrics import atr_policy_resolver_hit_total
    except Exception:
        atr_policy_resolver_hit_total = None

    pol_res = None
    # 1. Exact match
    key1 = f"cfg:atr_policy:active:{source}:{symbol}:{scenario}:{regime}:{risk_horizon_bucket}"
    res = r.get(key1)
    if res:
        if atr_policy_resolver_hit_total:
            atr_policy_resolver_hit_total.labels(level="exact").inc()
        try: pol_res = json.loads(res)
        except Exception: pass
        
    # 2. Fallback regime=na
    if not pol_res:
        key2 = f"cfg:atr_policy:active:{source}:{symbol}:{scenario}:na:{risk_horizon_bucket}"
        res = r.get(key2)
        if res:
            if atr_policy_resolver_hit_total:
                atr_policy_resolver_hit_total.labels(level="fallback_regime").inc()
            try: pol_res = json.loads(res)
            except Exception: pass
        
    # 3. Fallback scenario=default, regime=na
    if not pol_res:
        key3 = f"cfg:atr_policy:active:{source}:{symbol}:default:na:{risk_horizon_bucket}"
        res = r.get(key3)
        if res:
            if atr_policy_resolver_hit_total:
                atr_policy_resolver_hit_total.labels(level="fallback_default").inc()
            try: pol_res = json.loads(res)
            except Exception: pass
        
    if not pol_res:
        if atr_policy_resolver_hit_total:
            atr_policy_resolver_hit_total.labels(level="miss").inc()
        return {}

    # Fetch rollout stages
    key_stop = f"cfg:atr_policy_rollout:state:{source}:{symbol}:{scenario}:{regime}:{risk_horizon_bucket}:stop_ttl"
    key_trail = f"cfg:atr_policy_rollout:state:{source}:{symbol}:{scenario}:{regime}:{risk_horizon_bucket}:trailing"
    
    stages = r.mget([key_stop, key_trail])
    pol_res["rollout_stage_stop_ttl"] = stages[0] or "shadow"
    pol_res["rollout_stage_trailing"] = stages[1] or "shadow"
    
    return pol_res

