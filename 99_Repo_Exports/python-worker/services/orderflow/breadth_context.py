import json
from typing import Optional, Dict, Any

async def aread_breadth_context(redis) -> Optional[Dict[str, Any]]:
    """Read the runtime:breadth context from Redis."""
    try:
        if not redis:
            return None
        raw = await redis.hgetall("runtime:breadth")
        if not raw:
            return None
        
        # Redis hgetall returns bytes keys and bytes values
        def _get_float(k: str) -> float:
            v = raw.get(k.encode()) or raw.get(k)
            if not v:
                return 0.0
            try:
                return float(v)
            except Exception:
                return 0.0

        return {
            "ret_24h": _get_float("ret_24h")
            "vol_z": _get_float("vol_z")
            "btc_ret": _get_float("btc_ret")
            "eth_ret": _get_float("eth_ret")
            "leader_confirm": _get_float("leader_confirm")
        }
    except Exception:
        return None
