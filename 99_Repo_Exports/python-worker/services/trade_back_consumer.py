from __future__ import annotations

from typing import Any

from services.trade_closed_hydrator import hydrate_trade_closed
from core.redis_keys import RedisStreams as RS


class TradeBackConsumer:
    def __init__(self, redis, *args, **kwargs):
        self.redis = redis

    def _norm_map(self, m: dict[str, Any]) -> dict[str, str]:
        return {str(k): str(v) for k, v in (m or {}).items() if v is not None}

    def poll_recent(self, cutoff_ms: int, limit: int = 2000):
        min_id = f"{cutoff_ms}-0"
        entries = self.redis.xrevrange(RS.TRADES_CLOSED, max="+", min=min_id, count=limit) or []
        out = []
        for _id, fields in entries:
            t = self._norm_map(fields or {})
            # NEW: если stream в compact режиме (или не хватает полей) — подтягиваем order:{id}.
            # Это делает consumer стабильным при переключении TRADES_CLOSED_STREAM_COMPACT=1.
            t = hydrate_trade_closed(self.redis, t, require_closed=False, merge_precedence="hash")
            t = self._norm_map(t)
            out.append(t)
        return out
