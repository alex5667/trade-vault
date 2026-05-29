"""manip_gate_runtime_overrides.py

Reader for MANIP Gate Autocalibrator dynamic thresholds.
Fetches the `autocal:manip_gate:state` snapshot from Redis.
"""

import json
import logging
import time
from typing import Any

from core.redis_client import get_redis
from core.redis_keys import RK

logger = logging.getLogger("manip-cal-reader")

class ManipGateCalReader:
    def __init__(self, r: Any = None, ttl_sec: int = 30):
        self.r = r or get_redis()
        self.ttl_sec = ttl_sec
        self._cache_ts: float = 0.0
        self._cache_data: dict[str, Any] = {}

    def _refresh(self) -> None:
        now = time.time()
        if now - self._cache_ts < self.ttl_sec:
            return
            
        try:
            raw = self.r.get(RK.AUTOCAL_MANIP_GATE)
            if raw:
                snap = json.loads(raw if isinstance(raw, str) else raw.decode())
                
                # Only apply if promoted to ENFORCE
                if snap.get("promoted", False):
                    self._cache_data = snap.get("bins", {})
                else:
                    self._cache_data = {}
            else:
                self._cache_data = {}
            
            self._cache_ts = now
        except Exception as exc:
            logger.debug("ManipGateCalReader _refresh failed: %s", exc)
            self._cache_ts = now # prevent spamming

    def get_thresholds(self, symbol: str) -> dict[str, float] | None:
        self._refresh()
        return self._cache_data.get(symbol)

_GLOBAL_READER: ManipGateCalReader | None = None

def get_reader() -> ManipGateCalReader:
    global _GLOBAL_READER
    if _GLOBAL_READER is None:
        _GLOBAL_READER = ManipGateCalReader()
    return _GLOBAL_READER
