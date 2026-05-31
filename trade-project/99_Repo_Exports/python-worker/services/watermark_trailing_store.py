"""Phase B.2: Redis-persisted storage для WatermarkTrailingFSM.

Redis key: `trail:wm:{sid}` (hash).
TTL:       по умолчанию 7 дней — типичный максимум жизни позиции.

Producer: trade_monitor / watermark_tracker_runner после TP-hit.
Consumer: тот же поток. State хранится между тиками, чтобы переживать перезапуск.
"""

from __future__ import annotations

import os

import redis

from common.log import setup_logger
from services.watermark_trailing import WatermarkSnapshot, WatermarkTrailingFSM

log = setup_logger("watermark_trailing_store")

KEY_PREFIX = "trail:wm:"
DEFAULT_TTL_SEC = 7 * 24 * 3600


class WatermarkStore:
    def __init__(self, redis_client: redis.Redis | None = None, ttl_sec: int = DEFAULT_TTL_SEC):
        if redis_client is None:
            redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            self.r: redis.Redis = redis.from_url(redis_url, decode_responses=True)
        else:
            self.r = redis_client
        self.ttl_sec = ttl_sec

    @staticmethod
    def _key(sid: str) -> str:
        return f"{KEY_PREFIX}{sid}"

    def load(self, sid: str) -> WatermarkSnapshot | None:
        try:
            data = self.r.hgetall(self._key(sid))
        except Exception as e:
            log.warning("WatermarkStore.load failed for sid=%s: %s", sid, e)
            return None
        if not data:
            return None
        try:
            return WatermarkSnapshot.from_dict(data)  # type: ignore[arg-type]
        except Exception as e:
            log.warning("WatermarkStore.load: cannot parse snapshot sid=%s: %s", sid, e)
            return None

    def save(self, snap: WatermarkSnapshot) -> None:
        try:
            key = self._key(snap.sid)
            pipe = self.r.pipeline()
            pipe.hset(key, mapping=snap.to_dict())  # type: ignore[arg-type]
            pipe.expire(key, self.ttl_sec)
            pipe.execute()
        except Exception as e:
            log.warning("WatermarkStore.save failed for sid=%s: %s", snap.sid, e)

    def delete(self, sid: str) -> None:
        try:
            self.r.delete(self._key(sid))
        except Exception as e:
            log.warning("WatermarkStore.delete failed for sid=%s: %s", sid, e)

    def load_fsm(self, sid: str) -> WatermarkTrailingFSM | None:
        snap = self.load(sid)
        return WatermarkTrailingFSM(snap=snap) if snap else None
