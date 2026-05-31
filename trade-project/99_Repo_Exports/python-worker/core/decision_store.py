import json
from typing import Any

from .decision_record import DecisionRecord
from .redis_client import get_redis
from .redis_keys import RedisStreams as RS


class DecisionStore:
    """
    Persistence layer for DecisionRecords.
    """
    def __init__(self, redis_client=None):
        self.redis = redis_client or get_redis()

    def save_decision(self, record: DecisionRecord, expire_sec: int = 86400 * 3):
        """
        Saves decision to Redis key decision:{sid} as canonical JSON.
        Sets expiration (default 3 days).
        """
        key = f"decision:{record.sid}"
        payload = json.dumps(record.to_dict(), ensure_ascii=False, separators=(",", ":"), default=str)
        self.redis.set(key, payload, ex=expire_sec)

    def publish_decision(self, record: DecisionRecord, max_len: int = 10000):
        """
        Publishes decision to decisions:final stream using the canonical payload JSON field.
        """
        stream_key = RS.DECISIONS_FINAL
        payload = json.dumps(record.to_dict(), ensure_ascii=False, separators=(",", ":"), default=str)
        self.redis.xadd(
            stream_key,
            {
                "sid": str(record.sid),
                "symbol": str(record.symbol),
                "ts_ms": str(record.ts),
                "payload": payload,
            },
            maxlen=max_len,
            approximate=True,
        )

    def load_decision(self, sid: str) -> DecisionRecord | None:
        """
        Loads decision from Redis Hash decision:{sid}.
        Returns None if not found.
        """
        key = f"decision:{sid}"
        raw = self.redis.get(key)
        if raw:
            return self._parse_json_record(raw)

        # Backward-compatible read path for historical Hash records.
        data = self.redis.hgetall(key)
        if not data:
            return None
        return DecisionRecord.parse_from_redis(data)

    @staticmethod
    def _parse_json_record(raw: Any) -> DecisionRecord:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        data = json.loads(str(raw))
        return DecisionRecord.from_dict(data)
