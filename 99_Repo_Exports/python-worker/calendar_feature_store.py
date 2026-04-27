from utils.time_utils import get_ny_time_millis
import time
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List

import redis

log = logging.getLogger("calendar-feature-store")

def safe_int(x: Any, d: int = 0) -> int:
    try: return int(float(x))
    except Exception: return d

@dataclass(frozen=True)
class CalConfig:
    redis_url: str = "redis://redis-worker-1:6379/0"
    stream_in: str = "calendar:events"
    group: str = "calendar-feature-store"
    consumer: str = "calendar-feature-store-1"
    block_ms: int = 5000
    count: int = 200

    ttl_sec: int = 3600
    next_prefix: str = "calendar:next:"


class CalendarFeatureStore:
    def __init__(self, cfg: CalConfig, r: redis.Redis):
        self.cfg = cfg
        self.r = r

    def ensure_group(self) -> None:
        try:
            self.r.xgroup_create(self.cfg.stream_in, self.cfg.group, id="$", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" in str(e):
                return
            raise

    def run_forever(self) -> None:
        self.ensure_group()
        log.info("started stream=%s group=%s consumer=%s", self.cfg.stream_in, self.cfg.group, self.cfg.consumer)

        while True:
            msgs = self.r.xreadgroup(
                groupname=self.cfg.group,
                consumername=self.cfg.consumer,
                streams={self.cfg.stream_in: ">"},
                count=self.cfg.count,
                block=self.cfg.block_ms,
            )
            if not msgs:
                continue

            pipe = self.r.pipeline(transaction=False)
            ack_ids: List[str] = []

            for _, entries in msgs:
                for msg_id, f in entries:
                    ack_ids.append(msg_id)
                    self._apply_one(pipe, f)

            if ack_ids:
                pipe.xack(self.cfg.stream_in, self.cfg.group, *ack_ids)
            pipe.execute()

    def _apply_one(self, pipe: redis.client.Pipeline, f: Dict[str, Any]) -> None:
        # контракт from Go CalendarEvent.ToStreamFields()
        uid = str(f.get("uid") or "")
        currency = str(f.get("currency") or "").upper()
        event_ts_ms = safe_int(f.get("event_ts_ms"), 0)
        importance = safe_int(f.get("importance"), 0)

        if not currency or event_ts_ms <= 0:
            return

        # grade_id можно маппить из importance (1..5) → 0..3
        grade_id = 0
        if importance >= 5: grade_id = 3
        elif importance >= 4: grade_id = 2
        elif importance >= 3: grade_id = 1

        key = f"{self.cfg.next_prefix}{currency}"
        now_ms = get_ny_time_millis()

        # Если событие уже прошло — не делаем next
        if event_ts_ms < now_ms - 60_000:
            return

        # Сохраняем как "candidate next".
        # Если в key уже есть более раннее событие — не перетираем.
        cur = self.r.get(key)
        if cur:
            try:
                obj = json.loads(cur)
                cur_ts = int(obj.get("event_ts_ms") or 0)
                if cur_ts > 0 and cur_ts <= event_ts_ms:
                    return
            except Exception:
                pass

        obj = {"event_ts_ms": event_ts_ms, "grade_id": grade_id, "ref": f"calendar:event:{uid}"}
        pipe.set(key, json.dumps(obj, separators=(",", ":")), ex=self.cfg.ttl_sec)
