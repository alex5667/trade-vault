import time
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List

import redis

log = logging.getLogger("news-feature-store")


def ema(prev: float, x: float, alpha: float) -> float:
    return (alpha * x) + ((1.0 - alpha) * prev)

def safe_float(x: Any, d: float = 0.0) -> float:
    try: return float(x)
    except Exception: return d

def safe_int(x: Any, d: int = 0) -> int:
    try: return int(float(x))
    except Exception: return d

@dataclass(frozen=True)
class FSConfig:
    redis_url: str = "redis://redis-worker-1:6379/0"
    stream_in: str = "news:analysis"
    group: str = "news-feature-store"
    consumer: str = "news-feature-store-1"
    block_ms: int = 5000
    count: int = 200

    feature_ttl_sec: int = 3600
    alpha: float = 0.20  # EMA агрессивность

    key_prefix: str = "news:agg:"


class NewsFeatureStore:
    def __init__(self, cfg: FSConfig, r: redis.Redis):
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

            for _, entries in msgs:
                pipe = self.r.pipeline(transaction=False)
                ack_ids: List[str] = []

                for msg_id, f in entries:
                    ack_ids.append(msg_id)
                    self._apply_one(pipe, f)

                # ack пачкой
                if ack_ids:
                    pipe.xack(self.cfg.stream_in, self.cfg.group, *ack_ids)
                pipe.execute()

    def _apply_one(self, pipe: redis.client.Pipeline, f: Dict[str, Any]) -> None:
        # compact поля из analyzer
        risk = safe_float(f.get("risk"), 0.0)
        surprise = safe_float(f.get("surprise"), 0.0)
        grade_id = safe_int(f.get("grade_id"), 0)
        tags_mask = safe_int(f.get("tags_mask"), 0)
        primary_tag_id = safe_int(f.get("primary_tag_id"), 0)
        ref = str(f.get("ref") or "")
        ts_ms = safe_int(f.get("analyzed_ts_ms"), int(time.time() * 1000))

        # impacted_symbols (если решите добавить) – иначе GLOBAL
        impacted = []
        try:
            impacted = json.loads(f.get("impacted_symbols") or "[]")
        except Exception:
            impacted = []

        targets = impacted if impacted else ["GLOBAL"]

        for sym in targets:
            key = f"{self.cfg.key_prefix}{sym}"

            # Читаем текущие значения (можно оптимизировать: HGETALL не всегда нужен).
            cur = self.r.hgetall(key) or {}
            prev_risk = safe_float(cur.get("risk_ema"), 0.0)
            prev_sur = safe_float(cur.get("surprise_ema"), 0.0)
            prev_grade = safe_int(cur.get("grade_id"), 0)
            prev_tags = safe_int(cur.get("tags_mask"), 0)

            new_risk = ema(prev_risk, risk, self.cfg.alpha)
            new_sur = ema(prev_sur, surprise, self.cfg.alpha)

            # grade: берем максимум (headline risk лучше "переоценить", чем недооценить)
            new_grade = max(prev_grade, grade_id)

            # tags_mask: OR + можно сделать decay отдельным механизмом (упрощаем)
            new_tags = prev_tags | tags_mask

            pipe.hset(key, mapping={
                "risk_ema": str(new_risk),
                "surprise_ema": str(new_sur),
                "grade_id": str(new_grade),
                "tags_mask": str(new_tags),
                "primary_tag_id": str(primary_tag_id),
                "last_ref": ref,
                "last_ts_ms": str(ts_ms),
            })
            pipe.expire(key, self.cfg.feature_ttl_sec)
