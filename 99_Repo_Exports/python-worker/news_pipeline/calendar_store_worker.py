from __future__ import annotations
from utils.time_utils import get_ny_time_millis
import os
import time
import logging
from typing import Any, Dict, List, Optional

import redis

from news_pipeline.stream_worker import StreamWorker
from news_pipeline.postgres_writer import NewsPostgresWriter

log = logging.getLogger("calendar_store")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CAL_STREAM = os.getenv("CALENDAR_EVENTS_STREAM", "calendar:events")
GROUP = os.getenv("CALENDAR_GROUP", "calendar-feature-store")
CONSUMER = os.getenv("CALENDAR_CONSUMER", os.getenv("HOSTNAME", "calendar-store-1"))
DLQ = os.getenv("CALENDAR_DLQ", "calendar:events:dlq")

AGG_TTL_SEC = int(os.getenv("CALENDAR_AGG_TTL_SEC", "3600"))
LOOKAHEAD_SEC = int(os.getenv("CALENDAR_LOOKAHEAD_SEC", str(7 * 24 * 3600)))

# Какие asset_class поддерживаем
DEFAULT_SCOPES = os.getenv("CALENDAR_DEFAULT_SCOPES", "crypto,forex,metals,equities")

HEARTBEAT_TTL_SEC = int(os.getenv("HEARTBEAT_TTL_SEC", "30"))
INSTANCE_ID = os.getenv("INSTANCE_ID", f"py-cal-store:{os.getpid()}")


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default

def _s(v: Any) -> str:
    try:
        return str(v or "").strip()
    except Exception:
        return ""

def importance_to_grade_id(importance: int) -> int:
    """
    В Go importance: 0..3
    Мы приводим к grade_id: 0..4
      0 -> 0
      1 -> 1 (low)
      2 -> 2 (medium)
      3 -> 4 (high)
    """
    if importance <= 0:
        return 0
    if importance == 1:
        return 1
    if importance == 2:
        return 2
    return 4

def map_scopes(currency: str, country: str, importance: int) -> List[str]:
    """
    Выбранная политика: важные события по USD/EUR/GBP/JPY/CNY -> всем классам.
    Иначе -> forex (+metals если medium/high).
    """
    ccy = (currency or "").upper()
    ctry = (country or "").upper()

    base_scopes = [x.strip().lower() for x in DEFAULT_SCOPES.split(",") if x.strip()]
    if not base_scopes:
        base_scopes = ["crypto", "forex", "metals", "equities"]

    major = {"USD", "EUR", "GBP", "JPY", "CNY", "CHF", "AUD", "NZD", "CAD"}
    if ccy in major and importance >= 2:
        return base_scopes

    scopes = ["forex"]
    if importance >= 2:
        scopes.append("metals")
        scopes.append("crypto")  # macro surprise часто двигает crypto тоже
    return list(dict.fromkeys(scopes))

class CalendarStoreWorker(StreamWorker):
    """
    Читает calendar:events и пишет:
      - Redis HASH calendar:agg:<asset_class> (next event)
      - Postgres calendar_events (сырье)
    """

    def __init__(self, *, redis: redis.Redis, pg: Optional[NewsPostgresWriter] = None):
        super().__init__(
            redis=redis,
            stream=CAL_STREAM,
            group=GROUP,
            consumer=CONSUMER,
            dlq_stream=DLQ,
            block_ms=2000,
            count=200,
            claim_idle_ms=60_000,
        )
        self.pg = pg
        self._last_hb = 0.0

    def on_idle(self) -> None:
        """Write hb:calendar heartbeat so news-watchdog does not warn."""
        now = time.time()
        if now - self._last_hb < (HEARTBEAT_TTL_SEC / 2):
            return
        self._last_hb = now
        try:
            import json as _json
            obj = {
                "ts_ms": int(now * 1000),
                "kind": "calendar",
                "ok": True,
                "err": "",
                "added": 0,
                "instance": INSTANCE_ID,
            }
            self.r.set("hb:calendar", _json.dumps(obj, separators=(",", ":")), ex=HEARTBEAT_TTL_SEC)
        except Exception:
            pass

    def handle_message(self, msg_id: str, fields: Dict[str, Any]) -> None:
        uid = _s(fields.get("uid"))
        if not uid:
            return

        event_ts_ms = _i(fields.get("event_ts_ms"), 0)
        ing_ts_ms = _i(fields.get("ingested_ts_ms"), get_ny_time_millis())

        country = _s(fields.get("country"))
        currency = _s(fields.get("currency"))
        title = _s(fields.get("title"))
        source = _s(fields.get("source")) or "unknown"
        payload = _s(fields.get("payload"))

        # совместимость: иногда могут прислать event_grade_id напрямую
        importance = _i(fields.get("importance"), 0)
        event_grade_id = _i(fields.get("event_grade_id"), 0)
        if event_grade_id <= 0:
            event_grade_id = importance_to_grade_id(importance)

        forecast = _s(fields.get("forecast"))
        previous = _s(fields.get("previous"))
        unit = _s(fields.get("unit"))

        # 1) Postgres сырье (fail-open)
        if self.pg is not None:
            try:
                self.pg.insert_calendar_event(
                    uid=uid,
                    event_ts_ms=event_ts_ms,
                    ingested_ts_ms=ing_ts_ms,
                    country=country,
                    currency=currency,
                    title=title,
                    importance=int(importance),
                    grade_id=int(event_grade_id),
                    forecast=forecast,
                    previous=previous,
                    unit=unit,
                    source=source,
                    payload_json=payload,
                )
            except Exception:
                pass

        # 2) Redis agg: выбираем "next event" per scope
        now_ms = get_ny_time_millis()
        tminus = int((event_ts_ms - now_ms) / 1000) if event_ts_ms > 0 else -1

        # если событие слишком далеко, можно игнорировать
        if event_ts_ms > 0 and tminus > LOOKAHEAD_SEC:
            return

        scopes = map_scopes(currency=currency, country=country, importance=importance)
        for ac in scopes:
            scope_norm = ac
            if scope_norm == "forex":
                scope_norm = "fx"
            
            key = f"calendar:agg:{scope_norm}"
            prev = self.r.hgetall(key) or {}
            prev_next = _i(prev.get("next_ts_ms"), 0)

            # Policy: keep the nearest FUTURE event for this scope.
            # NOTE: do not persist tminus as source-of-truth; downstream must recompute from event_ts_ms and now_ts_ms.
            should_write = (event_ts_ms > 0 and event_ts_ms >= now_ms and ((prev_next <= now_ms) or (prev_next > 0 and event_ts_ms < prev_next)))
            if should_write:
                pipe = self.r.pipeline(transaction=False)
                pipe.hset(key, mapping={
                "event_ts_ms": int(event_ts_ms),
                    "next_ts_ms": int(event_ts_ms),
                    "event_tminus_sec": int(tminus),
                    "event_grade_id": int(event_grade_id),
                    "event_ref": uid,
                    "updated_ts_ms": int(now_ms),
                    "currency": currency,
                    "country": country,
                    "title": title[:256],
                    "source": source,
                })
                pipe.expire(key, AGG_TTL_SEC)
                pipe.execute()
                
                # Optional: insert feature snapshot into Postgres
                if self.pg is not None:
                    try:
                        self.pg.insert_calendar_feature_scope(
                            scope=scope_norm,
                            ts_ms=now_ms,
                            next_event_ts_ms=event_ts_ms,
                            event_grade_id=event_grade_id,
                            event_ref=uid,
                            event_tminus_sec=tminus
                        )
                    except Exception:
                        pass

def main() -> None:
    try:
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True, health_check_interval=30)

        pg = None
        if (os.getenv("PG_ENABLED", "1").lower() not in {"0", "false", "no"}):
            pg = NewsPostgresWriter.from_env()
            pg.ensure_schema()

        CalendarStoreWorker(redis=r, pg=pg).run_forever()
    except BaseException as e:
        log.error(f"FATAL Exception in main: {e}", exc_info=True)
        import sys
        sys.exit(1)
    finally:
        log.info("calendar_store_worker main() completely exited.")
        import sys
        sys.stdout.flush()
        sys.stderr.flush()

if __name__ == "__main__":
    main()
