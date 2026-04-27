from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set

import redis

from news_pipeline.stream_worker import StreamWorker

try:
    from news_pipeline.postgres_writer import NewsPostgresWriter
except Exception:  # pragma: no cover
    NewsPostgresWriter = None  # type: ignore


log = logging.getLogger("calendar_store")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

CALENDAR_EVENTS_STREAM = os.getenv("CALENDAR_EVENTS_STREAM", "calendar:events")
GROUP = os.getenv("CALENDAR_STORE_GROUP", "calendar-store")
CONSUMER = os.getenv("CALENDAR_STORE_CONSUMER", os.getenv("HOSTNAME", "calendar-store-1"))
DLQ = os.getenv("CALENDAR_STORE_DLQ", "calendar:events:dlq")

# TTL for calendar:agg:<scope> hashes.
CALENDAR_AGG_TTL_SEC = int(os.getenv("CALENDAR_AGG_TTL_SEC", str(2 * 3600)))  # 2h
# TTL for storing heavy calendar:event:<uid> JSON (optional but useful).
CALENDAR_EVENT_TTL_SEC = int(os.getenv("CALENDAR_EVENT_TTL_SEC", str(7 * 24 * 3600)))  # 7d

# Which scopes should exist in redis. We keep it small and aligned with ctx.asset_class.
KNOWN_SCOPES = {"crypto", "fx", "forex", "metals"}

# Major currencies => treat as cross-asset macro risk.
MAJOR_CCY = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD", "CNY"}

POSTGRES_DSN = os.getenv("NEWS_POSTGRES_DSN", os.getenv("POSTGRES_DSN", ""))


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _safe_str(v: Any) -> str:
    try:
        return str(v or "")
    except Exception:
        return ""


def importance_to_event_grade_id(importance: int) -> int:
    """Map Go importance (0..3) -> event_grade_id (0..4).

    Keep it simple and monotonic:
      0 -> 0 (none)
      1 -> 1 (low)
      2 -> 2 (medium)
      3 -> 4 (high/critical)
    """
    if importance <= 0:
        return 0
    if importance == 1:
        return 1
    if importance == 2:
        return 2
    return 4


def derive_scopes(country: str, currency: str, title: str) -> Set[str]:
    """Decide which asset_class scopes should be impacted by this calendar event.

    Rationale (trading/system view):
    - Macro calendar (USD/EUR events) affects FX and metals directly, and crypto indirectly.
    - We keep scopes aligned with OrderflowSignalContext.asset_class.
    - We err on the side of *including* crypto for major macro events to avoid false negatives.

    Rules:
    - If currency is major => scopes = {"fx", "metals", "crypto"}
    - Else => scopes = {"fx"}
    - If title/country hints a metals-specific event => add "metals".
    """
    ccy = (currency or "").strip().upper()
    ctry = (country or "").strip().upper()
    t = (title or "").lower()

    scopes: Set[str] = set()

    if ccy in MAJOR_CCY or ctry in {"US", "EU", "GB", "JP", "CN"}:
        scopes.update({"fx", "metals", "crypto"})
    else:
        scopes.add("fx")

    # crude hinting: some event titles explicitly mention gold/commodities
    if "gold" in t or "xau" in t or "commod" in t:
        scopes.add("metals")

    return scopes


@dataclass(slots=True)
class CalendarEvent:
    uid: str
    event_ts_ms: int
    ingested_ts_ms: int
    country: str
    currency: str
    title: str
    importance: int
    forecast: str
    previous: str
    unit: str
    source: str
    payload: str


class CalendarStoreWorker(StreamWorker):
    """Consumes calendar:events stream and maintains next-event aggregates.

    Redis:
      calendar:agg:<scope> (HASH)
        next_ts_ms        : int64
        event_tminus_sec  : int
        event_grade_id    : int
        event_ref         : str  ("calendar:event:<uid>")
        asof_ts_ms        : int64

      calendar:event:<uid> (STRING JSON, optional)
        used for debug/UX/backtests; does not touch tick loop.

    Postgres (optional):
      calendar_events           - raw events (uid PK)
      calendar_features_scope   - time series of next-event features per scope
    """

    def __init__(self, *, redis: redis.Redis, pg: Optional["NewsPostgresWriter"] = None):
        super().__init__(
            redis=redis,
            stream=CALENDAR_EVENTS_STREAM,
            group=GROUP,
            consumer=CONSUMER,
            dlq_stream=DLQ,
            block_ms=2000,
            count=200,
            claim_idle_ms=60_000,
        )
        self.pg = pg
        self._flush_deadline_ms = 0

    def _parse_event(self, fields: Dict[str, Any]) -> Optional[CalendarEvent]:
        uid = _safe_str(fields.get("uid"))
        if not uid:
            return None

        # Go emits these exact keys:
        # uid, event_ts_ms, ingested_ts_ms, country, currency, title, importance, forecast, previous, unit, source, payload
        event_ts_ms = _safe_int(fields.get("event_ts_ms"), 0)
        ing_ts_ms = _safe_int(fields.get("ingested_ts_ms"), 0) or int(time.time() * 1000)

        return CalendarEvent(
            uid=uid,
            event_ts_ms=event_ts_ms,
            ingested_ts_ms=ing_ts_ms,
            country=_safe_str(fields.get("country")),
            currency=_safe_str(fields.get("currency")),
            title=_safe_str(fields.get("title")),
            importance=_safe_int(fields.get("importance"), 0),
            forecast=_safe_str(fields.get("forecast")),
            previous=_safe_str(fields.get("previous")),
            unit=_safe_str(fields.get("unit")),
            source=_safe_str(fields.get("source")) or "fmp",
            payload=_safe_str(fields.get("payload")),
        )

    def handle_message(self, msg_id: str, fields: Dict[str, Any]) -> None:
        ev = self._parse_event(fields)
        if ev is None:
            return

        now_ms = int(time.time() * 1000)

        # Store heavy JSON (optional). This is NOT used by tick loop.
        try:
            ref_key = f"calendar:event:{ev.uid}"
            payload_obj = {
                "uid": ev.uid,
                "event_ts_ms": ev.event_ts_ms,
                "ingested_ts_ms": ev.ingested_ts_ms,
                "country": ev.country,
                "currency": ev.currency,
                "title": ev.title,
                "importance": ev.importance,
                "forecast": ev.forecast,
                "previous": ev.previous,
                "unit": ev.unit,
                "source": ev.source,
                "payload": ev.payload,
            }
            self.r.set(ref_key, json.dumps(payload_obj, ensure_ascii=False), ex=CALENDAR_EVENT_TTL_SEC)
        except Exception:
            # fail-open: no heavy storage
            ref_key = f"calendar:event:{ev.uid}"

        # Optional Postgres raw persistence
        if self.pg is not None:
            try:
                self.pg.insert_calendar_event(
                    uid=ev.uid,
                    event_ts_ms=ev.event_ts_ms,
                    ingested_ts_ms=ev.ingested_ts_ms,
                    country=ev.country,
                    currency=ev.currency,
                    title=ev.title,
                    importance=ev.importance,
                    forecast=ev.forecast,
                    previous=ev.previous,
                    unit=ev.unit,
                    source=ev.source,
                    payload_json={"payload": ev.payload} if ev.payload else {},
                )
            except Exception:
                pass

        scopes = derive_scopes(ev.country, ev.currency, ev.title)
        event_grade_id = importance_to_event_grade_id(ev.importance)

        # Update each scope's "next event" if this event is earlier than current.
        for scope in scopes:
            scope_norm = scope.strip().lower()
            if scope_norm == "forex":
                scope_norm = "fx"
            if scope_norm not in KNOWN_SCOPES:
                continue

            agg_key = f"calendar:agg:{scope_norm}"
            cur = self.r.hgetall(agg_key) or {}
            cur_next = _safe_int(cur.get("next_ts_ms"), 0)

            # Ignore events with no timestamp.
            if ev.event_ts_ms <= 0:
                continue

            # If current next event already passed, accept any future event.
            # Otherwise accept only strictly earlier future event.
            should_update = False
            if cur_next <= 0 or cur_next <= now_ms:
                should_update = ev.event_ts_ms >= now_ms
            else:
                if ev.event_ts_ms >= now_ms and ev.event_ts_ms < cur_next:
                    should_update = True

            if not should_update:
                continue

            tminus_sec = int((ev.event_ts_ms - now_ms) / 1000)
            mapping = {
                "next_ts_ms": int(ev.event_ts_ms),
                "event_tminus_sec": int(tminus_sec),
                "event_grade_id": int(event_grade_id),
                "event_ref": ref_key,
                "asof_ts_ms": int(now_ms),
            }

            pipe = self.r.pipeline(transaction=False)
            pipe.hset(agg_key, mapping=mapping)
            pipe.expire(agg_key, CALENDAR_AGG_TTL_SEC)
            pipe.execute()

            # Optional Postgres feature persistence
            if self.pg is not None:
                try:
                    self.pg.insert_calendar_feature_scope(
                        scope=scope_norm,
                        ts_ms=now_ms,
                        next_event_ts_ms=ev.event_ts_ms,
                        event_grade_id=event_grade_id,
                        event_ref=ref_key,
                        event_tminus_sec=tminus_sec,
                    )
                    # We insert per update, events are low-frequency.
                except Exception:
                    pass


def main() -> None:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True, health_check_interval=30)

    pg = None
    if POSTGRES_DSN and NewsPostgresWriter is not None:
        try:
            pg = NewsPostgresWriter(dsn=POSTGRES_DSN)
            pg.ensure_schema()
        except Exception as e:
            log.warning("Postgres disabled (init failed): %s", e)
            pg = None

    CalendarStoreWorker(redis=r, pg=pg).run_forever()


if __name__ == "__main__":
    main()
