from __future__ import annotations

import json
import logging

import redis

from . import config
from .models import CalendarEvent
from .redis_streams import ensure_group, xack, xreadgroup_block
from .utils import now_ms
import contextlib

log = logging.getLogger("calendar-feature-store")


def _event_key(event_id: str) -> str:
    return f"calendar:event:{event_id}"

def _idx_key_currency(cur: str) -> str:
    return f"calendar:idx:CUR:{cur}"

def _idx_key_region(reg: str) -> str:
    return f"calendar:idx:REG:{reg}"

def _idx_key_symbol(sym: str) -> str:
    return f"calendar:idx:SYM:{sym}"

def _agg_key(key: str) -> str:
    return f"calendar:agg:{key}"

def _keys_set() -> str:
    return "calendar:keys"  # set всех ключей, по которым есть индекс


def _touch_key(r: redis.Redis, key: str) -> None:
    # Запоминаем ключи (без TTL или с большим TTL)
    with contextlib.suppress(Exception):
        r.sadd(_keys_set(), key)


def _refresh_agg_for_key(r: redis.Redis, key: str, idx_zset: str) -> None:
    """
    Находим ближайшее событие >= now и пишем calendar:agg:<key>
    Храним только компактно:
      - next_ts_ms
      - event_tminus_sec
      - grade_id
      - event_ref
    """
    now = now_ms()
    try:
        # ближайший member с score >= now
        res = r.zrangebyscore(idx_zset, min=now, max="+inf", start=0, num=1, withscores=True)
        if not res:
            # если нет будущих — удалим agg (или обнулим)
            r.delete(_agg_key(key))
            return

        event_id, ts_score = res[0]
        ev_id = str(event_id)
        ts_ms = int(ts_score)

        # grade_id возьмём из event payload (можно хранить отдельно, но проще так)
        ev_raw = r.get(_event_key(ev_id)) or ""
        grade_id = 0
        try:
            payload = json.loads(ev_raw) if ev_raw else {}
            grade_id = int(payload.get("grade_id") or 0)
        except Exception:
            grade_id = 0

        tminus = max(0, int((ts_ms - now) / 1000))

        r.hset(
            _agg_key(key),
            mapping={
                "event_ts_ms": str(ts_ms),
                "next_ts_ms": str(ts_ms),
                "event_tminus_sec": str(tminus),
                "event_grade_id": str(int(grade_id)),
                "event_ref": _event_key(ev_id),
                "updated_ts_ms": str(int(now)),
            }
        )
        r.expire(_agg_key(key), int(config.CALENDAR_AGG_TTL_SEC))

    except Exception:
        # fail-open
        return


class CalendarFeatureStoreService:
    """
    ConsumerGroup:
      - читает calendar:events
      - сохраняет событие (heavy) в key calendar:event:<id> (TTL)
      - добавляет в индексы ZSET (currency/region/symbol)
      - обновляет агрегаты calendar:agg:<...>

    Tick-loop потом читает calendar:agg:* за 1 RTT.
    """

    def __init__(
        self,
        r: redis.Redis,
        consumer: str = "cal-fs-1",
        block_ms: int = 5000,
        batch: int = 50,
    ) -> None:
        self.r = r
        self.consumer = consumer
        self.block_ms = block_ms
        self.batch = batch
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def run_forever(self) -> None:
        ensure_group(self.r, config.CALENDAR_EVENTS_STREAM, config.CALENDAR_FEATURE_GROUP, mkstream=True)
        log.info("calendar-feature-store started consumer=%s", self.consumer)

        while not self._stop:
            items = xreadgroup_block(
                self.r,
                config.CALENDAR_EVENTS_STREAM,
                config.CALENDAR_FEATURE_GROUP,
                consumer=self.consumer,
                count=self.batch,
                block_ms=self.block_ms,
            )
            if not items:
                continue

            for _stream, msgs in items:
                for msg_id, fields in msgs.items():
                    try:
                        ev = CalendarEvent.from_stream_fields(fields)
                        if not ev.event_id:
                            xack(self.r, config.CALENDAR_EVENTS_STREAM, config.CALENDAR_FEATURE_GROUP, msg_id)
                            continue

                        # heavy save
                        ek = _event_key(ev.event_id)
                        heavy = {
                            "event_id": ev.event_id,
                            "title": ev.title,
                            "ts_ms": ev.ts_ms,
                            "grade_id": ev.grade_id,
                            "currency": ev.currency,
                            "region": ev.region,
                            "symbols": ev.symbols,
                            "payload": ev.payload,
                        }
                        self.r.set(ek, json.dumps(heavy, ensure_ascii=False))
                        self.r.expire(ek, int(config.CALENDAR_EVENT_TTL_SEC))

                        # indexes + aggs
                        keys_to_refresh: list[tuple[str, str]] = []

                        if ev.currency:
                            key = f"CUR:{ev.currency}"
                            idx = _idx_key_currency(ev.currency)
                            self.r.zadd(idx, {ev.event_id: ev.ts_ms})
                            self.r.expire(idx, int(config.CALENDAR_EVENT_TTL_SEC))
                            _touch_key(self.r, key)
                            keys_to_refresh.append((key, idx))

                        if ev.region:
                            key = f"REG:{ev.region}"
                            idx = _idx_key_region(ev.region)
                            self.r.zadd(idx, {ev.event_id: ev.ts_ms})
                            self.r.expire(idx, int(config.CALENDAR_EVENT_TTL_SEC))
                            _touch_key(self.r, key)
                            keys_to_refresh.append((key, idx))

                        for s in (ev.symbols or []):
                            s2 = str(s)
                            key = f"SYM:{s2}"
                            idx = _idx_key_symbol(s2)
                            self.r.zadd(idx, {ev.event_id: ev.ts_ms})
                            self.r.expire(idx, int(config.CALENDAR_EVENT_TTL_SEC))
                            _touch_key(self.r, key)
                            keys_to_refresh.append((key, idx))

                        for k, idx in keys_to_refresh:
                            _refresh_agg_for_key(self.r, k, idx)

                        xack(self.r, config.CALENDAR_EVENTS_STREAM, config.CALENDAR_FEATURE_GROUP, msg_id)

                    except Exception as e:
                        log.exception("calendar store failed msg_id=%s err=%s", msg_id, e)
                        with contextlib.suppress(Exception):
                            xack(self.r, config.CALENDAR_EVENTS_STREAM, config.CALENDAR_FEATURE_GROUP, msg_id)
