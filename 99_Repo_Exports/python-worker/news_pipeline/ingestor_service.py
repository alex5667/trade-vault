from __future__ import annotations

import logging
import json
import time
from typing import Iterable, List, Protocol, Dict, Any, Optional

import redis

from .models import NewsRawItem, CalendarEvent
from .utils import make_news_uid, now_ms
from .redis_streams import xadd_trim
from . import config


log = logging.getLogger("news-ingestor")


class NewsSource(Protocol):
    def fetch(self) -> List[Dict[str, Any]]:
        """
        Вернуть список сырья (dict), где минимум:
        - source, url, title, ts_ms (или published)
        - symbols (optional)
        - payload (optional)
        """
        ...


class CalendarSource(Protocol):
    def fetch(self) -> List[Dict[str, Any]]:
        """
        Вернуть список событий (dict):
        - event_id, title, ts_ms, grade_id, currency, region
        - symbols (optional), payload (optional)
        """
        ...


def _dedup_key(uid: str) -> str:
    return f"news:dedup:{uid}"


def _dedup_pass(r: redis.Redis, uid: str, ttl_sec: int) -> bool:
    # SET key "1" NX EX ttl — быстрый дедуп
    try:
        return bool(r.set(_dedup_key(uid), "1", nx=True, ex=int(ttl_sec)))
    except Exception:
        # fail-open: лучше пропустить, чем стопнуть поток
        return True


def normalize_news_item(raw: Dict[str, Any]) -> Optional[NewsRawItem]:
    try:
        source = str(raw.get("source") or "unknown")
        url = str(raw.get("url") or "")
        title = str(raw.get("title") or "")
        if not title or not url:
            return None

        ts = raw.get("ts_ms")
        if ts is None:
            # допускаем published_sec/published_ms
            if raw.get("published_ms") is not None:
                ts = int(raw.get("published_ms") or 0)
            elif raw.get("published_sec") is not None:
                ts = int(raw.get("published_sec") or 0) * 1000
            else:
                ts = now_ms()
        ts_ms = int(ts)

        symbols = raw.get("symbols") or []
        if isinstance(symbols, str):
            symbols = [s for s in symbols.split(",") if s]
        if not isinstance(symbols, list):
            symbols = []

        payload = raw.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}

        uid = raw.get("uid")
        if not uid:
            uid = make_news_uid(source, url, title, ts_ms, config.NEWS_TS_BUCKET_SEC)

        return NewsRawItem(
            uid=str(uid)
            source=source
            url=url
            title=title
            ts_ms=ts_ms
            symbols=[str(s) for s in symbols][:50]
            payload=payload
        )
    except Exception:
        return None


def normalize_calendar_event(raw: Dict[str, Any]) -> Optional[CalendarEvent]:
    try:
        event_id = str(raw.get("event_id") or raw.get("uid") or "")
        title = str(raw.get("title") or "")
        ts_ms = int(raw.get("ts_ms") or 0)
        grade_id = int(raw.get("grade_id") or 0)
        currency = str(raw.get("currency") or "").upper()
        region = str(raw.get("region") or "").upper()
        if not event_id or not title or ts_ms <= 0:
            return None

        symbols = raw.get("symbols") or []
        if isinstance(symbols, str):
            symbols = [s for s in symbols.split(",") if s]
        if not isinstance(symbols, list):
            symbols = []

        payload = raw.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}

        return CalendarEvent(
            event_id=event_id
            title=title
            ts_ms=ts_ms
            grade_id=grade_id
            currency=currency
            region=region
            symbols=[str(s) for s in symbols][:50]
            payload=payload
        )
    except Exception:
        return None


class NewsIngestorService:
    """
    Периодически:
      - читает источники новостей и календаря
      - нормализует
      - дедупит
      - пишет в Redis Streams: news:raw и calendar:events

    Важно:
      - никаких LLM тут
      - payload в stream держим компактным
    """

    def __init__(
        self
        r: redis.Redis
        news_sources: List[NewsSource]
        calendar_sources: List[CalendarSource]
        poll_sec: float = 10.0
    ) -> None:
        self.r = r
        self.news_sources = news_sources
        self.calendar_sources = calendar_sources
        self.poll_sec = poll_sec
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def run_forever(self) -> None:
        log.info("news-ingestor started poll_sec=%s", self.poll_sec)
        while not self._stop:
            try:
                self._poll_once()
            except Exception as e:
                log.exception("poll error: %s", e)
            time.sleep(self.poll_sec)

    def _poll_once(self) -> None:
        # NEWS
        for src in self.news_sources:
            items = []
            try:
                items = src.fetch()
            except Exception:
                log.exception("news source fetch failed: %s", src)
                continue

            for raw in items:
                item = normalize_news_item(raw)
                if not item:
                    continue
                if not _dedup_pass(self.r, item.uid, config.NEWS_DEDUP_TTL_SEC):
                    continue

                xadd_trim(
                    self.r
                    config.NEWS_RAW_STREAM
                    item.to_stream_fields()
                    maxlen=config.NEWS_MAXLEN
                )

        # CALENDAR
        for src in self.calendar_sources:
            events = []
            try:
                events = src.fetch()
            except Exception:
                log.exception("calendar source fetch failed: %s", src)
                continue
            for raw in events:
                ev = normalize_calendar_event(raw)
                if not ev:
                    continue
                xadd_trim(
                    self.r
                    config.CALENDAR_EVENTS_STREAM
                    ev.to_stream_fields()
                    maxlen=config.NEWS_MAXLEN
                )
