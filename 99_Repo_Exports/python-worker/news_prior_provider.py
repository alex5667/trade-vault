"""NewsPriorProvider — non-blocking ingestion of priors into in-memory cache.

Два режима (выбирайте по инфраструктуре):
1) consumer (recommended): подписка на Redis Stream `stream:signals_news`.
   - 1 asyncio-task читает XREAD (block) и обновляет cache.
   - критический loop НЕ блокируется (get() — sync).

2) key poller: периодический GET `news:prior:<sym>` для активных symbols.
   - полезно как fallback или если вы не хотите stream consumer в ядре.

Этот модуль написан как "интеграционный" и может быть скопирован в ваш python-worker.
В самом news_agent репозитории он не запускается.

Payload формат из news_reasoner:
- stream:signals_news: fields {"payload": <json of NewsPriorDTO>}
- NewsPriorDTO содержит symbols: ["BTCUSDT", ...] и expires_ms
- в Redis keys: news:prior:<SYMBOL> = same JSON

ENV:
- NEWS_PRIOR_PROVIDER_MODE=consumer|poll|both
- NEWS_STREAM_SIGNALS=stream:signals_news
- NEWS_PRIOR_KEY_PREFIX=news:prior:
- NEWS_PRIOR_PROVIDER_BLOCK_MS=250
- NEWS_PRIOR_PROVIDER_POLL_SEC=5
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
from utils.task_manager import safe_create_task

import json
import os
import time
from typing import Any, Dict, Optional, Set

try:
    from redis.asyncio import Redis
except Exception:  # pragma: no cover
    Redis = Any  # type: ignore

from .news_prior_cache import NewsPriorCache


def _now_ms() -> int:
    return get_ny_time_millis()


def parse_prior_payload(payload: Any) -> Optional[Dict[str, Any]]:
    """Parse various payload shapes into a dict prior.

    Accepts:
    - JSON string
    - dict

    Returns None if payload is not parseable.
    """
    if payload is None:
        return None
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = payload.decode("utf-8", "ignore")
        except Exception:
            return None
    if isinstance(payload, str):
        s = payload.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None
    return None


class NewsPriorProvider:
    def __init__(
        self,
        *,
        redis: Redis,
        cache: Optional[NewsPriorCache] = None,
        mode: Optional[str] = None,
        stream: Optional[str] = None,
        key_prefix: Optional[str] = None,
    ) -> None:
        self.redis = redis
        self.cache = cache or NewsPriorCache()
        self.mode = (mode or os.getenv("NEWS_PRIOR_PROVIDER_MODE", "consumer")).lower()  # consumer|poll|both
        self.stream = stream or os.getenv("NEWS_STREAM_SIGNALS", "stream:signals_news")
        self.key_prefix = key_prefix or os.getenv("NEWS_PRIOR_KEY_PREFIX", "news:prior:")
        self.block_ms = int(os.getenv("NEWS_PRIOR_PROVIDER_BLOCK_MS", "250"))
        self.poll_sec = float(os.getenv("NEWS_PRIOR_PROVIDER_POLL_SEC", "5"))
        self._active_syms: Set[str] = set()
        self._stop = asyncio.Event()

        # consumer cursor (XREAD). Start at $ (new messages only).
        self._last_id = "$"

    def note_active_symbol(self, symbol: str) -> None:
        if symbol:
            self._active_syms.add(symbol)

    def get_cached_prior(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self.cache.get(symbol)

    async def start(self) -> None:
        """Start background tasks according to mode."""
        tasks = []
        if self.mode in ("consumer", "both"):
            tasks.append(safe_create_task(self._run_consumer(), name="news-prior-consumer"))
        if self.mode in ("poll", "both"):
            tasks.append(safe_create_task(self._run_poller(), name="news-prior-poller"))

        if not tasks:
            return

        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        finally:
            self._stop.set()
            for t in tasks:
                t.cancel()

    async def stop(self) -> None:
        self._stop.set()

    async def _run_consumer(self) -> None:
        """Continuously read `stream:signals_news` and update cache."""
        while not self._stop.is_set():
            try:
                resp = await self.redis.xread({self.stream: self._last_id}, count=200, block=self.block_ms)
                if not resp:
                    continue

                # resp: [(stream, [(id, {fields})...])]
                for _stream, msgs in resp:
                    for msg_id, fields in msgs:
                        self._last_id = msg_id
                        prior = parse_prior_payload(fields.get("payload") if isinstance(fields, dict) else None)
                        if not prior:
                            continue
                        self._apply_prior(prior)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Fail-open: don't kill the core.
                await asyncio.sleep(0.25)

    async def _run_poller(self) -> None:
        """Periodically fetch `news:prior:<sym>` for active symbols."""
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self.poll_sec)
                syms = list(self._active_syms)
                if not syms:
                    continue

                keys = [f"{self.key_prefix}{sym}" for sym in syms]
                vals = await self.redis.mget(keys)
                now = _now_ms()

                for sym, raw in zip(syms, vals):
                    prior = parse_prior_payload(raw)
                    if not prior:
                        continue
                    # If prior doesn't include symbols, inject current.
                    if "symbols" not in prior:
                        prior = dict(prior)
                        prior["symbols"] = [sym]
                    self.cache.update(sym, prior, now_ms=now)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(0.5)

    def _apply_prior(self, prior: Dict[str, Any]) -> None:
        """Update cache for every symbol in prior.symbols."""
        syms = prior.get("symbols")
        if not isinstance(syms, list) or not syms:
            return
        now = _now_ms()
        for sym in syms:
            if isinstance(sym, str) and sym:
                self.cache.update(sym, prior, now_ms=now)
                self.note_active_symbol(sym)
