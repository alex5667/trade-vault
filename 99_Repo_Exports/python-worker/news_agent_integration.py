"""news_agent → scanner_infra integration module.

This module wires all three components together:
  - NewsPriorProvider  (background Redis consumer/poller)
  - NewsPriorCache     (O(1) in-memory lookup)
  - NewsPriorGate      (Stage-5 gate: flags / risk overrides)

Usage in your python-worker asyncio entry point:
--------------------------------------------------
    from news_agent_integration import NewsAgentIntegration

    # At startup (once, per process):
    news_integration = NewsAgentIntegration()
    await news_integration.start(redis_client)   # launches background task

    # In pre_publish_gates (critical path — sync, no IO):
    news_integration.inject(ctx, symbol)         # sets ctx.news_prior
    gate = news_integration.gate
    gate.validate(ctx, cand, q)                  # may set flags/overrides

    # At shutdown:
    await news_integration.stop()

Environment variables (all optional with safe defaults):
    NEWS_PRIOR_GATE_PROFILE      soft | tighten | hard    (default: soft)
    NEWS_PRIOR_PROVIDER_MODE     consumer | poll | both   (default: consumer)
    NEWS_STREAM_SIGNALS          stream:signals_news
    NEWS_PRIOR_KEY_PREFIX        news:prior:
    NEWS_PRIOR_CACHE_TTL_MS      900000
    NEWS_PRIOR_CACHE_MAX_SYMBOLS 2048

See redis-worker-1-acl.conf: go_gateway/go_worker already have
~* key + @stream + `mget` access — no ACL changes needed.
"""

from __future__ import annotations

import asyncio
from utils.task_manager import safe_create_task

import os
from typing import Any, Optional

from news_prior_cache import NewsPriorCache
from news_prior_gate import NewsPriorGate
from news_prior_provider import NewsPriorProvider


class NewsAgentIntegration:
    """One-stop integration object for news_agent priors in scanner_infra.

    Thread-safety:
    - `inject()` and `gate.validate()` are safe to call from any coroutine.
    - Do NOT call inject() or validate() from a thread pool without external lock.
    """

    def __init__(self) -> None:
        self.cache = NewsPriorCache(
            default_ttl_ms=int(os.getenv("NEWS_PRIOR_CACHE_TTL_MS", "900000")),
            max_symbols=int(os.getenv("NEWS_PRIOR_CACHE_MAX_SYMBOLS", "2048")),
        )
        self.gate = NewsPriorGate()
        self._provider: Optional[NewsPriorProvider] = None
        self._task: Optional[asyncio.Task] = None

    async def start(self, redis: Any) -> None:
        """Start the background prior consumer/poller.

        Call once at worker startup:
            safe_create_task(integration.start(r))
        or simply:
            await integration.start(r)   # blocks forever — use create_task

        redis: redis.asyncio.Redis instance (decode_responses=True preferred)
        """
        self._provider = NewsPriorProvider(
            redis=redis,
            cache=self.cache,
            mode=os.getenv("NEWS_PRIOR_PROVIDER_MODE", "consumer"),
            stream=os.getenv("NEWS_STREAM_SIGNALS", "stream:signals_news"),
            key_prefix=os.getenv("NEWS_PRIOR_KEY_PREFIX", "news:prior:"),
        )
        await self._provider.start()

    async def stop(self) -> None:
        if self._provider:
            await self._provider.stop()

    def inject(self, ctx: Any, symbol: str) -> None:
        """Inject ctx.news_prior = cache.get(symbol).

        Must be called BEFORE gate.validate().
        This is O(1) sync — safe on the critical path.

        Example:
            news_integration.inject(ctx, symbol)
            news_integration.gate.validate(ctx, cand, q)
        """
        prior = self.cache.get(symbol)
        ctx.news_prior = prior
        if self._provider and symbol:
            self._provider.note_active_symbol(symbol)

    def inject_and_validate(self, ctx: Any, cand: Any, q: Any, symbol: str) -> None:
        """Convenience: inject ctx.news_prior then run gate.validate().

        For Stage-5 gates use this single call instead of two separate calls.
        """
        self.inject(ctx, symbol)
        self.gate.validate(ctx, cand, q)


# ---------------------------------------------------------------------------
# Singleton pattern — use this in production workers for zero-overhead sharing
# ---------------------------------------------------------------------------

_singleton: Optional[NewsAgentIntegration] = None


def get_integration() -> NewsAgentIntegration:
    """Get or create the global NewsAgentIntegration instance.

    Safe to call from any module — returns the same object every time.
    """
    global _singleton
    if _singleton is None:
        _singleton = NewsAgentIntegration()
    return _singleton
