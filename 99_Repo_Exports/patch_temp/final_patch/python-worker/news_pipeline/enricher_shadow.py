"""news_pipeline.enricher_shadow

Tick-loop enricher that uses the in-process shadow cache.

Compared to news_pipeline.enricher_sync.NewsEnricherSync:
- attach() does *no Redis I/O* (bounded to local dict lookup)
- the Redis work happens in ShadowRefresher thread

You still get:
- fail-open behavior
- optional data_quality_flags (dq_flags)

Integration
-----------

At initialization (one time):

    from news_pipeline.enricher_shadow import NewsEnricherShadow

    # Use a *fast* Redis client for the refresher thread.
    # IMPORTANT: tick-loop will not use Redis.
    enricher = NewsEnricherShadow(
        redis=handler.redis_news_fast,  # or any redis.Redis
        max_tick_block_ms=50,
        connect_timeout_ms=200,
    )
    enricher.start()

In tick-loop:

    enricher.attach(ctx, asset_class=ctx.asset_class or "")

On shutdown (optional):

    enricher.stop()

"""

from __future__ import annotations

import logging
from typing import Optional

from contexts import OrderflowSignalContext

from news_pipeline.shadow_cache import ShadowCache, ShadowRefresher

log = logging.getLogger("news_enricher_shadow")


class NewsEnricherShadow:
    """Facade: manages shadow cache + background refresher."""

    def __init__(
        self,
        *,
        redis,
        per_symbol_cache_ms: int = 1500,
        refresh_news_ms: int = 250,
        refresh_calendar_ms: int = 1000,
        active_symbol_ttl_ms: int = 30_000,
        active_asset_ttl_ms: int = 60_000,
        max_symbols: int = 256,
        max_assets: int = 8,
        max_tick_block_ms: int = 50,
        max_age_ms: int = 300_000,
        connect_timeout_ms: int = 200,
    ) -> None:
        # NOTE: max_tick_block_ms / connect_timeout_ms kept for documentation and
        # future optional guards. In current design tick-loop never blocks on IO.
        _ = (max_tick_block_ms, connect_timeout_ms)

        self.cache = ShadowCache(per_symbol_cache_ms=int(per_symbol_cache_ms), max_age_ms=int(max_age_ms))
        self.refresher = ShadowRefresher(
            redis=redis,
            cache=self.cache,
            refresh_news_ms=int(refresh_news_ms),
            refresh_calendar_ms=int(refresh_calendar_ms),
            active_symbol_ttl_ms=int(active_symbol_ttl_ms),
            active_asset_ttl_ms=int(active_asset_ttl_ms),
            max_symbols=int(max_symbols),
            max_assets=int(max_assets),
        )

    def start(self) -> None:
        """Start background refresher thread (idempotent)."""
        self.refresher.start()

    def stop(self) -> None:
        """Stop background refresher thread (best-effort)."""
        self.refresher.stop()

    def attach(self, ctx: OrderflowSignalContext, *, asset_class: str = "") -> None:
        """Attach cached NewsFeatures to ctx. Tick-loop safe (no IO)."""

        sym = (ctx.symbol or "GLOBAL").upper()
        ac = (asset_class or "").strip().lower()

        # 1) register interest (tick-loop does only dict writes)
        self.refresher.register_interest(symbol=sym, asset_class=ac)

        # 2) read from local cache
        nf = self.cache.get(symbol=sym, asset_class=ac)
        if nf is None:
            # fail-open: don't break signal pipeline
            try:
                ctx.news = None
            except Exception:
                pass
            _dq(ctx, "news_cache_miss")
            return

        try:
            ctx.news = nf
        except Exception:
            pass

        # optional staleness markers
        if self.cache.is_stale(nf):
            _dq(ctx, "news_cache_stale")


def _dq(ctx: OrderflowSignalContext, flag: str) -> None:
    """Append a dq flag without hard dependency on your dq_flags helper."""
    try:
        # prefer your centralized helpers if present
        from common.dq_flags import append_dq_flag  # type: ignore

        append_dq_flag(ctx, flag)
        return
    except Exception:
        pass

    try:
        lst = getattr(ctx, "data_quality_flags", None)
        if lst is None:
            return
        if flag not in lst:
            lst.append(flag)
    except Exception:
        return
