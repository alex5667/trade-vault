# Tick-loop safe news enrichment (zero-IO path)

## Goal

Guarantee a hard upper bound on tick-loop blocking due to news/calendar feature reads.

Budgets:
- Tick-loop maximum allowed blocking from news: **50ms** (target)
- Redis connect timeout (background refresher): **200ms**

With the shadow-cache design, the tick-loop does **no Redis I/O**, so the practical
blocking from network is **0ms**. Only a local dict lookup remains.

## What changes

1) Keep the existing long-timeout `get_redis()` for heavy/background tasks.
2) Add a separate fast Redis client `get_redis_fast_news()` (own pool, tight timeouts).
3) Introduce a shadow cache refreshed by a background thread.
4) In the tick-loop, attach cached features without touching Redis.

## Environment variables

Fast Redis client:
- `NEWS_REDIS_SOCKET_TIMEOUT_MS` (default: `50`) -> socket timeout for background reads
- `NEWS_REDIS_CONNECT_TIMEOUT_MS` (default: `200`) -> connect timeout
- `NEWS_REDIS_MAX_CONNECTIONS` (default: `50`) -> pool cap

Shadow refresher:
- `NEWS_SHADOW_REFRESH_NEWS_MS` (default: `250`) -> refresh period for `news:agg:*`
- `NEWS_SHADOW_REFRESH_CAL_MS` (default: `1000`) -> refresh period for `calendar:agg:*`
- `NEWS_SHADOW_SYMBOL_TTL_MS` (default: `30000`) -> keep a symbol active for refresh this long after last tick
- `NEWS_SHADOW_MAX_SYMBOLS` (default: `512`) -> cap per refresh (avoid huge pipelines)

Tick-loop staleness policy:
- `NEWS_TICK_MAX_AGE_MS` (default: `300000`) -> if cached features are older than this, drop them (fail-open)

## Wiring (minimal)

Wherever you create the news enricher that is called inside the tick-loop, replace it.

Before:
```python
from news_pipeline.enricher_sync import NewsEnricherSync
self.news_enricher = NewsEnricherSync(redis=self.handler.redis, per_symbol_cache_ms=1500)
```

After:
```python
from core.redis_client import get_redis_fast_news
from news_pipeline.enricher_shadow import NewsEnricherShadow

redis_fast = get_redis_fast_news()  # tight timeouts
self.news_enricher = NewsEnricherShadow(redis=redis_fast)
```

Tick-loop call stays the same:
```python
self.news_enricher.attach(ctx, asset_class=ctx.asset_class)
```

## Data-quality flags

If `ctx.data_quality_flags` is present (it is in your legacy OrderflowContext), the enricher
can add:
- `news_cache_miss`    -> no cached features yet
- `news_cache_stale`   -> cached features too old

This preserves your fail-open telemetry style.

## Testing

Added pytest suite:
- `python-worker/tests/test_news_shadow_cache.py`

Run:
```bash
pytest -q
```

The tests validate:
- refresher reads from fake Redis and populates cache
- `ref` normalization to `news:analysis:<uid>`
- tick-loop attach does not call Redis
