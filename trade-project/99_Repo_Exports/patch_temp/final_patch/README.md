# trade_patch_omega

## What this patch provides

1. **Zero-IO tick-loop enrichment** for news + calendar features.
   - Tick-loop reads only an in-process dict.
   - No Redis calls from tick-loop.

2. **Dedicated fast Redis client** for news/calendar background refresh.
   - Socket timeout default: **50ms**
   - Connect timeout default: **200ms**
   - Fail-open refresh: errors never stop tick-loop.

3. **Background refresher thread**
   - Refreshes compact hashes:
     - `news:agg:<SYMBOL>`
     - `calendar:agg:<asset_class>`

4. **Tests (pytest)** for the shadow cache and the "no Redis in attach" guarantee.

## Files included

- `python-worker/news_pipeline/shadow_cache.py`
  - `ShadowCache` (in-memory store)
  - `ShadowRefresher` (background refresh loop)

- `python-worker/news_pipeline/enricher_shadow.py`
  - `NewsEnricherShadow` (tick-loop interface: attach(ctx))

- `python-worker/core/redis_client.py`
  - Drop-in replacement for your existing file.
  - Keeps `get_redis()` unchanged.
  - Adds `get_redis_fast_news()` using an independent pool.

- `python-worker/tests/test_news_shadow_cache.py`

- `docs/news_enrichment_tickloop.md`

## Integration steps

1. Copy files into your repo (preserve paths).

2. Create the fast Redis client once during initialization:

```python
from core.redis_client import get_redis_fast_news
redis_fast = get_redis_fast_news()
```

3. Build the enricher once and reuse it:

```python
from news_pipeline.enricher_shadow import NewsEnricherShadow

news_enricher = NewsEnricherShadow(redis=redis_fast)
```

4. In the tick-loop, call:

```python
news_enricher.attach(ctx, asset_class=ctx.asset_class)
```

5. Start the background refresher thread (usually at init time):

```python
news_enricher.start()
```

6. Optional: stop on shutdown:

```python
news_enricher.stop(join=True)
```

## Environment variables

### Fast Redis client (background refresh)

- `NEWS_REDIS_SOCKET_TIMEOUT_MS` (default `50`)
- `NEWS_REDIS_CONNECT_TIMEOUT_MS` (default `200`)
- `NEWS_REDIS_MAX_CONNECTIONS` (default `50`)

### Shadow cache behavior

- `NEWS_SHADOW_REFRESH_MS` (default `250`)
- `CAL_SHADOW_REFRESH_MS` (default `1000`)
- `NEWS_SHADOW_SYMBOL_TTL_MS` (default `30000`)
- `NEWS_SHADOW_MAX_SYMBOLS` (default `256`)
- `NEWS_ENRICHER_MAX_AGE_MS` (default `300000`)

## Running tests

```bash
pytest -q python-worker/tests/test_news_shadow_cache.py
```
