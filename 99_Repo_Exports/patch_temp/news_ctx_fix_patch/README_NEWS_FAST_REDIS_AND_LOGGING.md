This patch adds:

1) `get_redis_fast_news()` with tight timeouts (defaults: 50ms read/write, 200ms connect)
   for usage near the tick-loop.

2) `news_pipeline/news_debug.py` helpers:
   - mini logging: `extract_news_mini(ctx)`
   - deterministic sampling: `stable_sample_pct(symbol, ts_ms, pct)`
   - optional full debug JSON fetch by `ctx.news.ref` (controlled by env)

3) pytest unit tests (pure, no Redis required).

---

## How to wire it into tick-loop

### A) Ensure ctx has `symbol` and `ts`
`stable_sample_pct` requires `ctx.symbol` and `ctx.ts`.
If your `OrderflowSignalContext` currently doesn't store `ts`, add it.

### B) Use fast client
In tick-loop initialization:

```python
from core.redis_client import get_redis_fast_news
redis_fast = get_redis_fast_news()
```

### C) Mini log (sampled)

```python
from news_pipeline.news_debug import extract_news_mini, stable_sample_pct

if ctx.news and stable_sample_pct(ctx.symbol, ctx.ts, int(os.getenv("NEWS_LOG_MINI_SAMPLE_PCT", "1"))):
    logger.debug("news_mini %s", extract_news_mini(ctx))
```

### D) Full debug (behind flag)

```python
from news_pipeline.news_debug import maybe_log_news_full

maybe_log_news_full(redis_fast, logger, ctx)
```

---

## Env knobs

- `NEWS_REDIS_HOST` / `NEWS_REDIS_PORT` / `NEWS_REDIS_DB`
- `NEWS_REDIS_SOCKET_TIMEOUT_SEC` (default 0.05)
- `NEWS_REDIS_CONNECT_TIMEOUT_SEC` (default 0.2)
- `NEWS_DEBUG_FULL` ("true"/"false")
- `NEWS_DEBUG_SAMPLE_PCT` (default 1)
- `NEWS_DEBUG_MAX_BYTES` (default 2000)

---

## Run tests

```bash
cd python-worker
pytest -q
```
