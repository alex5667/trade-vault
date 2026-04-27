# News logging (tick-loop safe)

## Requirements
1. Do **not** log the entire `OrderflowSignalContext` (`ctx`) — it can be huge and may contain PII / internal fields.
2. Tick-loop must keep a hard upper bound on blocking; we treat **50ms** as the maximum acceptable budget for the *whole* emit path.
3. Full debug logging must not introduce Redis IO into the tick-loop.

## Mini-log (always on)
Log only:
- `symbol`, `ts_ms`
- `news_risk`, `news_tminus_sec`, `news_grade_id`, `news_tags_mask`
- plus compact useful fields: `news_primary_tag_id`, `news_confidence`, `news_horizon_sec`, `news_asof_ts_ms`, `news_ref`

Use:
```python
from news_pipeline.news_logging import add_news_minilog

ev = {"kind": "signal", "symbol": ctx.symbol, "ts_ms": ctx.ts}
add_news_minilog(ev, ctx)
logger.info(json.dumps(ev, ensure_ascii=False, separators=(",", ":")))
```

## Full debug (separate log line, sampled)
Enable:
- `NEWS_DEBUG_FULL=true`
- `NEWS_DEBUG_SAMPLE_PCT=1` (default, 1%)
- `NEWS_DEBUG_MAX_BYTES=65536` (cap heavy JSON)

Start once at process init (not per tick):
```python
from core.redis_client import get_redis_fast_news
from news_pipeline.news_logging import NewsFullDebugFetcher

redis_fast = get_redis_fast_news()
news_full = NewsFullDebugFetcher(redis=redis_fast)
news_full.start()
```

Enqueue only when a signal is emitted (not on every tick):
```python
news_full.enqueue(ref=ctx.news.ref, symbol=ctx.symbol, ts_ms=ctx.ts)
```

The background thread will:
- `GET news:analysis:<uid>` with tight timeouts (fast client)
- write one JSON line to logger `news_full_debug`

This keeps tick-loop free from additional IO.

## Extra hardening: safe ctx repr
If you implement a safe `__repr__` for `OrderflowSignalContext`, accidental `logger.info(ctx)` will still be bounded.
