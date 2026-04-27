# trade_patch_shadow_hardening

What this patch fixes/adds

1) Makes `NewsEnricherShadow` and `shadow_cache` **consistent** (previous patch had API mismatches):
- `ShadowCache(per_symbol_cache_ms=..., max_age_ms=...)` exists
- `ShadowRefresher(cfg=ShadowCacheConfig(...))` is the only constructor
- `register_interest()` exists (backward compatible) and maps to `cache.mark_seen()`
- calendar refresh uses HMGET fixed fields (bounded payload)

2) Tick-loop guarantees
- `NewsEnricherShadow.attach()` does **zero Redis I/O**
- staleness drop: if `asof_ts_ms` is older than `max_age_ms`, we fail-open and set `ctx.news=None`
- fallback: if ctx has no `.news` field, sets `ctx.extra["news"]`

3) Tests (pytest, no asyncio)
- refresher populates cache and merges calendar fields
- attach() does not call Redis pipeline
- attach() fallback to ctx.extra works

Integration

Copy these files into your repo (preserve paths):

- python-worker/news_pipeline/shadow_cache.py
- python-worker/news_pipeline/enricher_shadow.py
- python-worker/tests/test_news_shadow_cache.py

Run tests:

```bash
pytest -q python-worker/tests/test_news_shadow_cache.py
```

Data-processor `_filter_dataclass_kwargs` fix (copy/paste)

If you still create ctx via `_filter_dataclass_kwargs(OrderflowSignalContext, ctx_kwargs)`,
ensure unknown keys are not silently lost:

```python
import dataclasses
from typing import Any

def _filter_dataclass_kwargs(cls: type, kwargs: dict[str, Any]) -> dict[str, Any]:
    field_names = {f.name for f in dataclasses.fields(cls)}
    out: dict[str, Any] = {}
    extra: dict[str, Any] = {}
    for k, v in kwargs.items():
        if k in field_names:
            out[k] = v
        else:
            extra[k] = v

    # Preserve dropped keys into ctx.extra if the dataclass supports it
    if "extra" in field_names and extra:
        cur = out.get("extra")
        if isinstance(cur, dict):
            cur.update(extra)
            out["extra"] = cur
        else:
            out["extra"] = extra

    return out
```

This keeps deterministic behavior while preventing silent loss of fields.
