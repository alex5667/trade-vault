"""news_pipeline.llm_cache

Redis cache for LLM results keyed by job_id.
Prevents duplicate Playwright calls for the same news item + prompt version.

Key: news:llm:cache:{job_id}
TTL: NEWS_LLM_CACHE_TTL_SEC (default 86400 = 24h)
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

from .llm_job import LLMResult, LLMStatus

log = logging.getLogger(__name__)

_CACHE_TTL_SEC = int(os.getenv("NEWS_LLM_CACHE_TTL_SEC", "86400"))
_PREFIX        = "news:llm:cache:"


def cache_key(job_id: str) -> str:
    return f"{_PREFIX}{job_id}"


def get(job_id: str, redis: Any) -> LLMResult | None:
    """Return cached LLMResult or None."""
    try:
        raw = redis.get(cache_key(job_id))
        if not raw:
            return None
        d = json.loads(raw)
        result = LLMResult.from_dict(d)
        result.status = LLMStatus.CACHE_HIT
        return result
    except Exception as exc:
        log.debug("llm_cache.get failed job_id=%s: %r", job_id, exc)
        return None


def put(job_id: str, result: LLMResult, redis: Any) -> None:
    """Cache result. Skips error statuses so bad results are not cached."""
    if result.status not in {LLMStatus.OK}:
        return
    try:
        redis.setex(cache_key(job_id), _CACHE_TTL_SEC, json.dumps(result.to_dict()))
    except Exception as exc:
        log.debug("llm_cache.put failed job_id=%s: %r", job_id, exc)


def invalidate(job_id: str, redis: Any) -> None:
    try:
        redis.delete(cache_key(job_id))
    except Exception:
        pass
