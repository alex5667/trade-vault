from __future__ import annotations

import os
import threading

# NOTE:
# This module provides a *single* NewsEnricherShadow instance for the whole process.
# Goal: avoid duplicate background threads / duplicate Redis polling.
#
# Tick-loop safety:
# - attach() must be *zero IO* (reads only in-memory shadow cache)
# - all Redis IO happens in the background refresher thread
#
# Enable/disable:
# - NEWS_ENRICHER_ENABLE=1 (default) enables
# - NEWS_ENRICHER_ENABLE=0 disables entirely (returns None)

_lock = threading.Lock()
_enricher = None  # type: ignore


def get_news_enricher():
    """Return a started NewsEnricherShadow singleton, or None if disabled/unavailable."""
    global _enricher

    if os.getenv("NEWS_ENRICHER_ENABLE", "1").strip() in ("0", "false", "False", "no", "NO"):
        return None

    if _enricher is not None:
        return _enricher

    with _lock:
        if _enricher is not None:
            return _enricher

        try:
            from core.redis_client import get_redis_fast_news
            from news_pipeline.enricher_shadow import NewsEnricherShadow

            redis_fast = get_redis_fast_news()
            e = NewsEnricherShadow(redis=redis_fast)
            e.start()
            _enricher = e
            return _enricher
        except Exception:
            # fail-open: do not break the trading pipeline if news infra is down
            _enricher = None
            return None
