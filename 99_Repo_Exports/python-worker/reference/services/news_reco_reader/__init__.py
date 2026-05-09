"""services.news_reco_reader

Trade-core drop-in module for reading news recommendations from Redis into
an in-memory cache.

Usage
-----
Startup (once, inside an asyncio loop):
    await ensure_started()

Hot-path (no IO):
    snap = get_reco("BTCUSDT")
"""

from .reader import NewsRecoReader, ensure_started, get_reco, shutdown  # noqa: F401
