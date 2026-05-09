
import redis

from news_pipeline.enricher_sync import NewsEnricherSync
from utils.time_utils import get_ny_time_millis


def test_enricher_prefixes_old_uid_ref():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # seed redis
    r.hset("news:agg:BTCUSDT", mapping={
        "ref": "abc123",
        "risk_ema": "0.5",
        "surprise_ema": "0.2",
        "news_grade_id": "2",
        "tags_mask": "0",
        "primary_tag_id": "0",
        "confidence": "0.5",
        "horizon_sec": "3600",
        "asof_ts_ms": str(get_ny_time_millis()),
    })

    # Clean up after test
    try:
        enricher = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)

        ctx = type('MockCtx', (), {'symbol': 'BTCUSDT', 'news': None})()
        enricher.attach(ctx)

        assert ctx.news is not None
        assert ctx.news.ref == "news:analysis:abc123"
    finally:
        r.delete("news:agg:BTCUSDT")
