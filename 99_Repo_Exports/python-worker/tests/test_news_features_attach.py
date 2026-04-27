from utils.time_utils import get_ny_time_millis
import time
import pytest
import redis

from contexts import OrderflowSignalContext, NewsFeatures
from news_pipeline.enricher_sync import NewsEnricherSync

@pytest.fixture
def redis_conn():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    # Clean up any test keys
    r.delete("news:agg:BTCUSDT", "calendar:agg:crypto", "news:agg:ETHUSDT")
    yield r
    # Cleanup
    r.delete("news:agg:BTCUSDT", "calendar:agg:crypto", "news:agg:ETHUSDT")

def test_attach_sets_ctx_news_fail_open(redis_conn):
    en = NewsEnricherSync(redis=redis_conn, per_symbol_cache_ms=0)

    # seed redis
    redis_conn.hset("news:agg:BTCUSDT", mapping={
        "ref": "news:analysis:abc",
        "risk_ema": "0.42",
        "surprise_ema": "-0.2",
        "news_grade_id": "3",
        "tags_mask": "8",
        "primary_tag_id": "5",
        "confidence": "0.9",
        "horizon_sec": "43200",
        "asof_ts_ms": str(get_ny_time_millis()),
    })
    redis_conn.hset("calendar:agg:crypto", mapping={
        "event_tminus_sec": "900",
        "event_grade_id": "2",
        "asof_ts_ms": str(get_ny_time_millis()),
    })

    ctx = OrderflowSignalContext(symbol="BTCUSDT", ts=1, price=1.0)
    en.attach(ctx, asset_class="crypto")

    assert ctx.news is not None
    assert isinstance(ctx.news, NewsFeatures)
    assert ctx.news.ref == "news:analysis:abc"
    assert abs(ctx.news.news_risk - 0.42) < 1e-9
    assert ctx.news.event_tminus_sec == 900

def test_attach_missing_keys_does_not_raise(redis_conn):
    en = NewsEnricherSync(redis=redis_conn, per_symbol_cache_ms=0)
    ctx = OrderflowSignalContext(symbol="ETHUSDT", ts=1, price=1.0)
    en.attach(ctx, asset_class="crypto")
    # fail-open: либо None, либо пустые дефолты
    assert ctx.news is None
