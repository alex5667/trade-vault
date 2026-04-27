from __future__ import annotations

from types import SimpleNamespace

from news_pipeline.enricher_sync import NewsEnricherSync
from tests.fake_redis import FakeRedis  # type: ignore


def test_enricher_attaches_news_features():
    r = FakeRedis()
    r.hashes["news:agg:BTCUSDT"] = {
        "ref": "news:analysis:abc",
        "risk_ema": "0.5",
        "surprise_ema": "0.1",
        "news_grade_id": "2",
        "tags_mask": "7",
        "primary_tag_id": "3",
        "confidence": "0.9",
        "horizon_sec": "1800",
        "asof_ts_ms": "1700000000000",
    }
    r.hashes["calendar:agg:crypto"] = {"event_tminus_sec": "600", "event_grade_id": "4"}

    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)

    ctx = SimpleNamespace(symbol="BTCUSDT", news=None, data_quality_flags=[])
    enr.attach(ctx, asset_class="crypto")  # type: ignore[arg-type]

    assert ctx.news is not None
    assert ctx.news.ref == "news:analysis:abc"
    assert ctx.news.event_tminus_sec == 600
    assert ctx.news.event_grade_id == 4
