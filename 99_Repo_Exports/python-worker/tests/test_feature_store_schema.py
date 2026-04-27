from __future__ import annotations

from news_pipeline.feature_store_worker import NewsFeatureStoreWorker
from tests.fake_redis import FakeRedis

def test_feature_store_writes_schema_v2():
    r = FakeRedis()
    # Create worker instance manually without calling __init__ to avoid Redis stream setup
    w = NewsFeatureStoreWorker.__new__(NewsFeatureStoreWorker)
    w.r = r

    w.handle_message("1-0", {
        "uid": "abc",
        "symbol": "BTCUSDT",
        "risk": "0.7",
        "surprise": "-0.2",
        "tags_mask": "3",
        "primary_tag_id": "9",
        "news_grade_id": "2",
        "horizon_sec": "900",
        "confidence": "0.8",
    })

    h = r.hgetall("news:agg:BTCUSDT")
    assert h["ref"] == "news:analysis:abc"
    for k in ["risk_ema","surprise_ema","news_grade_id","tags_mask","primary_tag_id","horizon_sec","confidence","asof_ts_ms"]:
        assert k in h
