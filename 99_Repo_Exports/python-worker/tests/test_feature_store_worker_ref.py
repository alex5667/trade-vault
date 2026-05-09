import fakeredis

from news_pipeline.feature_store_worker import NewsFeatureStoreWorker


def test_feature_store_sets_ref_pointer():
    r = fakeredis.FakeRedis(decode_responses=True)
    w = NewsFeatureStoreWorker(redis=r)

    w.handle_message("1-0", {
        "uid": "u1",
        "symbol": "BTCUSDT",
        "risk": "0.9",
        "surprise": "-0.2",
        "tags_mask": "1",
        "primary_tag_id": "3",
        "confidence": "0.8",
    })

    h = r.hgetall("news:agg:BTCUSDT")
    assert h["ref"].startswith("news:analysis:")
    assert h["ref"] == "news:analysis:u1"
