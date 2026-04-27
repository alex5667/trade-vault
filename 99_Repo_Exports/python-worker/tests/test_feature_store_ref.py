from unittest.mock import MagicMock
from news_pipeline.feature_store_worker import NewsFeatureStoreWorker

def test_feature_store_ref_is_key():
    r = MagicMock()
    r.hgetall.return_value = {}
    pipe = MagicMock()
    r.pipeline.return_value = pipe
    pipe.execute.return_value = []

    w = NewsFeatureStoreWorker(redis=r)
    w.pg = None  # отключим pg

    fields = {
        "uid": "u1",
        "symbol": "BTCUSDT",
        "risk": "0.9",
        "surprise": "0.1",
        "confidence": "0.8",
        "tags_mask": "1",
        "primary_tag_id": "3",
    }
    w.handle_message("1-0", fields)

    # hset mapping должен содержать ref с префиксом news:analysis:
    args, kwargs = pipe.hset.call_args
    mapping = kwargs["mapping"]
    assert mapping["ref"].startswith("news:analysis:")