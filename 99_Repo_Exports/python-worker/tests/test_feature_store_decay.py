import fakeredis
import time
from unittest.mock import patch
from news_pipeline.feature_store_worker import NewsFeatureStoreWorker

def test_feature_store_decay_no_backward_jump():
    r = fakeredis.FakeRedis(decode_responses=True)
    w = NewsFeatureStoreWorker(redis=r)

    # Первый анализ
    w.handle_message("1-0", {
        "uid": "u1",
        "symbol": "BTCUSDT",
        "risk": "0.8",
        "surprise": "0.2",
        "tags_mask": "1",
        "primary_tag_id": "3",
        "confidence": "0.9",
    })

    h1 = r.hgetall("news:agg:BTCUSDT")
    risk1 = float(h1["risk_ema"])

    # Мокаем время для decay
    with patch('time.time', return_value=time.time() + 3600):  # +1h
        # Второй анализ с меньшим риском
        w.handle_message("2-0", {
            "uid": "u2",
            "symbol": "BTCUSDT",
            "risk": "0.2",
            "surprise": "-0.1",
            "tags_mask": "1",
            "primary_tag_id": "3",
            "confidence": "0.5",
        })

    h2 = r.hgetall("news:agg:BTCUSDT")
    risk2 = float(h2["risk_ema"])

    # EMA не должен "прыгать назад" из-за decay
    assert risk2 < risk1  # decayed down
    assert risk2 > 0.1   # но не слишком низко
