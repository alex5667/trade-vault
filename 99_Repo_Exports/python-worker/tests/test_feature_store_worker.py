from __future__ import annotations

from news_pipeline.feature_store_worker import NewsFeatureStoreWorker, decay_factor
from tests.fake_redis import FakeRedis  # type: ignore


def test_decay_factor_basic():
    # dt=0 => d ~ 1
    assert abs(decay_factor(0, 1800) - 1.0) < 1e-9
    # positive dt gives 0<d<1
    d = decay_factor(900, 1800)
    assert 0.0 < d < 1.0


def test_feature_store_writes_ref_and_ema():
    r = FakeRedis()
    w = NewsFeatureStoreWorker(redis=r, pg=None)

    msg = {
        "uid": "abc",
        "symbol": "BTCUSDT",
        "risk": "0.7",
        "surprise": "-0.2",
        "tags_mask": "3",
        "primary_tag_id": "12",
        "confidence": "0.8",
        "ts_ms": "1700000000000",
        "source": "test",
    }
    w.handle_message("1-0", msg)

    agg = r.hashes["news:agg:BTCUSDT"]
    assert agg["ref"] == "news:analysis:abc"
    assert float(agg["risk_ema"]) >= 0.7
    assert int(agg["primary_tag_id"]) == 12
    assert "asof_ts_ms" in agg
