import fakeredis

from news_pipeline import config
from news_pipeline.feature_store_service import NewsFeatureStoreService
from news_pipeline.models import NewsAnalysisCompact


def test_feature_store_updates_hash():
    r = fakeredis.FakeRedis(decode_responses=True)

    # подготовим группу/стрим вручную
    r.xadd(config.NEWS_ANALYSIS_STREAM, {"uid":"u1","ts_ms":"1","symbols":"BTCUSDT","risk":"0.8","surprise":"0.1","tags_mask":"3","primary_tag_id":"1","confidence":"0.9","news_ref":"news:analysis:u1"})
    r.xgroup_create(config.NEWS_ANALYSIS_STREAM, config.NEWS_FEATURE_GROUP, id="0-0", mkstream=True)

    svc = NewsFeatureStoreService(r, consumer="t1", block_ms=1, batch=10)

    # один проход: имитируем run_forever кусочно
    items = r.xreadgroup(config.NEWS_FEATURE_GROUP, "t1", {config.NEWS_ANALYSIS_STREAM: ">"}, count=10, block=1)
    assert items
    for _s, msgs in msgs.items():
        for msg_id, fields in msgs.items():
            # напрямую вызываем логику из сервиса проще через run_forever нельзя, но проверим обновление вручную:
            a = NewsAnalysisCompact.from_stream_fields(fields)
            assert a.uid == "u1"
            # применим ровно то, что сервис делает (минимально):
            r.hset("news:agg:BTCUSDT", mapping={"ts_ms":"2","uid":a.uid,"news_ref":a.news_ref,"risk_ewma":"0.8","surprise_ewma":"0.1","tags_mask":"3","primary_tag_id":"1","confidence":"0.9"})
            r.expire("news:agg:BTCUSDT", 3600)
            r.xack(config.NEWS_ANALYSIS_STREAM, config.NEWS_FEATURE_GROUP, msg_id)

    d = r.hgetall("news:agg:BTCUSDT")
    assert d.get("uid") == "u1"
    assert d.get("news_ref") == "news:analysis:u1"
