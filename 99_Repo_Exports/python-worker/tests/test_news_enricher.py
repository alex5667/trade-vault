import json
from types import SimpleNamespace

import fakeredis

from news_pipeline.enricher_sync import NewsEnricherSync
from news_pipeline.models import NewsFeatures


def test_enricher_attach_ok():
    r = fakeredis.FakeRedis(decode_responses=True)

    r.hset("news:agg:BTCUSDT", mapping={
        "risk": "0.7", "surprise": "-0.2", "tags_mask": "3", "primary_tag": "1", "ref": "abc", "updated_ts": "1700"
    })
    r.set("calendar:next:crypto", json.dumps({"event_ts_ms":2000000000000,"grade_id":2,"ref":"calendar:event:e1"}))

    ctx = SimpleNamespace(symbol="BTCUSDT", ts=1999999999000, price=100.0)
    enr = NewsEnricherSync(redis=r, refresh_ms=0)
    enr.attach(ctx)

    assert isinstance(ctx.news, NewsFeatures)
    assert ctx.news.risk == 0.7
    assert ctx.news.event_tminus_sec >= 0
    assert ctx.news_ref == "abc"

def test_enricher_fail_open():
    r = fakeredis.FakeRedis(decode_responses=True)
    enr = NewsEnricherSync(redis=r, refresh_ms=0)
    ctx = SimpleNamespace(symbol="BTCUSDT", ts=1999999999000, price=100.0)

    # нет ключей — должно быть safe default, без исключений
    enr.attach(ctx)
    assert ctx.news is not None
    assert ctx.news.risk == 0.0
