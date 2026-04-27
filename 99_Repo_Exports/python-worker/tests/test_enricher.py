from __future__ import annotations

from news_pipeline.enricher_sync import NewsEnricherSync
from tests.fake_redis import FakeRedis

class Ctx:
    __slots__ = ("symbol","news")
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.news = None

def test_enricher_attach_reads_global_fallback():
    r = FakeRedis()
    r.hset("news:agg:GLOBAL", {"ref":"news:analysis:zzz","risk_ema":"0.9","asof_ts_ms":"1"})
    e = NewsEnricherSync(redis=r)  # type: ignore

    ctx = Ctx("ETHUSDT")
    e.attach(ctx)
    assert ctx.news is not None
    assert ctx.news.ref == "news:analysis:zzz"
