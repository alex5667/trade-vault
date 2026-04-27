# python-worker/tests/test_enricher_ref.py
from news_pipeline.enricher_sync import NewsEnricherSync
from contexts import OrderflowSignalContext

class FakePipe:
    def __init__(self, res):
        self.res = res
    def hgetall(self, _): pass
    def execute(self): return self.res

class FakeRedis:
    def __init__(self, news_hash, cal_hash=None):
        self.news_hash = news_hash
        self.cal_hash = cal_hash or {}
    def pipeline(self, transaction=False):
        # emulate two calls: news + calendar
        if self.cal_hash:
            return FakePipe([self.news_hash, self.cal_hash])
        return FakePipe([self.news_hash])

def test_ref_is_prefixed_if_needed():
    r = FakeRedis({"ref":"abc123", "risk_ema":"0.5", "surprise_ema":"0.1", "asof_ts_ms":"1"})
    e = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)
    ctx = OrderflowSignalContext(symbol="BTCUSDT")
    e.attach(ctx, asset_class="crypto")
    assert ctx.news is not None
    assert ctx.news.ref.startswith("news:analysis:")
