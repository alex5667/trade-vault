from types import SimpleNamespace

from news_pipeline.enricher_sync import NewsEnricherSync


class _FakePipe:
    def __init__(self, result, raises=None):
        self._result = result
        self._raises = raises

    def hmget(self, key, *fields):
        return self

    def execute(self):
        if self._raises:
            raise self._raises
        return self._result


class _FakeRedis:
    def __init__(self, pipe):
        self._pipe = pipe
        self.pipelines = 0

    def pipeline(self, transaction=False):
        self.pipelines += 1
        return self._pipe


def test_attach_sets_news_and_ref_key():
    # hmget returns list values in field order
    news_vals = ["uid123", "0.8", "0.2", "3", "5", "2", "0.9", "3600", "1000"]
    cal_vals = ["120", "2"]

    r = _FakeRedis(_FakePipe([news_vals, cal_vals]))
    e = NewsEnricherSync(redis=r, per_symbol_cache_ms=10_000)

    ctx = SimpleNamespace(symbol="BTCUSDT", news=None)
    e.attach(ctx, asset_class="crypto")

    assert ctx.news is not None
    assert ctx.news.ref == "news:analysis:uid123"
    assert ctx.news.risk_ema == 0.8
    assert ctx.news.cal_tminus_sec == 120

    # cache hit: no new pipeline
    before = r.pipelines
    e.attach(ctx, asset_class="crypto")
    assert r.pipelines == before


def test_fail_open_on_timeout():
    r = _FakeRedis(_FakePipe([], raises=TimeoutError("redis timeout")))
    e = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)

    ctx = SimpleNamespace(symbol="BTCUSDT", news="OLD")
    e.attach(ctx, asset_class="crypto")

    assert ctx.news is None
