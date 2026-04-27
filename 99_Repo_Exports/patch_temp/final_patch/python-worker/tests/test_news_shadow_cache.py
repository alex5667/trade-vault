import time

import pytest


class FakePipeline:
    def __init__(self, redis):
        self._redis = redis
        self._calls = []

    def hmget(self, key, keys):
        self._calls.append(("hmget", key, tuple(keys)))
        return self

    def hgetall(self, key):
        self._calls.append(("hgetall", key))
        return self

    def execute(self):
        out = []
        for c in self._calls:
            if c[0] == "hmget":
                _, key, fields = c
                h = self._redis.hashes.get(key, {})
                out.append([h.get(f) for f in fields])
            elif c[0] == "hgetall":
                _, key = c
                out.append(dict(self._redis.hashes.get(key, {})))
        self._calls.clear()
        return out


class FakeRedis:
    def __init__(self, hashes=None):
        self.hashes = hashes or {}
        self.pipeline_calls = 0

    def pipeline(self, transaction=False):
        self.pipeline_calls += 1
        return FakePipeline(self)


def test_shadow_refresher_populates_cache_news_and_calendar(monkeypatch):
    # import here so tests can be collected even if package layout differs
    from news_pipeline.shadow_cache import ShadowCache, ShadowRefresher

    # Minimal hashes as stored by your feature_store_worker/calendar_store_worker
    uid = "abc123"
    now_ms = int(time.time() * 1000)

    fake = FakeRedis(
        hashes={
            "news:agg:BTCUSDT": {
                "ref": uid,  # stored as uid in Redis; enricher should prefix
                "risk_ema": "0.8",
                "surprise_ema": "-0.3",
                "news_grade_id": "3",
                "tags_mask": "5",
                "primary_tag_id": "2",
                "horizon_sec": "3600",
                "confidence": "0.9",
                "asof_ts_ms": str(now_ms),
            },
            "calendar:agg:crypto": {
                "event_tminus_sec": "120",
                "event_grade_id": "2",
            },
        }
    )

    cache = ShadowCache(per_symbol_cache_ms=0, max_age_ms=999999)
    ref = ShadowRefresher(
        redis=fake,
        cache=cache,
        refresh_news_ms=999999,
        refresh_calendar_ms=999999,
        active_symbol_ttl_ms=999999,
        active_asset_ttl_ms=999999,
        max_symbols=10,
        max_assets=10,
    )

    ref.register_interest(symbol="BTCUSDT", asset_class="crypto")

    # single refresh
    ref.refresh_news_once()
    ref.refresh_calendar_once()

    nf = cache.get("BTCUSDT", "crypto")
    assert nf is not None

    # ref must be normalized to full Redis key
    assert nf.ref == f"news:analysis:{uid}"

    assert nf.news_risk == pytest.approx(0.8)
    assert nf.surprise_score == pytest.approx(-0.3)
    assert nf.news_grade_id == 3
    assert nf.tags_mask == 5
    assert nf.primary_tag_id == 2
    assert nf.horizon_sec == 3600
    assert nf.confidence == pytest.approx(0.9)

    # calendar fields copied
    assert nf.event_tminus_sec == 120
    assert nf.event_grade_id == 2


def test_tickloop_attach_does_not_call_redis(monkeypatch):
    from news_pipeline.enricher_shadow import NewsEnricherShadow

    # redis is not supposed to be touched by attach(); it is used by refresher only
    fake = FakeRedis(hashes={})

    enricher = NewsEnricherShadow(redis=fake, per_symbol_cache_ms=0)

    # Provide a minimal ctx object shape
    class Ctx:
        def __init__(self):
            self.symbol = "BTCUSDT"
            self.news = None
            self.data_quality_flags = []

    ctx = Ctx()
    enricher.attach(ctx, asset_class="crypto")

    # attach() should not create redis pipeline
    assert fake.pipeline_calls == 0
    assert ctx.news is None
    assert "news_cache_miss" in ctx.data_quality_flags
