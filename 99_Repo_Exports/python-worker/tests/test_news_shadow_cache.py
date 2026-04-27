from utils.time_utils import get_ny_time_millis
import time

import pytest


class FakePipeline:
    def __init__(self, redis):
        self._redis = redis
        self._calls = []

    def hmget(self, key, *fields):
        self._calls.append(("hmget", key, tuple(fields)))
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


def test_shadow_refresher_populates_cache_news_and_calendar():
    from contexts import NewsFeatures
    from news_pipeline.shadow_cache import ShadowCache, ShadowCacheConfig, ShadowRefresher

    uid = "abc123"
    now_ms = get_ny_time_millis()

    fake = FakeRedis(
        hashes={
            "news:agg:BTCUSDT": {
                "ref": uid,  # stored as uid; refresher should prefix
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
                "updated_ts_ms": str(now_ms),
            },
        }
    )

    cache = ShadowCache(per_symbol_cache_ms=0, max_age_ms=999999)
    cfg = ShadowCacheConfig(
        refresh_ms=999999,
        calendar_refresh_ms=999999,
        symbol_interest_ttl_ms=999999,
        asset_interest_ttl_ms=999999,
        max_symbols_per_refresh=10,
        max_assets_per_refresh=10,
        max_feature_age_ms=999999,
    )
    ref = ShadowRefresher(redis=fake, cache=cache, cfg=cfg)

    ref.register_interest(symbol="BTCUSDT", asset_class="crypto")

    ref.refresh_news_once()
    ref.refresh_calendar_once()

    nf = cache.get("BTCUSDT", "crypto")
    assert nf is not None
    assert isinstance(nf, NewsFeatures)

    assert nf.ref == f"news:analysis:{uid}"
    assert nf.news_risk == pytest.approx(0.8)
    assert nf.surprise_score == pytest.approx(-0.3)
    assert nf.news_grade_id == 3
    assert nf.tags_mask == 5
    assert nf.primary_tag_id == 2
    assert nf.horizon_sec == 3600
    assert nf.confidence == pytest.approx(0.9)

    assert nf.event_tminus_sec == 120
    assert nf.event_grade_id == 2


def test_tickloop_attach_does_not_call_redis():
    from contexts import NewsFeatures
    from news_pipeline.enricher_shadow import NewsEnricherShadow

    fake = FakeRedis(hashes={})
    enricher = NewsEnricherShadow(redis=fake, per_symbol_cache_ms=0)

    # preload cache to avoid miss
    enricher.cache.news_by_symbol["BTCUSDT"] = NewsFeatures(ref="news:analysis:x", asof_ts_ms=get_ny_time_millis())

    class Ctx:
        def __init__(self):
            self.symbol = "BTCUSDT"
            self.news = None
            self.data_quality_flags = []
            self.extra = {}

    ctx = Ctx()
    enricher.attach(ctx, asset_class="crypto")

    assert fake.pipeline_calls == 0
    assert ctx.news is not None


def test_attach_fallback_to_extra_when_ctx_has_no_news_attr():
    from contexts import NewsFeatures
    from news_pipeline.enricher_shadow import NewsEnricherShadow
    from dataclasses import dataclass

    fake = FakeRedis(hashes={})
    enricher = NewsEnricherShadow(redis=fake, per_symbol_cache_ms=0, max_age_ms=0)

    nf = NewsFeatures(ref="news:analysis:uid1", asof_ts_ms=get_ny_time_millis())
    enricher.cache.news_by_symbol["ETHUSDT"] = nf

    @dataclass(frozen=True, slots=True)
    class FrozenCtx:
        symbol: str
        data_quality_flags: list
        extra: dict

    ctx = FrozenCtx(symbol="ETHUSDT", data_quality_flags=[], extra={})
    enricher.attach(ctx, asset_class="crypto")

    assert fake.pipeline_calls == 0
    assert ctx.extra.get("news") is not None
