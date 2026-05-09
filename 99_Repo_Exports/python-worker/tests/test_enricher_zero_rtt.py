
import pytest

from news_pipeline.enricher_zero_rtt import NewsAggCache, NewsEnricherZeroRTT
from utils.time_utils import get_ny_time_millis


class FakePipe:
    def __init__(self, r):
        self.r = r
        self.calls = []

    def hmget(self, key, fields):
        self.calls.append(("hmget", key, tuple(fields)))
        return self

    def execute(self):
        out = []
        for _, key, fields in self.calls:
            h = self.r.data.get(key, {})
            out.append([h.get(f) for f in fields])
        return out


class FakeRedis:
    def __init__(self):
        self.data = {}

    def pipeline(self, transaction=False):
        return FakePipe(self)


class Ctx:
    def __init__(self, symbol="BTCUSDT"):
        self.symbol = symbol
        self.news = None
        self.data_quality_flags = []


@pytest.fixture()
def fake_redis():
    r = FakeRedis()
    now = get_ny_time_millis()
    r.data["news:agg:BTCUSDT"] = {
        "ref": "abc",
        "risk_ema": "0.8",
        "surprise_ema": "0.2",
        "news_grade_id": "3",
        "tags_mask": "5",
        "primary_tag_id": "2",
        "confidence": "0.9",
        "horizon_sec": "3600",
        "asof_ts_ms": str(now),
    }
    r.data["calendar:agg:crypto"] = {
        "event_tminus_sec": "120",
        "event_grade_id": "2",
    }
    return r


def test_cache_refresh_and_attach(fake_redis):
    cache = NewsAggCache(redis_fast=fake_redis, poll_ms=999999, enable_calendar=True)
    cache.note_symbol("BTCUSDT", "crypto")
    cache.refresh_once()

    enr = NewsEnricherZeroRTT(cache=cache)
    ctx = Ctx("BTCUSDT")
    enr.attach(ctx, asset_class="crypto")

    assert ctx.news is not None
    assert ctx.news.ref == "abc"
    assert abs(ctx.news.news_risk - 0.8) < 1e-9
    assert ctx.news.event_tminus_sec == 120
    assert ctx.news.event_grade_id == 2


def test_cache_stale_drop(fake_redis):
    # сделаем asof_ts_ms сильно старым
    old = get_ny_time_millis() - 999999
    fake_redis.data["news:agg:BTCUSDT"]["asof_ts_ms"] = str(old)

    cache = NewsAggCache(
        redis_fast=fake_redis,
        poll_ms=999999,
        enable_calendar=False,
        stale_drop_ms=1000,  # 1s
    )
    cache.note_symbol("BTCUSDT", "")
    cache.refresh_once()

    nf = cache.get_features("BTCUSDT", "")
    assert nf is None
