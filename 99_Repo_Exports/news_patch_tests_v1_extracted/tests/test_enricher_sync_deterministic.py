from dataclasses import dataclass


class FakePipeline:
    def __init__(self, r):
        self._r = r
        self._ops = []

    def hgetall(self, key):
        self._ops.append(("hgetall", key))
        return self

    def execute(self):
        out = []
        for op, key in self._ops:
            if op == "hgetall":
                out.append(self._r.hgetall(key))
        return out


class FakeRedis:
    def __init__(self):
        self._hash = {}
        self.pipeline_calls = 0

    def hset(self, key, mapping):
        self._hash.setdefault(key, {}).update(mapping)

    def hgetall(self, key):
        return dict(self._hash.get(key, {}))

    def pipeline(self, transaction=False):
        self.pipeline_calls += 1
        return FakePipeline(self)


# Fallback minimal contexts if your repo doesn't provide them
try:
    from contexts import NewsFeatures, OrderflowSignalContext  # type: ignore
except Exception:

    @dataclass
    class NewsFeatures:
        ref: str
        news_risk: float
        surprise_score: float
        news_grade_id: int
        tags_mask: int
        primary_tag_id: int
        confidence: float
        horizon_sec: int
        asof_ts_ms: int
        event_tminus_sec: int
        event_grade_id: int

    @dataclass
    class OrderflowSignalContext:
        symbol: str
        news: NewsFeatures | None = None
        data_quality_flags: list[str] | None = None


def test_enricher_uses_tick_time_and_forex_maps_to_fx():
    from enricher_sync import NewsEnricherSync

    r = FakeRedis()
    r.hset(
        "news:agg:BTCUSDT",
        {
            "risk_ema": "0.2",
            "surprise_ema": "-0.1",
            "news_grade_id": "2",
            "horizon_sec": "600",
            "asof_ts_ms": "850000",
        },
    )
    r.hset(
        "calendar:agg:fx",
        {
            "event_grade_id": "4",
            "event_ts_ms": "1000000",
            "updated_ts_ms": "800000",
        },
    )

    ctx = OrderflowSignalContext(symbol="BTCUSDT", data_quality_flags=[])
    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=1500)

    enr.attach(ctx, asset_class="forex", now_ts_ms=900_000)

    assert ctx.news is not None
    assert ctx.news.event_tminus_sec == 100  # (1_000_000 - 900_000)/1000
    assert "time_fallback_wall_clock" not in (ctx.data_quality_flags or [])


def test_enricher_sets_dq_flag_on_wall_clock_fallback():
    from enricher_sync import NewsEnricherSync

    r = FakeRedis()
    r.hset("news:agg:GLOBAL", {"risk_ema": "0"})

    ctx = OrderflowSignalContext(symbol="", data_quality_flags=[])
    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=1500)

    enr.attach(ctx, asset_class="crypto", now_ts_ms=None)

    assert "time_fallback_wall_clock" in (ctx.data_quality_flags or [])
