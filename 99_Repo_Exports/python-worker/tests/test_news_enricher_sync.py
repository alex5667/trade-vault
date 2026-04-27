from utils.time_utils import get_ny_time_millis
import time
import pytest

from news_pipeline.enricher_sync import NewsEnricherSync


class DummyCtx:
    def __init__(self, symbol="BTCUSDT", asset_class="crypto"):
        self.symbol = symbol
        self.asset_class = asset_class
        self.news = None
        self.data_quality_flags = []

    def __setattr__(self, name, value):
        self.__dict__[name] = value


class FakePipe:
    def __init__(self, owner):
        self.owner = owner
        self.calls = []

    def hmget(self, key, fields):
        self.calls.append(("hmget", key, tuple(fields)))
        return self

    def execute(self):
        return self.owner._execute()


class FakeRedis:
    def __init__(self):
        self.mode = "ok"
        self.exec_count = 0
        self.last_pipe = None

        self.news_map = {
            "ref": "abc123",
            "risk_ema": "0.8",
            "surprise_ema": "-0.2",
            "news_grade_id": "3",
            "tags_mask": "5",
            "primary_tag_id": "2",
            "confidence": "0.9",
            "horizon_sec": "3600",
            "asof_ts_ms": str(get_ny_time_millis()),
        }
        self.cal_map = {
            "event_tminus_sec": "120",
            "event_grade_id": "2",
            "updated_ts_ms": str(get_ny_time_millis()),
        }

    def pipeline(self, transaction=False):
        self.last_pipe = FakePipe(self)
        return self.last_pipe

    def _execute(self):
        self.exec_count += 1
        if self.mode == "timeout":
            raise TimeoutError("redis timeout")
        if self.mode == "error":
            raise RuntimeError("redis down")

        # HMGET returns list aligned with requested fields.
        # We infer requested fields from last_pipe.calls.
        calls = self.last_pipe.calls
        assert calls[0][0] == "hmget"
        news_fields = calls[0][2]
        news_vals = [self.news_map.get(f, None) for f in news_fields]

        if len(calls) > 1:
            cal_fields = calls[1][2]
            cal_vals = [self.cal_map.get(f, None) for f in cal_fields]
            return [news_vals, cal_vals]

        return [news_vals]


def test_cache_skips_redis_second_time(monkeypatch):
    r = FakeRedis()
    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=10_000)

    ctx = DummyCtx("BTCUSDT", "crypto")
    enr.attach(ctx, asset_class="crypto")
    assert ctx.news is not None
    first_exec = r.exec_count

    enr.attach(ctx, asset_class="crypto")
    assert r.exec_count == first_exec  # no new redis call


def test_timeout_uses_stale_cache():
    r = FakeRedis()
    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=10000)  # enable cache

    ctx = DummyCtx("BTCUSDT", "crypto")
    enr.attach(ctx, asset_class="crypto")
    assert ctx.news is not None

    r.mode = "timeout"
    ctx2 = DummyCtx("BTCUSDT", "crypto")
    enr.attach(ctx2, asset_class="crypto")
    assert ctx2.news is not None  # stale


def test_circuit_breaker_opens_and_skips_redis():
    r = FakeRedis()
    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)

    # force errors
    r.mode = "error"
    ctx = DummyCtx("BTCUSDT", "crypto")
    enr.attach(ctx, asset_class="crypto")
    enr.attach(ctx, asset_class="crypto")
    enr.attach(ctx, asset_class="crypto")  # threshold default=3

    # CB open: next attach should not execute redis
    before = r.exec_count
    enr.attach(ctx, asset_class="crypto")
    after = r.exec_count
    assert after == before
    assert "news_cb_open" in ctx.data_quality_flags


def test_ref_is_normalized_to_key():
    r = FakeRedis()
    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)
    ctx = DummyCtx("BTCUSDT", "crypto")
    enr.attach(ctx, asset_class="crypto")
    assert ctx.news.ref.startswith("news:analysis:")
