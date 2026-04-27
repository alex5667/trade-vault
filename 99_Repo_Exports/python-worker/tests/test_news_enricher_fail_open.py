from __future__ import annotations

import types
import pytest


class DummyRedis:
    def pipeline(self, transaction=False):
        raise TimeoutError("boom")


class DummyCtx:
    def __init__(self):
        self.symbol = "BTCUSDT"
        self.news = "should_be_overwritten"
        self.data_quality_flags = []


def test_news_enricher_fail_open_sets_none_and_dq_flag(monkeypatch):
    # Patch the local _append_dq_flag function in enricher_sync
    called = []

    def fake_append(ctx, flag):
        called.append(flag)
        # mimic actual behavior
        if not hasattr(ctx, "data_quality_flags") or ctx.data_quality_flags is None:
            ctx.data_quality_flags = []
        if flag not in ctx.data_quality_flags:
            ctx.data_quality_flags.append(flag)

    from common import dq_flags
    monkeypatch.setattr(dq_flags, "append_dq_flag", fake_append)

    from news_pipeline.enricher_sync import NewsEnricherSync

    ctx = DummyCtx()
    e = NewsEnricherSync(redis=DummyRedis(), per_symbol_cache_ms=0)

    e.attach(ctx, asset_class="crypto")

    assert ctx.news is None
    assert "news_redis_error" in ctx.data_quality_flags
    assert "news_redis_error" in called
