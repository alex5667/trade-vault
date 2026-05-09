from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from news_pipeline.enricher_sync import NewsEnricherSync
from tests.fake_redis import FakeRedis  # type: ignore


def test_enricher_attaches_news_features():
    r = FakeRedis()
    r.hashes["news:agg:BTCUSDT"] = {
        "ref": "news:analysis:abc",
        "risk_ema": "0.5",
        "surprise_ema": "0.1",
        "news_grade_id": "2",
        "tags_mask": "7",
        "primary_tag_id": "3",
        "confidence": "0.9",
        "horizon_sec": "1800",
        "asof_ts_ms": "1700000000000",
    }
    r.hashes["calendar:agg:crypto"] = {"event_tminus_sec": "600", "event_grade_id": "4"}

    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)

    ctx = SimpleNamespace(symbol="BTCUSDT", news=None, data_quality_flags=[])
    enr.attach(ctx, asset_class="crypto")  # type: ignore[arg-type]

    assert ctx.news is not None
    assert ctx.news.ref == "news:analysis:abc"
    assert ctx.news.event_tminus_sec == 600
    assert ctx.news.event_grade_id == 4


def test_enricher_deterministic_time_with_now_ts_ms():
    """Test that enricher uses provided now_ts_ms for deterministic tminus calculation"""
    r = FakeRedis()
    # Event at ts 1700000000000 + 600*1000 = 1700000600000
    event_ts = 1700000600000
    r.hashes["calendar:agg:crypto"] = {
        "event_ts_ms": str(event_ts),
        "event_grade_id": "4"
    }

    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)

    # Test with deterministic now_ts_ms
    now_ts = 1700000000000  # Exactly 600 seconds before event
    ctx = SimpleNamespace(symbol="BTCUSDT", news=None, data_quality_flags=[])
    enr.attach(ctx, asset_class="crypto", now_ts_ms=now_ts)

    assert ctx.news is not None
    assert ctx.news.event_tminus_sec == 600  # Should be exactly 600
    assert "time_fallback_wall_clock" not in ctx.data_quality_flags


def test_enricher_fallback_to_wall_clock():
    """Test that enricher falls back to wall clock when now_ts_ms not provided"""
    r = FakeRedis()
    event_ts = 1700000600000
    r.hashes["calendar:agg:crypto"] = {
        "event_ts_ms": str(event_ts),
        "event_grade_id": "4"
    }

    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)

    # Mock time.time() to return predictable value
    with patch('news_pipeline.enricher_sync.time.time', return_value=1700000000.0):  # 600 seconds before event
        ctx = SimpleNamespace(symbol="BTCUSDT", news=None, data_quality_flags=[])
        enr.attach(ctx, asset_class="crypto")  # No now_ts_ms provided

        assert ctx.news is not None
        assert ctx.news.event_tminus_sec == 600
        assert "time_fallback_wall_clock" in ctx.data_quality_flags


def test_enricher_legacy_event_tminus_fallback():
    """Test fallback to legacy event_tminus_sec when event_ts_ms missing"""
    r = FakeRedis()
    r.hashes["calendar:agg:crypto"] = {
        "event_tminus_sec": "300",  # Legacy field
        "event_grade_id": "4"
        # No event_ts_ms
    }

    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)

    ctx = SimpleNamespace(symbol="BTCUSDT", news=None, data_quality_flags=[])
    enr.attach(ctx, asset_class="crypto", now_ts_ms=1700000000000)

    assert ctx.news is not None
    assert ctx.news.event_tminus_sec == 300  # Should use legacy value
    # Note: In real implementation this would set dq_flag, but our test doesn't check that


def test_enricher_forex_asset_class_normalization():
    """Test that forex asset class is normalized to fx"""
    r = FakeRedis()
    r.hashes["calendar:agg:fx"] = {
        "event_ts_ms": "1700000600000",
        "event_grade_id": "3"
    }

    enr = NewsEnricherSync(redis=r, per_symbol_cache_ms=0)

    ctx = SimpleNamespace(symbol="EURUSD", news=None, data_quality_flags=[])
    enr.attach(ctx, asset_class="forex", now_ts_ms=1700000000000)  # Should map to fx

    assert ctx.news is not None
    assert ctx.news.event_tminus_sec == 600
