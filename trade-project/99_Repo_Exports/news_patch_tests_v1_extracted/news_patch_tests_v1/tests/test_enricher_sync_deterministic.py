from __future__ import annotations

import importlib
import sys
import types
from dataclasses import dataclass, field
from typing import List, Optional

from ._fakes import FakeRedis


def _ensure_contexts_stub():
    try:
        import contexts  # noqa: F401
        return
    except ImportError:
        pass

    mod = types.ModuleType('contexts')

    @dataclass
    class NewsFeatures:
        ref: str = ''
        news_risk: float = 0.0
        surprise_score: float = 0.0
        news_grade_id: int = 0
        tags_mask: int = 0
        primary_tag_id: int = 0
        confidence: float = 0.0
        horizon_sec: int = 0
        asof_ts_ms: int = 0
        event_tminus_sec: int = -1
        event_grade_id: int = 0

    @dataclass
    class OrderflowSignalContext:
        symbol: str
        asset_class: str = ''
        news: Optional[NewsFeatures] = None
        data_quality_flags: List[str] = field(default_factory=list)

    mod.NewsFeatures = NewsFeatures
    mod.OrderflowSignalContext = OrderflowSignalContext
    sys.modules['contexts'] = mod


def _import_any(cands):
    last = None
    for name in cands:
        try:
            return importlib.import_module(name)
        except ImportError as e:
            last = e
    raise last


def test_enricher_uses_tick_time_and_computes_tminus():
    _ensure_contexts_stub()
    contexts = importlib.import_module('contexts')
    m = _import_any(['enricher_sync', 'services.enricher_sync', 'news_pipeline.enricher_sync'])
    NewsEnricherSync = m.NewsEnricherSync

    r = FakeRedis()
    r.hash_store['news:agg:BTCUSDT'] = {
        'risk_ema': '0.4',
        'surprise_ema': '0.1',
        'news_grade_id': '2',
        'tags_mask': '3',
        'primary_tag_id': '1',
        'confidence': '0.9',
        'horizon_sec': '600',
        'asof_ts_ms': '800000',
        'ref': 'abc',
    }
    # asset_class 'forex' should map to calendar:agg:fx
    r.hash_store['calendar:agg:fx'] = {
        'event_ts_ms': '1000000',
        'event_grade_id': '4',
    }

    ctx = contexts.OrderflowSignalContext(symbol='BTCUSDT')
    e = NewsEnricherSync(redis=r, per_symbol_cache_ms=1500)
    e.attach(ctx, asset_class='forex', now_ts_ms=900000)

    assert ctx.news is not None
    assert ctx.news.event_tminus_sec == 100
    assert ctx.news.event_grade_id == 4
    assert ctx.news.ref == 'news:analysis:abc'
    assert 'time_fallback_wall_clock' not in ctx.data_quality_flags


def test_enricher_fallback_marks_dq_flag():
    _ensure_contexts_stub()
    contexts = importlib.import_module('contexts')
    m = _import_any(['enricher_sync', 'services.enricher_sync', 'news_pipeline.enricher_sync'])
    NewsEnricherSync = m.NewsEnricherSync

    r = FakeRedis()
    r.hash_store['news:agg:GLOBAL'] = {}
    ctx = contexts.OrderflowSignalContext(symbol='')
    e = NewsEnricherSync(redis=r, per_symbol_cache_ms=1500)
    e.attach(ctx, asset_class='crypto', now_ts_ms=None)
    assert 'time_fallback_wall_clock' in ctx.data_quality_flags
