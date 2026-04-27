from __future__ import annotations

import importlib

from ._fakes import FakeRedis


def _import_any(cands):
    last = None
    for name in cands:
        try:
            return importlib.import_module(name)
        except ImportError as e:
            last = e
    raise last


def test_calendar_hard_block_grade4_within_window():
    m = _import_any(['common.news_gate', 'news_gate', 'services.news_gate', 'news_pipeline.news_gate'])
    NewsGate = m.NewsGate
    r = FakeRedis()
    # asset_class 'forex' is normalized to 'fx' inside the patched NewsGate
    r.hash_store['calendar:agg:fx'] = {
        'event_grade_id': '4',
        'event_ts_ms': '1000000',
        'title': 'NFP',
        'event_id': 'nfp',
    }
    gate = NewsGate(redis_client=r, asset_class='forex', window_sec=300, grade_min=4)
    d = gate.decide(now_ts_ms=900000)
    assert d.hard_block is True
    assert d.risk_factor_bps == 0
    assert d.hard_reason in ('calendar_hi_impact', 'manual_hi_impact', 'NFP', 'nfp')


def test_calendar_soft_gate_grade2_within_soft_window():
    m = _import_any(['common.news_gate', 'news_gate', 'services.news_gate', 'news_pipeline.news_gate'])
    NewsGate = m.NewsGate
    r = FakeRedis()
    r.hash_store['calendar:agg:crypto'] = {
        'event_grade_id': '2',
        'event_ts_ms': '1000000',
        'title': 'Medium Event',
    }
    gate = NewsGate(redis_client=r, asset_class='crypto', window_sec=300, grade_min=4)
    d = gate.decide(now_ts_ms=900000)
    assert d.hard_block is False
    assert 0 <= d.risk_factor_bps <= 10000
    # default soft_grade2_bps is 5000 (unless overridden)
    assert d.risk_factor_bps <= 5000


def test_news_soft_gate_clamps_to_min_bps():
    m = _import_any(['common.news_gate', 'news_gate', 'services.news_gate', 'news_pipeline.news_gate'])
    NewsGate = m.NewsGate
    r = FakeRedis()
    gate = NewsGate(redis_client=r, asset_class='crypto', window_sec=300, grade_min=4)
    d = gate.decide(
        now_ts_ms=1_000_000,
        news_risk=1.0,
        news_grade_id=3,
        confidence=1.0,
        horizon_sec=600,
        asof_ts_ms=999_900,
    )
    assert d.hard_block is False
    assert d.risk_factor_bps >= 0
    # default soft_news_min_bps is 2500
    assert d.risk_factor_bps >= 2500
    assert d.risk_factor_bps <= 10000
