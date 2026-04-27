from __future__ import annotations

from services.smt_bundle_aggregator import _news_gate_from_agg


def test_news_gate_blocks_in_window():
    now = 1_000_000
    # 60 sec before event, grade 4 => should block if pre_sec>=300
    agg = {"event_grade_id": "4", "event_tminus_sec": "60", "event_ts_ms": str(now + 60_000)}
    blocked, until_ms, reason = _news_gate_from_agg(agg, now, pre_sec=300, post_sec=300, hi_grade=4)
    assert blocked == 1
    assert until_ms >= now
    assert "NEWS" in reason

def test_news_gate_allows_outside_window():
    now = 1_000_000
    agg = {"event_grade_id": "4", "event_tminus_sec": "10000"}
    blocked, until_ms, reason = _news_gate_from_agg(agg, now, pre_sec=300, post_sec=300, hi_grade=4)
    assert blocked == 0
    assert until_ms == 0
