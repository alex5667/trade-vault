from __future__ import annotations

from orderflow_services.strategy_research_guard_state_exporter_v1 import _compute_state, classify_reason_kind


def test_classify_reason_kind_known_buckets() -> None:
    assert classify_reason_kind('pbo_high:0.31') == 'pbo_high'
    assert classify_reason_kind('PSR below threshold') == 'psr_low'
    assert classify_reason_kind('manual override requested') == 'manual'
    assert classify_reason_kind('') == 'none'


def test_compute_state_prefers_summary_and_blocker_fields() -> None:
    summary = {
        'updated_ts_ms': '1000',
        'success': '1',
        'report_only': '1',
        'primary_metric_value': '0.17',
        'net_expectancy': '0.06',
        'precision_at_top_x': '0.63',
        'mean_r': '0.28',
        'downside_adjusted_return': '0.11',
        'hit_rate_conditioned_on_cost': '0.57',
        'psr': '0.78',
        'dsr': '0.55',
        'pbo': '0.18',
        'cscv_splits': '16',
        'chosen_variant_unique': '1',
    }
    blocker = {
        'blocked': '1',
        'report_only': '0',
        'reason': 'pbo_high:0.24',
    }
    state = _compute_state(summary, blocker, now_ms=31_000)
    assert state.summary_present == 1.0
    assert state.blocker_present == 1.0
    assert state.last_success == 1.0
    assert state.report_only == 0.0
    assert state.blocker_active == 1.0
    assert state.blocker_reason_kind == 'pbo_high'
    assert state.report_age_seconds == 30.0
    assert state.psr == 0.78
    assert state.dsr == 0.55
    assert state.pbo == 0.18
    assert state.chosen_variant_unique == 1.0
