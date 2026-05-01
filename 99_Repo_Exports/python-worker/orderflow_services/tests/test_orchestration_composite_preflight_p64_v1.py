from __future__ import annotations

"""Tests for P6.4 strategy_research_stats drilldown in composite preflight exporter."""

from orderflow_services.orchestration_composite_preflight_exporter_v1 import (
    compute_purpose_state,
    normalize_reason_code,
    research_stats_reason_family,
)
from orderflow_services.orchestration_composite_preflight_history_exporter_v1 import (
    HistoryEvent,
    aggregate_events,
    render_metrics,
)


def test_normalize_reason_code_supports_strategy_research_stats() -> None:
    """Full strategy_research_stats reason codes are preserved; unknown collapse to :other."""
    assert normalize_reason_code('strategy_research_stats', 'strategy_research_stats:psr_low') == 'strategy_research_stats:psr_low'
    assert normalize_reason_code('strategy_research_stats', 'weird_runtime_failure') == 'strategy_research_stats:other'


def test_compute_purpose_state_extracts_strategy_research_stats_family() -> None:
    """PurposeState.strategy_research_stats_reason_family is populated from hash fields."""
    raw = {
        'updated_ts_ms': '1000',
        'status': 'block',
        'selected_source': 'strategy_research_stats',
        'selected_reason_code': 'strategy_research_stats:pbo_high',
        'strategy_research_stats_status': 'block',
        'strategy_research_stats_reason': 'pbo_high',
    },
    state = compute_purpose_state('conf_score_guardrails_promote', raw, now_ms=31_000)
    assert state.selected_reason_code == 'strategy_research_stats:pbo_high'
    assert state.strategy_research_stats_reason_family == 'pbo_high'


def test_research_stats_reason_family_bounds_unknown_values() -> None:
    """Unknown sub-reasons collapse to 'other'; known ones are matched correctly."""
    assert research_stats_reason_family('strategy_research_stats:psr_low') == 'psr_low'
    assert research_stats_reason_family('report_stale') == 'report_stale'
    assert research_stats_reason_family('something_new') == 'other'


def test_research_stats_reason_family_handles_empty_and_ok() -> None:
    """Empty or 'ok' raw reasons map to 'ok' family."""
    assert research_stats_reason_family('') == 'ok'
    assert research_stats_reason_family('ok') == 'ok'
    assert research_stats_reason_family('strategy_research_stats:ok') == 'ok'


def test_research_stats_reason_family_all_families() -> None:
    """Each bounded family is correctly matched from full reason code strings."""
    families = {
        'psr_low': 'strategy_research_stats:psr_low',
        'dsr_low': 'strategy_research_stats:dsr_low',
        'pbo_high': 'strategy_research_stats:pbo_high',
        'metric_low': 'strategy_research_stats:metric_low',
        'report_stale': 'strategy_research_stats:report_stale',
        'state_missing': 'strategy_research_stats:state_missing',
        'invalid': 'strategy_research_stats:invalid',
    },
    for expected_family, reason_code in families.items():
        assert research_stats_reason_family(reason_code) == expected_family, (
            f'Expected family {expected_family!r} for reason code {reason_code!r}'
        )


def test_history_render_emits_strategy_research_stats_family_metrics() -> None:
    """render_metrics emits the P6.4 drilldown lines for strategy_research_stats families."""
    now_ms = 8 * 24 * 60 * 60 * 1000
    events = [
        HistoryEvent('conf_score_guardrails_promote', 'block', 'strategy_research_stats', 'strategy_research_stats:psr_low', now_ms - 1000),
        HistoryEvent('conf_score_guardrails_promote', 'block', 'strategy_research_stats', 'strategy_research_stats:psr_low', now_ms - 2000),
        HistoryEvent('conf_score_guardrails_promote', 'invalid', 'strategy_research_stats', 'strategy_research_stats:report_stale', now_ms - 3000),
    ]
    counts, totals, scanned = aggregate_events(events, now_ms=now_ms)
    body = render_metrics(
        counts,
        totals,
        {
            '24h': type('S', (), {'complete': 1.0, 'oldest_age_seconds': 1.0, 'scanned_events': float(scanned['24h']), 'total_events': float(scanned['24h'])})(),
            '7d': type('S', (), {'complete': 1.0, 'oldest_age_seconds': 1.0, 'scanned_events': float(scanned['7d']), 'total_events': float(scanned['7d'])})(),
        },
        purposes=['conf_score_guardrails_promote'],
        scan_truncated=0.0,
    )
    # per-purpose per-window family total
    assert ('orchestration_composite_preflight_history_strategy_research_stats_reason_family_total'
            '{window="24h",purpose="conf_score_guardrails_promote",decision_status="block",family="psr_low"} 2.0') in body
    # cross-purpose summary
    assert ('orchestration_composite_preflight_history_strategy_research_stats_reason_family_summary'
            '{window="24h",decision_status="invalid",family="report_stale"} 1.0') in body
