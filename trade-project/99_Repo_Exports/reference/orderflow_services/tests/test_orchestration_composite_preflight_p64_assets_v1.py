from __future__ import annotations

"""P6.4 asset-presence tests: verify Grafana dashboard exists and references drilldown metrics."""

from pathlib import Path


def test_p64_dashboard_exists_and_references_strategy_research_stats_drilldown() -> None:
    """Grafana dashboard file must exist and reference all three P6.4 drilldown metric families."""
    body = Path('orderflow_services/grafana/orchestration_composite_preflight_strategy_research_stats_p64.json').read_text(encoding='utf-8')
    # live drilldown gauge
    assert 'orchestration_composite_preflight_strategy_research_stats_reason_family' in body
    # history drilldown gauge
    assert 'orchestration_composite_preflight_history_strategy_research_stats_reason_family_total' in body
    # source-level status gauge
    assert 'orchestration_composite_preflight_source_status' in body
