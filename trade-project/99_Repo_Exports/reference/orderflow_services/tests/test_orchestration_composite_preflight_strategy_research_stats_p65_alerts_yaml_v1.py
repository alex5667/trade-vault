from __future__ import annotations

"""P6.5 YAML-parse tests for strategy_research_stats reason-family alert rules.

Tests verify that both the primary and the tick_flow_full mirror copies of the
P6.5 Prometheus alert file:
  - parse as valid YAML,
  - contain all three expected alert names,
  - reference the P6.4 drilldown history metrics and the bounded family labels.

The alerts themselves depend on P6.4 metrics already emitted by the history
exporter; this test suite covers only the static asset validity.

Path resolution
---------------
  primary : <repo_root>/python-worker/orderflow_services/
  mirror   : <repo_root>/python-worker/tick_flow_full/orderflow_services/

Both paths are resolved relative to this test file so that invocation from any
working directory works correctly (pytest is typically run from python-worker/).
"""

import os

import pytest
import yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _alerts_path(tick_flow_full: bool = False) -> str:
    """Return the absolute path to the P6.5 alert YAML.

    When tick_flow_full=True, resolves to the mirror copy inside
    tick_flow_full/orderflow_services/.  When False, resolves to the primary
    copy inside orderflow_services/.

    Both are computed relative to this file so the test is location-agnostic.
    """
    # This file lives at:
    #   python-worker/orderflow_services/tests/test_*.py
    # The python-worker root is two levels up.
    here = os.path.dirname(os.path.abspath(__file__))
    # orderflow_services/ root (one level up from tests/)
    root = os.path.abspath(os.path.join(here, '..'))

    filename = 'prometheus_alerts_orchestration_composite_preflight_strategy_research_stats_p65.yml'

    if tick_flow_full:
        # python-worker/tick_flow_full/orderflow_services/<filename>
        return os.path.abspath(
            os.path.join(here, '..', '..', 'tick_flow_full', 'orderflow_services', filename)
        )
    # python-worker/orderflow_services/<filename>
    return os.path.join(root, filename)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('tick_flow_full', [False, True], ids=['primary', 'mirror'])
def test_strategy_research_stats_p65_alerts_yaml_parses(tick_flow_full: bool) -> None:
    """YAML file must be parseable and contain all three P6.5 alert names."""
    path = _alerts_path(tick_flow_full)
    with open(path, 'r', encoding='utf-8') as fh:
        doc = yaml.safe_load(fh)

    # Top-level structure
    assert 'groups' in doc and doc['groups'], "YAML must contain non-empty 'groups'"
    rules = doc['groups'][0].get('rules', [])
    assert rules, "Alert group must contain at least one rule"

    names = {r.get('alert') for r in rules}

    # All three P6.5 alert names must be present
    assert 'TradeStrategyResearchStatsPboHighSustained24h' in names, (
        f"Missing TradeStrategyResearchStatsPboHighSustained24h in {path}"
    )
    assert 'TradeStrategyResearchStatsReportStaleRecurring7d' in names, (
        f"Missing TradeStrategyResearchStatsReportStaleRecurring7d in {path}"
    )
    assert 'TradeStrategyResearchStatsPsrDsrLowShareRising24hVs7d' in names, (
        f"Missing TradeStrategyResearchStatsPsrDsrLowShareRising24hVs7d in {path}"
    )


@pytest.mark.parametrize('tick_flow_full', [False, True], ids=['primary', 'mirror'])
def test_strategy_research_stats_p65_alert_exprs_reference_history_metrics(tick_flow_full: bool) -> None:
    """All P6.5 alert expressions must reference the P6.4 history drilldown metrics
    and the expected family label values."""
    path = _alerts_path(tick_flow_full)
    with open(path, 'r', encoding='utf-8') as fh:
        doc = yaml.safe_load(fh)

    exprs = '\n'.join(
        str(rule.get('expr') or '') for rule in doc['groups'][0].get('rules', [])
    )

    # P6.4 drilldown history metric (per-purpose per-family counter)
    assert 'orchestration_composite_preflight_history_strategy_research_stats_reason_family_total' in exprs, (
        "Expressions must reference the P6.4 reason-family history metric"
    )
    # P6.4 events total (used as denominator for share computation)
    assert 'orchestration_composite_preflight_history_events_total' in exprs, (
        "Expressions must reference orchestration_composite_preflight_history_events_total"
    )

    # Bounded family label values used in the three alert expressions
    assert 'family="pbo_high"' in exprs, "pbo_high family label must appear in expressions"
    assert 'family="report_stale"' in exprs, "report_stale family label must appear in expressions"
    assert 'family=~"psr_low|dsr_low"' in exprs, "psr_low|dsr_low family regex must appear in expressions"
