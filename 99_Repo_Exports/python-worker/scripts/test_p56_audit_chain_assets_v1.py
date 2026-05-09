from __future__ import annotations

"""P5.6 asset integrity tests: Prometheus rules and Grafana dashboard must be valid and contain expected keys."""

import json
from pathlib import Path

# Root of the python-worker directory (two levels up from scripts/)
ROOT = Path(__file__).resolve().parents[1]


def test_prometheus_rules_exist() -> None:
    """Prometheus rules file must exist and contain all 4 expected alert names."""
    path = ROOT / "monitoring" / "prometheus_rules_execution_p56_audit_chain.yml"
    text = path.read_text(encoding="utf-8")
    assert "TradeExecutionAuditChainReportStale" in text
    assert "TradeExecutionAuditChainBrokenSignalPlan" in text
    assert "TradeExecutionAuditChainBrokenTradeLink" in text
    assert "TradeExecutionAuditChainBrokenAnalyticsLink" in text
    assert "domain: execution-audit" in text


def test_grafana_dashboard_valid_json() -> None:
    """Grafana dashboard must be valid JSON with correct uid and required panels."""
    path = ROOT / "monitoring" / "grafana" / "dashboards" / "trade_execution_p56_audit_chain_health.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc["uid"] == "trade-exec-audit-p56"
    titles = {panel["title"] for panel in doc["panels"]}
    assert "Broken chain total" in titles
    assert "Report freshness (s)" in titles
    assert "Broken by kind" in titles


def test_prometheus_rules_valid_yaml() -> None:
    """Prometheus rules YAML must have groups -> rules structure."""
    try:
        import yaml
    except ImportError:
        # yaml not installed: skip YAML validation but don't fail
        return
    path = ROOT / "monitoring" / "prometheus_rules_execution_p56_audit_chain.yml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc.get("groups"), list)
    group = doc["groups"][0]
    assert "rules" in group
    rule_names = [r.get("alert") for r in group["rules"]]
    assert "TradeExecutionAuditChainReportStale" in rule_names
