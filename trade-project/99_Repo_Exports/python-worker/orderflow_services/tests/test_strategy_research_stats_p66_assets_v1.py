from __future__ import annotations

from pathlib import Path

import yaml


def test_p66_alert_policy_assets_exist() -> None:
    base = Path(__file__).resolve().parents[1]
    assert (base / 'strategy_research_stats_alert_policy_exporter_v1.py').exists()
    assert (base / 'deploy' / 'compose' / 'docker-compose.strategy-research-stats-alert-policy-exporter-v1.yml').exists()
    assert (base / 'integrations' / 'metrics-proxy.strategy-research-stats-alert-policy-exporter-v1.conf').exists()
    assert (base / 'strategy_research_stats_alert_policy_override_v1.py').exists()


def test_p66_alerts_use_policy_metrics() -> None:
    base = Path(__file__).resolve().parents[1]
    doc = yaml.safe_load((base / 'prometheus_alerts_orchestration_composite_preflight_strategy_research_stats_p66.yml').read_text())
    text = str(doc)
    assert 'strategy_research_stats_alert_policy_min_events_24h' in text
    assert 'strategy_research_stats_alert_policy_min_events_7d' in text
    assert 'strategy_research_stats_alert_policy_suppress_active' in text
    assert 'strategy_research_stats_alert_policy_share_threshold_24h' in text
    assert 'strategy_research_stats_alert_policy_delta_vs_7d' in text


def test_p67_exporter_exposes_override_metrics() -> None:
    base = Path(__file__).resolve().parents[1]
    text = (base / 'strategy_research_stats_alert_policy_exporter_v1.py').read_text()
    assert 'strategy_research_stats_alert_policy_override_active' in text
    assert 'strategy_research_stats_alert_policy_override_remaining_seconds' in text
    assert 'strategy_research_stats_alert_policy_override_expire_unixtime' in text
    env = (base / 'deploy' / 'env' / 'latency-contract-sensitive-jobs.prod.env.example').read_text()
    assert 'STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX' in env
    assert 'STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_DEFAULT_TTL_S' in env
