from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding='utf-8')


def test_alert_yaml_has_required_rules_main_and_mirror() -> None:
    for rel in [
        'orderflow_services/prometheus_alerts_strategy_research_guard_p51.yml',
        'tick_flow_full/orderflow_services/prometheus_alerts_strategy_research_guard_p51.yml',
    ]:
        doc = yaml.safe_load(_read(rel))
        alerts = {rule['alert'] for group in doc['groups'] for rule in group.get('rules', []) if 'alert' in rule}
        assert 'TradeStrategyResearchGuardBlockerActiveCrit' in alerts
        assert 'TradeStrategyResearchGuardReportStaleWarn' in alerts
        assert 'TradeStrategyResearchGuardPBOHighWarn' in alerts


def test_compose_runs_exporter_module() -> None:
    txt = _read('orderflow_services/deploy/compose/docker-compose.strategy-research-guard-exporter-v1.yml')
    assert 'strategy-research-guard-exporter:' in txt
    assert 'orderflow_services.strategy_research_guard_state_exporter_v1' in txt
    assert 'STRATEGY_RESEARCH_GUARD_SUMMARY_KEY' in txt
    assert 'STRATEGY_RESEARCH_GUARD_BLOCKER_KEY' in txt


def test_systemd_wrapper_and_service_present() -> None:
    wrapper = _read('orderflow_services/deploy/systemd/run_trade_strategy_research_guard_exporter_v1.sh')
    service = _read('orderflow_services/deploy/systemd/trade-strategy-research-guard-exporter.service')
    assert 'strategy_research_guard_state_exporter_v1' in wrapper
    assert 'EnvironmentFile=' in service
    assert 'run_trade_strategy_research_guard_exporter_v1.sh' in service


def test_env_examples_cover_exporter_runtime() -> None:
    for rel in [
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example',
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example',
    ]:
        txt = _read(rel)
        for needle in [
            'STRATEGY_RESEARCH_GUARD_SUMMARY_KEY=',
            'STRATEGY_RESEARCH_GUARD_BLOCKER_KEY=',
            'STRATEGY_RESEARCH_GUARD_EXPORTER_PORT=',
            'STRATEGY_RESEARCH_GUARD_EXPORTER_INTERVAL_S=',
        ]:
            assert needle in txt


def test_metrics_proxy_route_snippet_present() -> None:
    txt = _read('orderflow_services/integrations/metrics-proxy.strategy-research-guard-exporter-v1.conf')
    assert '/m/strategy-research-guard-exporter' in txt
    assert 'strategy-research-guard-exporter' in txt
    assert '/metrics' in txt
