from __future__ import annotations

from pathlib import Path

# python-worker/orderflow_services/tests/ -> parents[2] = python-worker/
PYTHON_WORKER_ROOT = Path(__file__).resolve().parents[2]
# repo root is one level above python-worker/
REPO_ROOT = PYTHON_WORKER_ROOT.parent


def test_p62_assets_exist():
    files = [
        PYTHON_WORKER_ROOT / 'orderflow_services' / 'prometheus_alerts_strategy_research_stats_p62.yml',
        REPO_ROOT / 'tick_flow_full' / 'orderflow_services' / 'prometheus_alerts_strategy_research_stats_p62.yml',
        PYTHON_WORKER_ROOT / 'orderflow_services' / 'grafana' / 'strategy_research_stats_p62.json',
        PYTHON_WORKER_ROOT / 'orderflow_services' / 'integrations' / 'run_with_strategy_research_stats_rollout_preflight_v1.sh',
        PYTHON_WORKER_ROOT / 'orderflow_services' / 'deploy' / 'systemd' / 'run_trade_strategy_research_stats_gated_compose_job_v1.sh',
        PYTHON_WORKER_ROOT / 'orderflow_services' / 'deploy' / 'compose' / 'docker-compose.strategy-research-stats-exporter-v1.yml',
        PYTHON_WORKER_ROOT / 'orderflow_services' / 'deploy' / 'systemd' / 'trade-strategy-research-stats-exporter.service',
        PYTHON_WORKER_ROOT / 'orderflow_services' / 'integrations' / 'metrics-proxy.strategy-research-stats-exporter-v1.conf',
    ]
    for path in files:
        assert path.exists(), str(path)


def test_alert_rules_reference_expected_metrics():
    text = (PYTHON_WORKER_ROOT / 'orderflow_services' / 'prometheus_alerts_strategy_research_stats_p62.yml').read_text(encoding='utf-8')
    assert 'strategy_research_stats_blocker_active' in text
    assert 'strategy_research_stats_report_age_seconds' in text
    assert 'strategy_research_stats_pbo' in text
    assert 'strategy_research_stats_psr' in text
    assert 'strategy_research_stats_dsr' in text


def test_dashboard_references_expected_metrics():
    text = (PYTHON_WORKER_ROOT / 'orderflow_services' / 'grafana' / 'strategy_research_stats_p62.json').read_text(encoding='utf-8')
    assert 'strategy_research_stats_blocker_active' in text
    assert 'strategy_research_stats_psr' in text
    assert 'strategy_research_stats_pbo' in text
