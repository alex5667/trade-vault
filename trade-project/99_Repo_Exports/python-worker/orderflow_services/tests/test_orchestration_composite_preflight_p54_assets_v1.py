from __future__ import annotations

from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text(encoding='utf-8')


def test_compose_and_systemd_assets_exist() -> None:
    compose_txt = _read('orderflow_services/deploy/compose/docker-compose.orchestration-composite-preflight-exporter-v1.yml')
    assert 'orchestration-composite-preflight-exporter' in compose_txt
    assert 'orchestration_composite_preflight_exporter_v1' in compose_txt

    service_txt = _read('orderflow_services/deploy/systemd/trade-orchestration-composite-preflight-exporter.service')
    assert 'run_trade_orchestration_composite_preflight_exporter_v1.sh' in service_txt

    wrapper_txt = _read('orderflow_services/deploy/systemd/run_trade_orchestration_composite_preflight_exporter_v1.sh')
    assert 'orchestration_composite_preflight_exporter_v1' in wrapper_txt


def test_alerts_and_grafana_reference_composite_metrics() -> None:
    alerts = _read('orderflow_services/prometheus_alerts_orchestration_composite_preflight_p54.yml')
    assert 'orchestration_composite_preflight_decision_status' in alerts
    assert 'orchestration_composite_preflight_state_age_seconds' in alerts

    dashboard = _read('orderflow_services/grafana/orchestration_composite_preflight_p54.json')
    assert 'orchestration_composite_preflight_selected_reason_code' in dashboard
    assert 'orchestration_composite_preflight_selected_source' in dashboard
    assert 'orchestration_composite_preflight_decision_status' in dashboard


def test_metrics_proxy_and_env_examples_include_exporter() -> None:
    proxy = _read('orderflow_services/integrations/metrics-proxy.orchestration-composite-preflight-exporter-v1.conf')
    assert '/m/orchestration-composite-preflight-exporter' in proxy
    assert 'ORCHESTRATION_COMPOSITE_PREFLIGHT_EXPORTER_PORT' in proxy

    for rel in [
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example',
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example',
    ]:
        txt = _read(rel)
        assert 'ORCHESTRATION_COMPOSITE_PREFLIGHT_EXPORTER_PORT=' in txt
        assert 'ORCHESTRATION_COMPOSITE_PREFLIGHT_EXPORTER_INTERVAL_S=' in txt
        assert 'ORCHESTRATION_COMPOSITE_PREFLIGHT_EXPORTER_PURPOSES=' in txt
