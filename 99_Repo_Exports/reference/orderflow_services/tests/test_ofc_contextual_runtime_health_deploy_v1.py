from pathlib import Path


def test_runtime_health_service_contains_exporter_entrypoint():
    txt = Path("orderflow_services/deploy/systemd/trade-ofc-contextual-runtime-health-exporter.service").read_text(encoding="utf-8")
    assert "orderflow_services.ofc_contextual_runtime_health_exporter_v1" in txt
    assert "Restart=always" in txt


def test_runtime_health_compose_contains_state_path_and_port():
    txt = Path("orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-health-exporter-v1.yml").read_text(encoding="utf-8")
    assert "OFC_CTX_RUNTIME_RELOADER_STATE_PATH" in txt
    assert "OFC_CTX_RUNTIME_HEALTH_EXPORTER_PORT" in txt


def test_runtime_health_alerts_reference_runtime_metrics():
    txt = Path("orderflow_services/prometheus_alerts_ofc_contextual_runtime_health_v1.yml").read_text(encoding="utf-8")
    assert "ofc_ctx_runtime_reloader_state_age_seconds" in txt
    assert "ofc_ctx_runtime_reloader_overlay_dirty" in txt
    assert "changes(ofc_ctx_runtime_reloader_restart_count[15m])" in txt
