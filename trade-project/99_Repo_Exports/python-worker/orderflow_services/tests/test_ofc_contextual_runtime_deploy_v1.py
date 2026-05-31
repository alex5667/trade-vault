from pathlib import Path


def test_runtime_wrapper_contains_reloader_invocation():
    txt = Path("orderflow_services/deploy/systemd/run_trade_ofc_contextual_runtime_v1.sh").read_text()
    assert "orderflow_services.ofc_contextual_runtime_reloader_v1" in txt
    assert "OFC_RUNTIME_COMMAND" in txt
    assert "OFC_CTX_RUNTIME_OVERLAY_ENV_FILE" in txt


def test_runtime_service_uses_wrapper():
    txt = Path("orderflow_services/deploy/systemd/trade-ofc-contextual-runtime.service").read_text()
    assert "run_trade_ofc_contextual_runtime_v1.sh" in txt
    assert "Restart=always" in txt


def test_runtime_compose_contains_overlay_paths():
    txt = Path("orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-v1.yml").read_text()
    assert "OFC_CTX_RUNTIME_OVERLAY_ENV_FILE" in txt
    assert "OFC_CTX_ROLLBACK_FLAG_PATH" in txt
    assert "orderflow_services.ofc_contextual_runtime_reloader_v1" in txt
