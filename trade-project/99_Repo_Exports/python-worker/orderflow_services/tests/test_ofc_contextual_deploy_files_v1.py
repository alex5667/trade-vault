from pathlib import Path


def test_rollout_systemd_and_compose_files_present():
    svc = Path("orderflow_services/deploy/systemd/trade-ofc-contextual-rollout-controller.service").read_text()
    timer = Path("orderflow_services/deploy/systemd/trade-ofc-contextual-rollout-controller.timer").read_text()
    wrapper = Path("orderflow_services/deploy/systemd/run_trade_ofc_contextual_rollout_controller_v1.sh").read_text()
    compose = Path("orderflow_services/deploy/compose/docker-compose.ofc-contextual-rollout-controller-v1.yml").read_text()
    assert "run_trade_latency_gated_compose_job_v1.sh" in wrapper
    assert "ExecStart=/bin/bash -lc 'cd \"${TRADE_REPO_ROOT:?}\" && ./python-worker/orderflow_services/deploy/systemd/run_trade_ofc_contextual_rollout_controller_v1.sh'" in svc
    assert "OnCalendar=*:0/10" in timer
    assert "orderflow_services.ofc_contextual_rollout_controller_v1" in compose


def test_exporter_systemd_files_present():
    svc = Path("orderflow_services/deploy/systemd/trade-ofc-contextual-exporter.service").read_text()
    wrapper = Path("orderflow_services/deploy/systemd/run_trade_ofc_contextual_exporter_v1.sh").read_text()
    assert "docker-compose.ofc-contextual-exporter-v1.yml" in wrapper
    assert "Restart=always" in svc
