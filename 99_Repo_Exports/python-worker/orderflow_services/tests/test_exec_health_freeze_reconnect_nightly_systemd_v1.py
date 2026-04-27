from pathlib import Path


def test_systemd_service_and_timer_present():
    svc = Path('orderflow_services/deploy/systemd/exec-health-freeze-reconnect-nightly.service').read_text()
    timer = Path('orderflow_services/deploy/systemd/exec-health-freeze-reconnect-nightly.timer').read_text()
    wrapper = Path('orderflow_services/deploy/systemd/run-exec-health-freeze-reconnect-nightly-v1.sh').read_text()
    assert 'docker compose -f' in wrapper
    assert 'ExecStart=/opt/scanner_infra/orderflow_services/deploy/systemd/run-exec-health-freeze-reconnect-nightly-v1.sh' in svc
    assert 'OnCalendar=*-*-* 03:17:00' in timer
