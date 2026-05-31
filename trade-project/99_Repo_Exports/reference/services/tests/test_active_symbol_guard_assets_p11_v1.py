from pathlib import Path


def test_p11_assets_and_endpoints_exist():
    policy = Path('services/active_symbol_guard_incident_policy.py').read_text()
    notifier = Path('services/active_symbol_guard_incident_notifier.py').read_text()
    exporter = Path('services/active_symbol_guard_exporter.py').read_text()
    cli = Path('services/active_symbol_guard_cli.py').read_text()
    alerts = Path('services/prometheus_alerts_active_symbol_guard_p11.yml').read_text()
    assert 'severity' in policy and 'runbook_actions' in policy
    assert 'notify:telegram' in notifier
    assert '/api/active-symbol-guard/triage/symbol/' in exporter
    assert '--triage-symbol' in cli and '--triage-sid' in cli
    assert 'execution_active_symbol_guard_incident_total' in alerts
    assert 'execution_active_symbol_guard_notify_total' in alerts
