from pathlib import Path


def test_p8_alert_rules_cover_conflicts_tombstones_and_resurrection():
    text = Path('services/prometheus_alerts_active_symbol_guard_p8.yml').read_text()
    assert 'execution_active_symbol_guard_cas_conflict_total' in text
    assert 'execution_active_symbol_guard_released_tombstone_age_ms' in text
    assert 'execution_active_symbol_guard_resurrection_attempt_total' in text
