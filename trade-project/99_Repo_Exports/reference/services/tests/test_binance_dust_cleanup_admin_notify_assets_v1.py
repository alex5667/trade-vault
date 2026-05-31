from __future__ import annotations

from pathlib import Path


def test_alert_rules_file_contains_expected_alerts():
    text = Path('services/prometheus_alerts_binance_dust_cleanup_admin_notify_v1.yml').read_text(encoding='utf-8')
    assert 'BinanceDustAdminOldDenylistEntry' in text
    assert 'BinanceDustAdminCooldownLoop' in text
    assert 'BinanceDustAdminReminderFailures' in text
