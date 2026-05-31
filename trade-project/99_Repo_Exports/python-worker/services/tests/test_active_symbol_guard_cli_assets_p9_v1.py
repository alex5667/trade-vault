from pathlib import Path


def test_p9_assets_present():
    assert Path('services/active_symbol_guard_exporter.py').exists()
    assert Path('services/active_symbol_guard_cli.py').exists()
    text = Path('services/prometheus_alerts_active_symbol_guard_p9.yml').read_text()
    assert 'execution_active_symbol_guard_snapshot_total' in text
    assert 'ActiveSymbolGuardStaleTombstonesPresent' in text
