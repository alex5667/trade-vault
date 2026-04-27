from pathlib import Path


def test_p10_assets_present():
    exporter = Path('services/active_symbol_guard_exporter.py').read_text()
    cli = Path('services/active_symbol_guard_cli.py').read_text()
    alerts = Path('services/prometheus_alerts_active_symbol_guard_p10.yml').read_text()
    assert '/api/active-symbol-guard/heatmap' in exporter
    assert '/api/active-symbol-guard/incident/symbol/' in exporter
    assert '--heatmap' in cli
    assert '--incident-symbol' in cli
    assert 'execution_active_symbol_guard_window_hot_symbols' in alerts
    assert 'ActiveSymbolGuardRaceChainsPresent' in alerts
