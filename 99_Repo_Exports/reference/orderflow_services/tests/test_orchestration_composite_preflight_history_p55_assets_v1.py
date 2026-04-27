from __future__ import annotations

from pathlib import Path


def test_p55_assets_exist() -> None:
    """Verify all P5.5 file assets exist on disk relative to the python-worker working directory."""
    root = Path('orderflow_services')
    assert (root / 'prometheus_alerts_orchestration_composite_preflight_history_p55.yml').exists()
    assert (root / 'grafana' / 'orchestration_composite_preflight_history_p55.json').exists()
    assert (root / 'orchestration_composite_preflight_history_exporter_v1.py').exists()


def test_p55_timer_integration_present() -> None:
    """Verify the timers worker contains the P5.5 job function and env guard."""
    body = Path('services/of_timers_worker.py').read_text(encoding='utf-8')
    assert 'run_orchestration_composite_preflight_history_exporter' in body
    assert 'ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORTER' in body
