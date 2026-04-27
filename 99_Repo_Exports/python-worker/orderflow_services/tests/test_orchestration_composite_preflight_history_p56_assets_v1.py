from __future__ import annotations

from pathlib import Path


def test_alert_files_exist() -> None:
    """Verify P5.6 Prometheus alert YAMLs exist on disk (relative to python-worker CWD)."""
    assert Path('orderflow_services/prometheus_alerts_orchestration_composite_preflight_rollup_p56.yml').exists()
    # tick_flow_full mirror lives one level up from python-worker
    tick = Path('../tick_flow_full/orderflow_services/prometheus_alerts_orchestration_composite_preflight_rollup_p56.yml')
    assert tick.exists(), str(tick.resolve())


def test_env_examples_include_p56_vars() -> None:
    """Verify env.example files have the P5.6 incremental rollup variables."""
    for rel in [
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example',
        'orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example',
    ]:
        txt = Path(rel).read_text(encoding='utf-8')
        assert 'ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_ROLLUP=' in txt
        assert 'ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORT_PATH=' in txt
        assert 'ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CURSOR_KEY=' in txt


def test_timer_worker_wiring_present() -> None:
    """Verify of_timers_worker.py wires both P5.6 timer functions."""
    body = Path('services/of_timers_worker.py').read_text(encoding='utf-8')
    assert 'run_orchestration_composite_preflight_history_rollup' in body
    assert 'run_orchestration_composite_preflight_history_textfile_exporter' in body

