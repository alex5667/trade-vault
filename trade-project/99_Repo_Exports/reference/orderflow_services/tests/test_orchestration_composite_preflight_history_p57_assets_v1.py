"""Asset-level tests for P5.7 patch: verify required files, env vars and timer wiring exist.

These checks are fast (no Redis, no subprocess) and ensure the patch is fully applied.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]  # scanner_infra root


def test_alert_files_exist() -> None:
    """Both alert YAML files must exist (primary + tick_flow_full mirror)."""
    for rel in [
        'python-worker/orderflow_services/prometheus_alerts_orchestration_composite_preflight_rollup_consistency_p57.yml',
        'tick_flow_full/orderflow_services/prometheus_alerts_orchestration_composite_preflight_rollup_consistency_p57.yml',
    ]:
        assert (ROOT / rel).exists(), f"Missing: {rel}"


def test_consistency_module_exists() -> None:
    """Both consistency module files must exist (primary + tick_flow_full mirror)."""
    for rel in [
        'python-worker/orderflow_services/orchestration_composite_preflight_history_consistency_v1.py',
        'tick_flow_full/orderflow_services/orchestration_composite_preflight_history_consistency_v1.py',
    ]:
        assert (ROOT / rel).exists(), f"Missing: {rel}"


def test_env_examples_include_p57_vars() -> None:
    """Env example files must declare all P5.7 environment variables."""
    for rel in [
        'python-worker/orderflow_services/deploy/env/latency-contract-sensitive-jobs.staging.env.example',
        'python-worker/orderflow_services/deploy/env/latency-contract-sensitive-jobs.prod.env.example',
    ]:
        path = ROOT / rel
        assert path.exists(), f"Missing: {rel}"
        txt = path.read_text(encoding='utf-8')
        assert 'ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_CHECK=' in txt, rel
        assert 'ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_WINDOW_HOURS=' in txt, rel
        assert 'ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_EXPORT_PATH=' in txt, rel


def test_timer_worker_wiring_present() -> None:
    """Timer worker must contain the P5.7 consistency check function name."""
    for rel in [
        'python-worker/services/of_timers_worker.py',
    ]:
        txt = (ROOT / rel).read_text(encoding='utf-8')
        assert 'run_orchestration_composite_preflight_history_consistency_check' in txt, rel
