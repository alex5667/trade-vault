"""Tests for latency_contract_rollout_preflight_v1 (P4.2)."""
import os


def test_wrapper_script_exists_and_references_preflight():
    """The shell wrapper must exist and contain both the preflight call and exec."""
    path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), '..', 'integrations',
            'run_with_latency_contract_rollout_preflight_v1.sh',
        )
    )
    with open(path, 'r', encoding='utf-8') as f:
        text = f.read()
    assert 'latency_contract_rollout_preflight_v1' in text
    assert 'exec "$@"' in text


def test_preflight_module_importable():
    """The preflight module must import cleanly (no syntax/import errors)."""
    import orderflow_services.latency_contract_rollout_preflight_v1 as mod  # noqa: F401
    assert hasattr(mod, 'main')
