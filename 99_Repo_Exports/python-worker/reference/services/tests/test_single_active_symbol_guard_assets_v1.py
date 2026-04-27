from pathlib import Path


def test_env_example_contains_single_active_symbol_guard_flags():
    text = Path('deploy/execution_safe_defaults_p104.env.example').read_text()
    assert 'EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL=1' in text
    assert 'EXEC_SINGLE_ACTIVE_POSITION_RELEASE_ON_TERMINAL=1' in text
    assert 'EXEC_SINGLE_ACTIVE_POSITION_STALE_TIMEOUT_MS=900000' in text
    assert 'ORDERS_ACTIVE_SYMBOL_KEY_PREFIX=orders:active_symbol_sid:' in text


def test_compose_contains_single_active_symbol_guard_flags_for_executor():
    text = Path('confidence_calculation/docker-compose-crypto-orderflow.yml').read_text()
    assert 'EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL=1' in text
    assert 'EXEC_SINGLE_ACTIVE_POSITION_RELEASE_ON_TERMINAL=1' in text
    assert 'EXEC_SINGLE_ACTIVE_POSITION_STALE_TIMEOUT_MS=900000' in text
    assert 'ORDERS_ACTIVE_SYMBOL_KEY_PREFIX=orders:active_symbol_sid:' in text
