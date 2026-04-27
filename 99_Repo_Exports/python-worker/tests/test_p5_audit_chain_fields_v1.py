"""Tests for P5 audit chain fields in crypto_orderflow_service.

Verifies that ensure_audit_chain_fields() correctly materializes
signal_id, execution_plan_id, decision_id, and audit_chain_ver
before the signal leaves the publish boundary.
"""
from pathlib import Path


def _load_func():
    """Import ensure_audit_chain_fields from crypto_orderflow_service via text inspection."""
    src_path = Path(__file__).resolve().parent.parent / 'services' / 'crypto_orderflow_service.py'
    src = src_path.read_text(encoding='utf-8')
    return src


def test_ensure_audit_chain_fields_present_in_source():
    """The function must exist in the source file."""
    src = _load_func()
    assert 'def ensure_audit_chain_fields' in src
    assert "signal['signal_id']" in src or 'signal["signal_id"]' in src
    assert "signal['execution_plan_id']" in src or 'signal["execution_plan_id"]' in src


def test_ensure_audit_chain_fields_called_in_pre_publish():
    """ensure_audit_chain_fields must be called inside _pre_publish_allows_signal."""
    src = _load_func()
    assert 'ensure_audit_chain_fields(signal)' in src


def test_ensure_audit_chain_fields_logic():
    """Standalone test of the function logic (imports it directly from the module)."""
    import sys
    import importlib.util
    sp = Path(__file__).resolve().parent.parent / 'services' / 'crypto_orderflow_service.py'

    # We only compile the module — never execute top-level code that imports redis etc.
    # Instead extract the function body via exec() in a minimal namespace.
    src = sp.read_text(encoding='utf-8')
    # Find the function definition and compile it in a minimal namespace
    start = src.find('def ensure_audit_chain_fields(')
    assert start != -1, "Function not found"
    # Take a safe prefix to capture only the function + its body
    end = src.find('\n\n\n', start)
    func_src = src[start:end] if end != -1 else src[start:start + 2000]
    ns = {'Dict': dict, 'Any': object}
    try:
        exec(func_src, ns)
    except SyntaxError as e:
        raise AssertionError(f"Function has syntax error: {e}")

    ensure_audit_chain_fields = ns['ensure_audit_chain_fields']

    # Test: decision_id provided → signal_id and execution_plan_id derived
    sig = {'decision_id': 'dec-abc', 'symbol': 'BTCUSDT'}
    out = ensure_audit_chain_fields(sig)
    assert out.get('signal_id') == 'dec-abc'
    assert out.get('execution_plan_id') == 'dec-abc'
    assert out.get('decision_id') == 'dec-abc'
    assert out.get('audit_chain_ver') == 'p5_execution_audit_v1'

    # Test: explicit signal_id wins over decision_id
    sig2 = {'signal_id': 'sig-explicit', 'decision_id': 'dec-fallback'}
    out2 = ensure_audit_chain_fields(sig2)
    assert out2['signal_id'] == 'sig-explicit'
    assert out2['decision_id'] == 'dec-fallback'

    # Test: non-dict input returned unchanged
    result = ensure_audit_chain_fields(None)
    assert result is None
