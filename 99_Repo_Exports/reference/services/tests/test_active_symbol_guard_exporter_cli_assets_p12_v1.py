"""P12 Runbook CLI/exporter/executor asset smoke-tests.

These tests verify structural invariants (function/method names, CLI flags,
HTTP endpoint strings) without importing the full service dependencies.
4 tests total — all must pass in isolation.
"""
from pathlib import Path


def test_cli_contains_runbook_flags():
    """All P12 CLI flags must be present in active_symbol_guard_cli.py."""
    src = Path(__file__).resolve().parents[2] / 'services' / 'active_symbol_guard_cli.py'
    text = src.read_text(encoding='utf-8')
    assert '--apply-hold-symbol' in text
    assert '--revoke-hold-symbol' in text
    assert '--force-release-symbol' in text
    assert '--ack-symbol' in text
    assert '--renew-symbol' in text


def test_exporter_contains_action_endpoints():
    """All P12 POST action endpoints must be present in active_symbol_guard_exporter.py."""
    src = Path(__file__).resolve().parents[2] / 'services' / 'active_symbol_guard_exporter.py'
    text = src.read_text(encoding='utf-8')
    assert '/api/active-symbol-guard/actions/hold/apply' in text
    assert '/api/active-symbol-guard/actions/hold/revoke' in text
    assert '/api/active-symbol-guard/actions/force-release' in text
    assert '/api/active-symbol-guard/actions/escalation/ack' in text
    assert '/api/active-symbol-guard/actions/escalation/renew' in text


def test_binance_executor_checks_manual_hold_before_open():
    """binance_executor.py must have both the method and the call site for manual hold guard."""
    src = Path(__file__).resolve().parents[2] / 'services' / 'binance_executor.py'
    text = src.read_text(encoding='utf-8')
    assert 'def _guard_symbol_not_manually_held' in text
    assert "self._guard_symbol_not_manually_held(symbol=symbol, action='open')" in text


def test_exporter_contains_runbook_get_endpoints():
    """GET runbook endpoints must be present in active_symbol_guard_exporter.py."""
    src = Path(__file__).resolve().parents[2] / 'services' / 'active_symbol_guard_exporter.py'
    text = src.read_text(encoding='utf-8')
    assert '/api/active-symbol-guard/runbook/symbol/' in text
    assert '/api/active-symbol-guard/runbook/sid/' in text
