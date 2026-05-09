"""Tests for P3.3 autonomy: auto-trigger decision logic.

Validates the should_trigger() function in auto_trigger_checkpoint_scrubber.py
against various health report states without requiring external services.
"""
import importlib.util
from pathlib import Path

# Load module directly from the scripts directory to avoid import path issues
_mod_path = Path(__file__).resolve().parent.parent / 'scripts' / 'auto_trigger_checkpoint_scrubber.py'
_spec = importlib.util.spec_from_file_location('auto_trigger_checkpoint_scrubber', _mod_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

should_trigger = _mod.should_trigger


def test_no_trigger_on_ok_status():
    """Healthy report → no trigger."""
    dec = should_trigger({'overall_status': 'ok', 'consistency': {'critical_mismatches': 0}, 'retention_guard': {'breached_checkpoints': 0}})
    assert dec['trigger'] is False
    assert dec['reasons'] == []


def test_trigger_on_critical_status():
    """Critical status → trigger regardless of other fields."""
    dec = should_trigger({'overall_status': 'critical', 'consistency': {'critical_mismatches': 0}, 'retention_guard': {'breached_checkpoints': 0}})
    assert dec['trigger'] is True
    assert 'overall_critical' in dec['reasons']


def test_trigger_on_warning_when_enabled():
    """Warning status + trigger_on_warning=True → trigger."""
    dec = should_trigger({'overall_status': 'warning', 'consistency': {}, 'retention_guard': {}}, trigger_on_warning=True)
    assert dec['trigger'] is True
    assert 'overall_warning' in dec['reasons']


def test_no_trigger_on_warning_when_disabled():
    """Warning status + trigger_on_warning=False → no trigger (from warning alone)."""
    dec = should_trigger({'overall_status': 'warning', 'consistency': {}, 'retention_guard': {'breached_checkpoints': 0}}, trigger_on_warning=False)
    assert dec['trigger'] is False


def test_should_trigger_on_retention_guard():
    """Retention-guard breach alone triggers the scrubber."""
    dec = should_trigger({
        'overall_status': 'ok',
        'consistency': {'critical_mismatches': 0},
        'retention_guard': {'breached_checkpoints': 2},
    })
    assert dec['trigger'] is True
    assert 'retention_guard_breached' in dec['reasons']


def test_should_trigger_on_critical_mismatches():
    """Critical mismatches alone triggers the scrubber."""
    dec = should_trigger({
        'overall_status': 'ok',
        'consistency': {'critical_mismatches': 3},
        'retention_guard': {'breached_checkpoints': 0},
    })
    assert dec['trigger'] is True
    assert 'critical_mismatches' in dec['reasons']


def test_multiple_reasons():
    """Multiple trigger conditions → all reasons reported."""
    dec = should_trigger({
        'overall_status': 'critical',
        'consistency': {'critical_mismatches': 1},
        'retention_guard': {'breached_checkpoints': 5},
    })
    assert dec['trigger'] is True
    assert 'overall_critical' in dec['reasons']
    assert 'retention_guard_breached' in dec['reasons']
    assert 'critical_mismatches' in dec['reasons']
