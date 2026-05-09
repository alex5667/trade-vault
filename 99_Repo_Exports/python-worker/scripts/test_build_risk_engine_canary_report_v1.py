"""Tests for build_risk_engine_canary_report.py — canary score and bucketing logic (P4.5)."""
import importlib.util
import sys
from pathlib import Path

# Standalone load so tests work without full package context
mod_path = Path(__file__).resolve().parent / 'build_risk_engine_canary_report.py'
spec = importlib.util.spec_from_file_location('build_risk_engine_canary_report', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_bucket_green():
    """Score >= 90 is 'green'."""
    assert mod._bucket(95) == 'green'
    assert mod._bucket(90) == 'green'


def test_bucket_yellow():
    """Score in [75, 90) is 'yellow'."""
    assert mod._bucket(80) == 'yellow'
    assert mod._bucket(75) == 'yellow'


def test_bucket_red():
    """Score < 75 is 'red'."""
    assert mod._bucket(50) == 'red'
    assert mod._bucket(0) == 'red'
    assert mod._bucket(74.9) == 'red'


def test_bucket_boundary():
    """Boundary values are correctly assigned."""
    assert mod._bucket(89.9) == 'yellow'
    assert mod._bucket(90.0) == 'green'
    assert mod._bucket(74.99) == 'red'
    assert mod._bucket(75.0) == 'yellow'
