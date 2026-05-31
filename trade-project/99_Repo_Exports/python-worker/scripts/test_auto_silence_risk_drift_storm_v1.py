"""P5X tests for auto_silence_risk_drift_storm.decide_autosilence logic."""
import importlib.util
import sys
from pathlib import Path

# Load the script module directly without executing its __main__ block
p = Path(__file__).resolve().parent / 'auto_silence_risk_drift_storm.py'
spec = importlib.util.spec_from_file_location('auto_silence_risk_drift_storm', p)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_decide_autosilence_triggers_on_rate():
    """High avg_mismatch_rate in 24h window should trigger silence."""
    summary = {'rows': [{'window_name': '24h', 'quarantine_count': 2, 'avg_mismatch_rate': 0.25}]}
    dec = mod.decide_autosilence(summary, quarantine_threshold=10, mismatch_rate_threshold=0.2)
    assert dec['should_silence'] is True
    assert dec['reason'] == 'storm'


def test_decide_autosilence_below_threshold():
    """Low quarantine count and low rate should not trigger silence."""
    summary = {'rows': [{'window_name': '24h', 'quarantine_count': 1, 'avg_mismatch_rate': 0.01}]}
    dec = mod.decide_autosilence(summary, quarantine_threshold=10, mismatch_rate_threshold=0.2)
    assert dec['should_silence'] is False
    assert dec['reason'] == 'below_threshold'


def test_decide_autosilence_triggers_on_quarantine_count():
    """High quarantine count alone should trigger silence even if rate is low."""
    summary = {'rows': [{'window_name': '24h', 'quarantine_count': 15, 'avg_mismatch_rate': 0.01}]}
    dec = mod.decide_autosilence(summary, quarantine_threshold=10, mismatch_rate_threshold=0.2)
    assert dec['should_silence'] is True


def test_decide_autosilence_ignores_non_24h_rows():
    """Only 24h window rows should affect the decision; 7d rows must be ignored."""
    summary = {'rows': [{'window_name': '7d', 'quarantine_count': 100, 'avg_mismatch_rate': 0.99}]}
    dec = mod.decide_autosilence(summary, quarantine_threshold=10, mismatch_rate_threshold=0.2)
    # 7d rows must not be evaluated
    assert dec['should_silence'] is False


def test_decide_autosilence_empty_summary():
    """Empty summary must not raise and should return should_silence=False."""
    dec = mod.decide_autosilence({}, quarantine_threshold=10, mismatch_rate_threshold=0.2)
    assert dec['should_silence'] is False
