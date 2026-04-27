"""P5X tests for check_risk_mismatch_summary_archive_consistency.render_textfile."""
import importlib.util
import sys
from pathlib import Path

# Load the script module directly without executing its __main__ block
p = Path(__file__).resolve().parent / 'check_risk_mismatch_summary_archive_consistency.py'
spec = importlib.util.spec_from_file_location('check_risk_mismatch_summary_archive_consistency', p)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_render_textfile_contains_metrics():
    """Rendered textfile must include all three expected metric names."""
    txt = mod.render_textfile({'generated_at_ms': 0, 'freshness_stale_threshold_sec': 10, 'mismatch_count': 2})
    assert 'trade_risk_mismatch_archive_consistency_mismatch_total' in txt
    assert 'trade_risk_mismatch_archive_consistency_freshness_seconds' in txt
    assert 'trade_risk_mismatch_archive_consistency_stale' in txt


def test_render_textfile_mismatch_count():
    """Mismatch count must be correctly reflected in metric output."""
    txt = mod.render_textfile({'generated_at_ms': 0, 'freshness_stale_threshold_sec': 10, 'mismatch_count': 5})
    assert 'trade_risk_mismatch_archive_consistency_mismatch_total 5' in txt


def test_render_textfile_stale_when_old():
    """Report with generated_at_ms=0 (epoch) must be flagged as stale."""
    txt = mod.render_textfile({'generated_at_ms': 0, 'freshness_stale_threshold_sec': 10, 'mismatch_count': 0})
    # freshness from epoch will always be very large, stale=1
    assert 'trade_risk_mismatch_archive_consistency_stale 1' in txt
