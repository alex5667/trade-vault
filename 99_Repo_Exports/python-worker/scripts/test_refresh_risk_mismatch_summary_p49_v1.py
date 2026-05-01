from pathlib import Path
import importlib.util
import sys

# Load refresh_risk_mismatch_summary module dynamically to avoid circular imports
path = Path(__file__).resolve().parents[0] / 'refresh_risk_mismatch_summary.py'
spec = importlib.util.spec_from_file_location('refresh_risk_mismatch_summary_p49', path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_render_textfile_contains_freshness_and_rate():
    """P4.9: render_textfile should produce valid Prometheus textfile with expected metrics."""
    text = mod.render_textfile({
        'generated_at_ms': 1,
        'freshness_stale_threshold_sec': 1800,
        'row_count': 1,
        'rows': [{'window_name': '24h', 'tier': 'A', 'avg_mismatch_rate': 0.12, 'quarantine_count': 4}],
    })
    assert 'trade_risk_mismatch_summary_freshness_seconds' in text
    assert 'trade_risk_mismatch_summary_avg_rate{window_name="24h",tier="A"} 0.12' in text
    assert 'trade_risk_mismatch_summary_quarantine_count{window_name="24h",tier="A"} 4' in text
    assert 'trade_risk_mismatch_summary_stale' in text


def test_render_textfile_stale_flag():
    """P4.9: stale flag should be 1 when generated_at_ms is very old."""
    text = mod.render_textfile({
        'generated_at_ms': 1,  # epoch 1ms — definitely stale
        'freshness_stale_threshold_sec': 1800,
        'row_count': 0,
        'rows': [],
    })
    assert 'trade_risk_mismatch_summary_stale 1' in text
