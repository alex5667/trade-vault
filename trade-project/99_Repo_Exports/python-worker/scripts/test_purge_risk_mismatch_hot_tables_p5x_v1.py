"""P5X tests for purge_risk_mismatch_hot_tables.render_textfile (retention observability)."""
import importlib.util
import sys
from pathlib import Path

# Load the script module directly without executing its __main__ block
p = Path(__file__).resolve().parent / 'purge_risk_mismatch_hot_tables.py'
spec = importlib.util.spec_from_file_location('purge_risk_mismatch_hot_tables', p)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_retention_textfile_contains_freshness():
    """Textfile must include all three retention metric names."""
    txt = mod.render_textfile({'generated_at_ms': 0, 'freshness_stale_threshold_sec': 10, 'purged_quarantine': 3})
    assert 'trade_risk_mismatch_retention_freshness_seconds' in txt
    assert 'trade_risk_mismatch_retention_stale' in txt
    assert 'trade_risk_mismatch_retention_last_purged_quarantine' in txt


def test_retention_textfile_purged_count():
    """Purged count must be correctly reflected in metric output."""
    txt = mod.render_textfile({'generated_at_ms': 0, 'freshness_stale_threshold_sec': 10, 'purged_quarantine': 7})
    assert 'trade_risk_mismatch_retention_last_purged_quarantine 7' in txt


def test_retention_textfile_stale_when_old():
    """Report with generated_at_ms=0 (epoch) must be flagged as stale."""
    txt = mod.render_textfile({'generated_at_ms': 0, 'freshness_stale_threshold_sec': 10, 'purged_quarantine': 0})
    assert 'trade_risk_mismatch_retention_stale 1' in txt
