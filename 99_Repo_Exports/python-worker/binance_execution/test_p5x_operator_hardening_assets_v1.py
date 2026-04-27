"""P5X test: verify all alertmanager and script assets are present after hardening."""
from pathlib import Path


def test_alertmanager_route_split_present():
    """Alertmanager config must have domain="risk-drift" route and trade-risk-drift receiver."""
    src = (Path(__file__).resolve().parents[2] / 'monitoring' / 'alertmanager' / 'trade_execution_alertmanager.yml').read_text(encoding='utf-8')
    assert 'domain="risk-drift"' in src, 'Missing risk-drift route matcher'
    assert 'trade-risk-drift' in src, 'Missing trade-risk-drift receiver'


def test_new_scripts_exist():
    """Both new P5X scripts must exist in the scripts directory."""
    root = Path(__file__).resolve().parents[1]
    assert (root / 'scripts' / 'auto_silence_risk_drift_storm.py').exists(), \
        'auto_silence_risk_drift_storm.py not found'
    assert (root / 'scripts' / 'check_risk_mismatch_summary_archive_consistency.py').exists(), \
        'check_risk_mismatch_summary_archive_consistency.py not found'
