"""P4.8 test: verify alertmanager template contains Risk Drift and Unified Ops Dashboard links."""
from pathlib import Path


def test_alertmanager_contains_risk_drift_links():
    """trade_execution.tmpl must include P4.8 deep-links for risk drift and unified dashboard."""
    src = (
        Path(__file__).resolve().parents[2]
        / 'monitoring'
        / 'alertmanager'
        / 'templates'
        / 'trade_execution.tmpl'
    ).read_text(encoding='utf-8')
    assert 'Risk Drift:' in src, 'Risk Drift deep-link must be present in alertmanager template'
    assert 'Unified Ops Dashboard:' in src, 'Unified Ops Dashboard deep-link must be present'


def test_alertmanager_risk_drift_points_to_api():
    """Risk Drift link must point to /api/risk-mismatch/latest."""
    src = (
        Path(__file__).resolve().parents[2]
        / 'monitoring'
        / 'alertmanager'
        / 'templates'
        / 'trade_execution.tmpl'
    ).read_text(encoding='utf-8')
    assert '/api/risk-mismatch/latest' in src, \
        'Risk Drift must link to /api/risk-mismatch/latest'
