"""P4.6: Verify that the Alertmanager template contains risk deep-links."""
from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_alertmanager_template_has_risk_canary_link() -> None:
    """Template must include a Risk Canary deep-link for risk incidents."""
    src = (
        _root() / 'monitoring' / 'alertmanager' / 'templates' / 'trade_execution.tmpl'
    ).read_text(encoding='utf-8')
    assert 'Risk Canary:' in src, "trade_execution.tmpl missing 'Risk Canary:' deep-link"


def test_alertmanager_template_has_risk_summary_link() -> None:
    """Template must include a Risk Summary deep-link for risk incidents."""
    src = (
        _root() / 'monitoring' / 'alertmanager' / 'templates' / 'trade_execution.tmpl'
    ).read_text(encoding='utf-8')
    assert 'Risk Summary:' in src, "trade_execution.tmpl missing 'Risk Summary:' deep-link"


def test_alertmanager_template_has_silence_link() -> None:
    """Template must include a Silence deep-link using ExternalURL."""
    src = (
        _root() / 'monitoring' / 'alertmanager' / 'templates' / 'trade_execution.tmpl'
    ).read_text(encoding='utf-8')
    assert 'Silence:' in src, "trade_execution.tmpl missing 'Silence:' link"
