from pathlib import Path


def test_alertmanager_template_contains_deeplinks():
    src = (
        Path(__file__).resolve().parents[2]
        / 'monitoring'
        / 'alertmanager'
        / 'templates'
        / 'trade_execution.tmpl'
    ).read_text(encoding='utf-8')
    assert 'silence' in src.lower()
    assert 'runbook' in src.lower()
