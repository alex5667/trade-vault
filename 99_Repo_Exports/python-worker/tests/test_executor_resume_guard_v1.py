from pathlib import Path


def test_executor_resume_guard_contract():
    src = (Path(__file__).resolve().parents[1] / 'services' / 'binance_executor.py').read_text(encoding='utf-8')
    assert 'RESUME_GUARD_BLOCKED' in src
    assert '_guard_sid_not_quarantined' in src
