from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_admin_cli_contains_required_commands():
    text = (ROOT / 'binance_dust_cleanup_admin_cli.py').read_text(encoding='utf-8')
    assert '--add-denylist-symbol' in text
    assert '--remove-denylist-symbol' in text
    assert '--clear-cooldown-symbol' in text
    assert '--show-state' in text
    assert '--show-audit' in text


def test_admin_http_contains_required_routes():
    text = (ROOT / 'binance_dust_cleanup_admin_http.py').read_text(encoding='utf-8')
    assert '/api/binance-dust/state' in text
    assert '/api/binance-dust/denylist/add' in text
    assert '/api/binance-dust/denylist/remove' in text
    assert '/api/binance-dust/cooldown/clear' in text
    assert '/api/binance-dust/audit' in text
