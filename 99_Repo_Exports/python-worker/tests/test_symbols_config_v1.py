"""test_symbols_config_v1.py — guard the single-source-of-truth symbol helper."""
from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    """Strip every symbol env so each test starts from a clean slate."""
    for k in (
        "CRYPTO_SYMBOLS",
        "QDP_SYMBOLS",
        "CDP_SYMBOLS",
        "RTP_SYMBOLS",
        "CVP_SYMBOLS",
    ):
        monkeypatch.delenv(k, raising=False)
    # Also reset the module-level warn-once set so multiple tests can
    # exercise the deprecation path independently.
    from core import symbols_config_v1 as mod
    mod._warned_aliases.clear()
    yield


def test_canonical_env_wins():
    from core.symbols_config_v1 import get_crypto_symbols
    os.environ["CRYPTO_SYMBOLS"] = "BTCUSDT,ETHUSDT"
    os.environ["QDP_SYMBOLS"] = "FOOUSDT"
    assert get_crypto_symbols(aliases=("QDP_SYMBOLS",)) == ["BTCUSDT", "ETHUSDT"]


def test_alias_used_when_canonical_missing():
    from core.symbols_config_v1 import get_crypto_symbols
    os.environ["CDP_SYMBOLS"] = "btcusdt, ETHUSDT"
    out = get_crypto_symbols(aliases=("CDP_SYMBOLS",))
    assert out == ["BTCUSDT", "ETHUSDT"]


def test_alias_priority_order():
    from core.symbols_config_v1 import get_crypto_symbols
    os.environ["RTP_SYMBOLS"] = "ABCUSDT"
    os.environ["CVP_SYMBOLS"] = "ZYXUSDT"
    out = get_crypto_symbols(aliases=("RTP_SYMBOLS", "CVP_SYMBOLS"))
    assert out == ["ABCUSDT"]


def test_default_fallback():
    from core.symbols_config_v1 import get_crypto_symbols
    out = get_crypto_symbols()
    assert out == ["BTCUSDT", "ETHUSDT", "SOLUSDT", "1000PEPEUSDT"]


def test_explicit_default():
    from core.symbols_config_v1 import get_crypto_symbols
    out = get_crypto_symbols(default="DOGEUSDT")
    assert out == ["DOGEUSDT"]


def test_empty_canonical_falls_through():
    from core.symbols_config_v1 import get_crypto_symbols
    os.environ["CRYPTO_SYMBOLS"] = "   "
    os.environ["QDP_SYMBOLS"] = "DOGEUSDT"
    out = get_crypto_symbols(aliases=("QDP_SYMBOLS",))
    assert out == ["DOGEUSDT"]


def test_alias_warns_only_once(caplog):
    import logging
    from core.symbols_config_v1 import get_crypto_symbols
    os.environ["QDP_SYMBOLS"] = "BTCUSDT"
    caplog.set_level(logging.WARNING, logger="symbols_config_v1")
    get_crypto_symbols(aliases=("QDP_SYMBOLS",))
    get_crypto_symbols(aliases=("QDP_SYMBOLS",))
    warns = [r for r in caplog.records if "deprecated" in r.message]
    assert len(warns) == 1


def test_all_4_producers_use_helper():
    """Schema-only check: every P1 shadow producer must source SYMBOLS via
    get_crypto_symbols, not via a hand-rolled os.getenv split."""
    import pathlib
    services_dir = pathlib.Path(__file__).parent.parent / "services"
    for fname in (
        "queue_dynamics_producer.py",
        "cost_dynamics_producer.py",
        "regime_transition_producer.py",
        "cross_venue_health_producer.py",
    ):
        src = (services_dir / fname).read_text(encoding="utf-8")
        assert "get_crypto_symbols" in src, (
            f"{fname} should use core.symbols_config_v1.get_crypto_symbols"
        )
        # The old pattern must be gone — guards against accidental revert.
        assert 'os.getenv(\n    "QDP_SYMBOLS"' not in src
        assert 'os.getenv(\n    "CDP_SYMBOLS"' not in src
        assert 'os.getenv(\n    "RTP_SYMBOLS"' not in src
        assert 'os.getenv(\n    "CVP_SYMBOLS"' not in src
