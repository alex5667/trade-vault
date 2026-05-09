from __future__ import annotations

import pytest


def test_resolve_risk_cfg_uses_risk_cfg_resolver(monkeypatch):
    """
    Проверяем что _resolve_risk_cfg_for_levels использует RiskCfgResolver.resolve()
    и корректно возвращает конфигурацию с symbol-specific overrides.
    """
    # Setup ENV для BTC и defaults
    monkeypatch.setenv("STOP_MODE", "ATR")
    monkeypatch.setenv("STOP_ATR_MULT", "0.6")
    monkeypatch.setenv("TP_MODE", "RR")
    monkeypatch.setenv("TP_RR", "1,2,3")

    # BTC-specific overrides
    monkeypatch.setenv("BTC_STOP_ATR_MULT", "0.8")
    monkeypatch.setenv("BTC_TP_RR", "1,1.5,2.5")

    from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler

    # Mock minimal handler
    class MockHandler:
        symbol = "BTCUSDT"
        _risk_cfg_resolver = None

    h = MockHandler()
    h._resolve_risk_cfg_for_levels = CryptoOrderFlowHandler._resolve_risk_cfg_for_levels.__get__(h, MockHandler)

    # Call _resolve_risk_cfg_for_levels - should use RiskCfgResolver
    cfg = h._resolve_risk_cfg_for_levels()

    # Verify BTC overrides were applied
    assert cfg["STOP_MODE"] == "ATR"
    assert float(cfg["STOP_ATR_MULT"]) == pytest.approx(0.8, rel=1e-9)  # BTC override
    assert cfg["TP_MODE"] == "RR"
    assert cfg["TP_RR"] == "1,1.5,2.5"  # BTC override

    # Verify resolver was cached
    assert h._risk_cfg_resolver is not None


def test_resolve_risk_cfg_caches_resolver(monkeypatch):
    """
    Проверяем что _resolve_risk_cfg_for_levels кэширует RiskCfgResolver
    и не создает его каждый раз.
    """
    monkeypatch.setenv("STOP_MODE", "ATR")
    monkeypatch.setenv("TP_MODE", "RR")

    from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler

    class MockHandler:
        symbol = "ETHUSDT"
        _risk_cfg_resolver = None

    h = MockHandler()
    h._resolve_risk_cfg_for_levels = CryptoOrderFlowHandler._resolve_risk_cfg_for_levels.__get__(h, MockHandler)

    # First call - creates resolver
    cfg1 = h._resolve_risk_cfg_for_levels()
    resolver1 = h._risk_cfg_resolver

    # Second call - reuses cached resolver
    cfg2 = h._resolve_risk_cfg_for_levels()
    resolver2 = h._risk_cfg_resolver

    assert resolver1 is resolver2, "Resolver should be cached"
    assert cfg1 == cfg2, "Config should be consistent"


def test_resolve_risk_cfg_eth_overrides(monkeypatch):
    """
    Проверяем что ETH-specific overrides корректно применяются.
    """
    # Defaults
    monkeypatch.setenv("STOP_MODE", "ATR")
    monkeypatch.setenv("STOP_ATR_MULT", "0.6")
    monkeypatch.setenv("TP_RR", "1,2,3")

    # ETH-specific
    monkeypatch.setenv("ETH_STOP_ATR_MULT", "0.8")
    monkeypatch.setenv("ETH_TP_RR", "1,1.5,2.5")

    from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler

    class MockHandler:
        symbol = "ETHUSDT"
        _risk_cfg_resolver = None

    h = MockHandler()
    h._resolve_risk_cfg_for_levels = CryptoOrderFlowHandler._resolve_risk_cfg_for_levels.__get__(h, MockHandler)

    cfg = h._resolve_risk_cfg_for_levels()

    # ETH overrides should be applied
    assert float(cfg["STOP_ATR_MULT"]) == pytest.approx(0.8, rel=1e-9)
    assert cfg["TP_RR"] == "1,1.5,2.5"


def test_resolve_risk_cfg_defaults_for_unknown_symbol(monkeypatch):
    """
    Проверяем что для символов без специфичных overrides используются defaults.
    """
    monkeypatch.setenv("STOP_MODE", "ATR")
    monkeypatch.setenv("STOP_ATR_MULT", "0.6")
    monkeypatch.setenv("TP_MODE", "RR")
    monkeypatch.setenv("TP_RR", "1,2,3")

    from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler

    class MockHandler:
        symbol = "XRPUSDT"  # No specific overrides
        _risk_cfg_resolver = None

    h = MockHandler()
    h._resolve_risk_cfg_for_levels = CryptoOrderFlowHandler._resolve_risk_cfg_for_levels.__get__(h, MockHandler)

    cfg = h._resolve_risk_cfg_for_levels()

    # Should use defaults
    assert cfg["STOP_MODE"] == "ATR"
    assert float(cfg["STOP_ATR_MULT"]) == pytest.approx(0.6, rel=1e-9)
    assert cfg["TP_MODE"] == "RR"
    assert cfg["TP_RR"] == "1,2,3"

