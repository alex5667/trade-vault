from handlers.crypto_orderflow.core.crypto_orderflow_confirmations import L2ConfirmCfg


def test_from_env_prefers_obi_top_n(monkeypatch):
    monkeypatch.setenv("OBI_TOP_N", "7")
    monkeypatch.delenv("L2_TOP_N", raising=False)
    cfg = L2ConfirmCfg.from_env(symbol="BTC")
    assert cfg.top_n == 7


def test_from_env_symbol_override(monkeypatch):
    monkeypatch.setenv("OBI_TOP_N", "7")
    monkeypatch.setenv("BTC_OBI_TOP_N", "3")
    cfg = L2ConfirmCfg.from_env(symbol="BTC")
    assert cfg.top_n == 3


def test_from_env_fallback_l2_top_n(monkeypatch):
    monkeypatch.delenv("OBI_TOP_N", raising=False)
    monkeypatch.setenv("L2_TOP_N", "9")
    cfg = L2ConfirmCfg.from_env(symbol="BTC")
    assert cfg.top_n == 9


def test_from_env_bounds(monkeypatch):
    monkeypatch.setenv("OBI_TOP_N", "999")
    cfg = L2ConfirmCfg.from_env(symbol="BTC")
    assert 1 <= cfg.top_n <= 50
