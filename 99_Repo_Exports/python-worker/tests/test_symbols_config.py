import pytest

from core.symbols_config import SymbolsConfig


def test_valid_config(tmp_path):
    config_content = """
universe:
  - BTCUSDT
  - ETHUSDT
shards:
  orderflow_1:
    - BTCUSDT
  orderflow_2:
    - ETHUSDT
execution:
  binance_allowlist:
    - BTCUSDT
    - ETHUSDT
metrics:
  canary_symbols:
    - BTCUSDT
"""
    file_path = tmp_path / "symbols.yml"
    file_path.write_text(config_content)

    config = SymbolsConfig(config_path=str(file_path))
    config.load()

    assert config.universe == ["BTCUSDT", "ETHUSDT"]
    assert config.shards["orderflow_1"] == ["BTCUSDT"]
    assert config.shards["orderflow_2"] == ["ETHUSDT"]
    assert config.binance_allowlist == ["BTCUSDT", "ETHUSDT"]
    assert config.canary_symbols == ["BTCUSDT"]

def test_invalid_shards_overlap(tmp_path):
    config_content = """
universe:
  - BTCUSDT
shards:
  orderflow_1:
    - BTCUSDT
  orderflow_2:
    - BTCUSDT
"""
    file_path = tmp_path / "symbols.yml"
    file_path.write_text(config_content)

    config = SymbolsConfig(config_path=str(file_path))
    with pytest.raises(ValueError, match="is duplicated in shards"):
        config.load()

def test_get_symbols_for_shard(tmp_path, monkeypatch):
    config_content = """
universe:
  - BTCUSDT
  - ETHUSDT
shards:
  orderflow_1:
    - BTCUSDT
"""
    file_path = tmp_path / "symbols.yml"
    file_path.write_text(config_content)

    # Need to patch the class's instantiation in get_symbols_for_shard to point to our temp file
    class MockSymbolsConfig(SymbolsConfig):
        def __init__(self, config_path=str(file_path)):
            super().__init__(config_path)

    monkeypatch.setattr("core.symbols_config.SymbolsConfig", MockSymbolsConfig)

    # Test priority 1: SYMBOLS
    monkeypatch.setenv("SYMBOLS", "SOLUSDT")
    assert MockSymbolsConfig.get_symbols_for_shard("CRYPTO_SYMBOLS_SHARD_1") == ["SOLUSDT"]
    monkeypatch.delenv("SYMBOLS")

    # Test priority 2: CRYPTO_SYMBOLS_OVERRIDE
    monkeypatch.setenv("CRYPTO_SYMBOLS_OVERRIDE", "ETHUSDT")
    assert MockSymbolsConfig.get_symbols_for_shard("CRYPTO_SYMBOLS_SHARD_1") == ["ETHUSDT"]
    monkeypatch.delenv("CRYPTO_SYMBOLS_OVERRIDE")

    # Test priority 3: YAML config
    assert MockSymbolsConfig.get_symbols_for_shard("CRYPTO_SYMBOLS_SHARD_1") == ["BTCUSDT"]
