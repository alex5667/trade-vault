"""
Unit тесты для core/instrument_config.py
"""

import pytest
import os
from core.instrument_config import (
    SymbolSpecs,
    OrderFlowConfig,
    get_config,
    get_specs,
    register_instrument,
    normalize_symbol,
    INSTRUMENT_CONFIGS,
    INSTRUMENT_SPECS,
)


class TestSymbolSpecs:
    """Тесты для SymbolSpecs"""
    
    def test_xauusd_specs(self):
        """Тест спецификации """
        specs = get_specs("")
        
        assert specs.symbol == ""
        assert specs.contract_size == 100.0
        assert specs.pip_value == 0.01
        assert specs.lot_step == 0.01
        assert specs.min_lot == 0.01
        assert specs.max_lot == 100.0
        assert specs.price_decimals == 2
        assert specs.price_decimals == 2
        assert specs.volume_decimals == 2
        assert specs.delta_z == 3.0
    
    def test_btcusd_specs(self):
        """Тест спецификации BTCUSD"""
        specs = get_specs("BTCUSD")

        assert specs.symbol == "BTCUSD"
        assert specs.contract_size == 1.0
        assert specs.lot_step == 0.001
        assert specs.min_lot == 0.001
        assert specs.price_decimals == 2
        assert specs.volume_decimals == 3

    def test_dogeusdt_specs(self):
        """Тест спецификации DOGEUSDT"""
        specs = get_specs("DOGEUSDT")

        assert specs.symbol == "DOGEUSDT"
        assert specs.contract_size == 1.0
        assert specs.price_decimals == 5
    
    def test_unknown_symbol_raises_error(self):
        """Тест на неизвестный символ"""
        with pytest.raises(ValueError, match="Unknown symbol"):
            get_specs("UNKNOWN")


class TestOrderFlowConfig:
    """Тесты для OrderFlowConfig"""
    
    def test_xauusd_config_from_preset(self):
        """Тест конфигурации  из пресета"""
        config = get_config(use_env=False)
        
        assert config.symbol == ""
        assert config.delta_z_threshold == 3.0
        assert config.weak_progress_atr == 0.10
        assert config.obi_threshold == 0.5
        assert config.min_signal_interval_sec == 60
    
    def test_btcusd_config_from_preset(self):
        """Тест конфигурации BTCUSD из пресета"""
        config = get_config("BTCUSD", use_env=False)

        assert config.symbol == "BTCUSD"
        assert config.delta_z_threshold == 2.7  # Меньше чем 
        assert config.min_signal_interval_sec == 20  # Чаще чем 

    def test_dogeusdt_config_from_preset(self):
        """Тест конфигурации DOGEUSDT из пресета"""
        config = get_config("DOGEUSDT", use_env=False)

        assert config.symbol == "DOGEUSDT"
        assert config.delta_z_threshold == 2.8
        assert config.delta_window_ticks == 150
        assert config.min_signal_interval_sec == 20
        assert config.stop_mode == "ATR"
        assert config.stop_atr_mult == 0.90
    
    def test_config_priority_static_over_env(self, monkeypatch):
        """Тест приоритета: Static Config > Env (старый тест - теперь env overlay работает)"""
        # Устанавливаем env переменные
        monkeypatch.setenv("XAU_DELTA_Z_THRESHOLD", "4.0")
        
        #  есть в INSTRUMENT_CONFIGS (Static)
        config = get_config(use_env=True)
        
        # Теперь env overlay работает: должно вернуться значение из Env (4.0), а не из Static (3.0)
        assert config.delta_z_threshold == 4.0
        assert config.min_signal_interval_sec == 60
    
    def test_config_env_overlay_on_preset(self, monkeypatch):
        """Тест что env overlay накладывается поверх пресета"""
        # Устанавливаем env переменную для XAUUSDT
        monkeypatch.setenv("XAU_OBI_THRESHOLD", "0.31")
        
        # XAUUSDT есть в INSTRUMENT_CONFIGS (preset = 0.30)
        config = get_config("XAUUSDT", use_env=True)
        
        # Должно вернуться значение из Env (0.31), наложенное поверх пресета
        assert abs(config.obi_threshold - 0.31) < 1e-9
        # Другие параметры из пресета должны остаться
        assert config.delta_z_threshold == 3.0
        assert config.min_signal_interval_sec == 30
    
    def test_unknown_symbol_with_env(self):
        """Тест на неизвестный символ с use_env=False"""
        with pytest.raises(ValueError, match="Unknown symbol"):
            get_config("UNKNOWN", use_env=False)
    
    def test_unknown_symbol_from_env_creates_config(self, monkeypatch):
        """Тест что use_env=True создает конфиг для неизвестного символа из Env/Defaults"""
        monkeypatch.setenv("CUSTOM_DELTA_Z_THRESHOLD", "5.5")
        config = get_config("CUSTOM", use_env=True)
        
        assert config.symbol == "CUSTOM"
        assert config.delta_z_threshold == 5.5


class TestInstrumentRegistration:
    """Тесты для регистрации инструментов"""
    
    def test_register_new_instrument(self):
        """Тест регистрации нового инструмента"""
        # Создаем custom конфигурацию и спецификацию
        custom_config = OrderFlowConfig(
            symbol="CUSTOM",
            delta_z_threshold=5.0,
        )
        
        custom_specs = SymbolSpecs(
            symbol="CUSTOM",
            contract_size=10.0,
            price_decimals=4,
        )
        
        # Регистрируем
        register_instrument("CUSTOM", custom_config, custom_specs)
        
        # Проверяем что зарегистрирован
        assert "CUSTOM" in INSTRUMENT_CONFIGS
        assert "CUSTOM" in INSTRUMENT_SPECS
        
        # Проверяем что можем получить
        config = get_config("CUSTOM", use_env=False)
        specs = get_specs("CUSTOM")
        
        assert config.delta_z_threshold == 5.0
        assert specs.contract_size == 10.0
        assert specs.price_decimals == 4
        
        # Cleanup
        del INSTRUMENT_CONFIGS["CUSTOM"]
        del INSTRUMENT_SPECS["CUSTOM"]


class TestAllPresets:
    """Тесты для всех пресетов"""
    
    @pytest.mark.parametrize("symbol", [
        "",
        "BTCUSD",
        "BTCUSDT",
        "ETHUSD",
        "ETHUSDT",
        "BNBUSD",
        "BNBUSDT",
        "1000PEPEUSDT",
        "DOGEUSDT",
        "1000SHIBUSDT",
        "1000FLOKIUSDT",
        "1000BONKUSDT",
        "WIFUSDT",
        "SUIUSDT",
        "APTUSDT",
        "XAUUSDT",
    ])
    def test_preset_exists(self, symbol):
        """Тест что для символа есть пресет конфигурации"""
        config = get_config(symbol, use_env=False)
        assert config.symbol == symbol
    
    @pytest.mark.parametrize("symbol", [
        "",
        "BTCUSD",
        "BTCUSDT",
        "ETHUSD",
        "ETHUSDT",
        "BNBUSD",
        "BNBUSDT",
        "1000PEPEUSDT",
        "DOGEUSDT",
        "1000SHIBUSDT",
        "1000FLOKIUSDT",
        "1000BONKUSDT",
        "WIFUSDT",
        "SUIUSDT",
        "APTUSDT",
        "XAUUSDT",
    ])
    def test_specs_exists(self, symbol):
        """Тест что для символа есть спецификация"""
        specs = get_specs(symbol)
        assert specs.symbol == symbol
        assert specs.contract_size > 0
        assert specs.min_lot > 0
        assert specs.price_decimals >= 2


class TestNormalizeSymbol:
    """Тесты для нормализации символов"""
    
    def test_normalize_symbol_strips_tv_suffix(self):
        """Тест что normalize_symbol убирает .P суффикс TradingView"""
        assert normalize_symbol("XAUUSDT.P") == "XAUUSDT"
        assert normalize_symbol("xauusdt.p") == "XAUUSDT"
        assert normalize_symbol("XAUUSDT.PERP") == "XAUUSDT"
        assert normalize_symbol("BTCUSDT.P") == "BTCUSDT"
    
    def test_normalize_symbol_strips_exchange_prefix(self):
        """Тест что normalize_symbol убирает префикс биржи (BINANCE:)"""
        assert normalize_symbol("BINANCE:XAUUSDT.P") == "XAUUSDT"
        assert normalize_symbol("binance:xauusdt.p") == "XAUUSDT"
        assert normalize_symbol("BINANCE:BTCUSDT") == "BTCUSDT"
    
    def test_normalize_symbol_handles_combined(self):
        """Тест комбинации префикса биржи и .P суффикса"""
        assert normalize_symbol("BINANCE:XAUUSDT.P") == "XAUUSDT"
        assert normalize_symbol("  binance:xauusdt.perp  ") == "XAUUSDT"
    
    def test_normalize_symbol_preserves_normal_symbols(self):
        """Тест что обычные символы остаются без изменений (кроме регистра)"""
        assert normalize_symbol("XAUUSDT") == "XAUUSDT"
        assert normalize_symbol("xauusdt") == "XAUUSDT"
        assert normalize_symbol("BTCUSDT") == "BTCUSDT"
        assert normalize_symbol("  ETHUSDT  ") == "ETHUSDT"


class TestXAUUSDTConfig:
    """Тесты для XAUUSDT конфигурации"""
    
    def test_xauusdt_config_from_preset(self):
        """Тест конфигурации XAUUSDT из пресета"""
        config = get_config("XAUUSDT", use_env=False)
        
        assert config.symbol == "XAUUSDT"
        assert config.delta_z_threshold == 3.0
        assert config.delta_abs_min_usd == 5_000.0
        assert config.obi_threshold == 0.30
        assert config.obi_min_duration == 1.8
        assert config.dist_bp_threshold == 12.0
        assert config.min_signal_interval_sec == 30
        assert config.stop_mode == "ATR"
        assert config.stop_atr_mult == 0.70
        assert config.tp_mode == "RR"
        assert config.tp_rr == "1,1.5,2.5"
        assert config.cancel_spike_enable == True
        assert config.cancel_spike_mode == "monitor"
        assert config.metadata["asset_class"] == "tradfi_perp"
        assert config.metadata["base_currency"] == "XAU"
        assert config.metadata["quote_currency"] == "USDT"
    
    def test_xauusdt_specs(self):
        """Тест спецификации XAUUSDT"""
        specs = get_specs("XAUUSDT")
        
        assert specs.symbol == "XAUUSDT"
        assert specs.contract_size == 1.0
        assert specs.pip_value == 0.01
        assert specs.lot_step == 0.001
        assert specs.min_lot == 0.001
        assert specs.max_lot == 1_000_000.0
        assert specs.tick_value == 0.01
        assert specs.price_decimals == 2
        assert specs.volume_decimals == 3
        assert specs.delta_z == 3.0
    
    def test_xauusdt_registry_alias(self):
        """Тест что XAUUSDT.P нормализуется и находит пресет"""
        c1 = get_config("XAUUSDT", use_env=False)
        c2 = get_config("XAUUSDT.P", use_env=False)
        assert c1.symbol == "XAUUSDT"
        assert c2.symbol == "XAUUSDT"
        assert c1.delta_z_threshold == c2.delta_z_threshold
    
    def test_xauusdt_env_overlay(self, monkeypatch):
        """Тест env overlay для XAUUSDT"""
        monkeypatch.setenv("XAU_DELTA_ABS_MIN_USD", "6000")
        monkeypatch.setenv("XAU_OBI_THRESHOLD", "0.32")
        monkeypatch.setenv("XAU_DIST_BP_THRESHOLD", "13.0")
        
        config = get_config("XAUUSDT", use_env=True)
        
        assert config.delta_abs_min_usd == 6000.0
        assert abs(config.obi_threshold - 0.32) < 1e-9
        assert config.dist_bp_threshold == 13.0
        # Параметры без env должны остаться из пресета
        assert config.delta_z_threshold == 3.0
        assert config.min_signal_interval_sec == 30

