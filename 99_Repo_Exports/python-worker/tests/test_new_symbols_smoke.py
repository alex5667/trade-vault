"""
Smoke тесты для новых криптовалютных символов (PEPEUSDT, DOGEUSDT, etc.)

Проверяем что:
- Конфигурации загружаются без ошибок
- Основные параметры валидны
- ENV переменные парсятся корректно
"""

import os
import pytest
from core.instrument_config import (
    get_config,
    get_specs,
    INSTRUMENT_CONFIGS,
    INSTRUMENT_SPECS,
    OrderFlowConfig,
)


class TestNewSymbolsConfig:
    """Тесты конфигураций новых символов"""

    NEW_SYMBOLS = [
        "PEPEUSDT", "DOGEUSDT", "SHIBUSDT", "FLOKIUSDT",
        "BONKUSDT", "WIFUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT"
    ]

    @pytest.mark.parametrize("symbol", NEW_SYMBOLS)
    def test_symbol_in_registry(self, symbol):
        """Тест что символ зарегистрирован в реестре"""
        assert symbol in INSTRUMENT_CONFIGS, f"Symbol {symbol} not found in INSTRUMENT_CONFIGS"
        assert symbol in INSTRUMENT_SPECS, f"Symbol {symbol} not found in INSTRUMENT_SPECS"

    @pytest.mark.parametrize("symbol", NEW_SYMBOLS)
    def test_config_loads_without_error(self, symbol):
        """Тест что конфигурация загружается без ошибок"""
        config = get_config(symbol, use_env=False)
        assert isinstance(config, OrderFlowConfig)
        assert config.symbol == symbol

    @pytest.mark.parametrize("symbol", NEW_SYMBOLS)
    def test_specs_loads_without_error(self, symbol):
        """Тест что спецификации загружаются без ошибок"""
        specs = get_specs(symbol)
        assert specs.symbol == symbol
        assert specs.contract_size > 0
        assert specs.min_lot > 0

    @pytest.mark.parametrize("symbol", NEW_SYMBOLS)
    def test_orderflow_params_valid(self, symbol):
        """Тест что основные параметры orderflow валидны"""
        config = get_config(symbol, use_env=False)

        # Проверяем базовые параметры
        assert config.delta_window_ticks > 0
        assert config.delta_z_threshold > 0
        assert config.weak_progress_atr > 0
        assert config.obi_threshold > 0
        assert config.min_signal_interval_sec > 0
        assert config.read_count > 0
        assert config.read_block_ms > 0

        # Проверяем risk management
        assert config.stop_mode in ["ATR", "PCT", "POINTS"]
        assert config.stop_atr_mult > 0
        assert config.tp_rr is not None

    def test_dogeusdt_specific_params(self):
        """Тест специфических параметров DOGEUSDT"""
        config = get_config("DOGEUSDT", use_env=False)

        assert config.delta_window_ticks == 150
        assert config.delta_z_threshold == 2.8
        assert config.min_signal_interval_sec == 20
        assert config.stop_atr_mult == 0.90

    def test_pepeusdt_specific_params(self):
        """Тест специфических параметров PEPEUSDT"""
        config = get_config("PEPEUSDT", use_env=False)

        assert config.delta_window_ticks == 240
        assert config.delta_z_threshold == 3.1
        assert config.min_signal_interval_sec == 45
        assert config.stop_atr_mult == 1.10


class TestNewSymbolsEnvParsing:
    """Тесты парсинга ENV переменных для новых символов"""

    def test_env_parsing_for_new_symbol(self, monkeypatch):
        """Тест что ENV переменные парсятся для новых символов"""
        # Устанавливаем тестовые ENV переменные для DOGE
        monkeypatch.setenv("DOGE_DELTA_Z_THRESHOLD", "4.0")
        monkeypatch.setenv("DOGE_MIN_SIGNAL_INTERVAL", "60")
        monkeypatch.setenv("DOGE_STOP_ATR_MULT", "1.2")

        config = get_config("DOGEUSDT", use_env=True)

        # Проверяем что ENV переменные применились
        assert config.delta_z_threshold == 4.0
        assert config.min_signal_interval_sec == 60
        assert config.stop_atr_mult == 1.2

    def test_env_parsing_fallback(self, monkeypatch):
        """Тест что при отсутствии ENV используются дефолты"""
        # Очищаем все DOGE переменные
        for key in os.environ:
            if key.startswith("DOGE_"):
                monkeypatch.delenv(key, raising=False)

        config = get_config("DOGEUSDT", use_env=True)

        # Должны использоваться значения из конфига
        assert config.delta_z_threshold == 2.8
        assert config.min_signal_interval_sec == 20


class TestNewSymbolsIntegration:
    """Интеграционные тесты для новых символов"""

    def test_all_new_symbols_have_configs(self):
        """Тест что все новые символы имеют полные конфигурации"""
        expected_symbols = {
            "PEPEUSDT", "DOGEUSDT", "SHIBUSDT", "FLOKIUSDT",
            "BONKUSDT", "WIFUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT"
        }

        actual_symbols = set(INSTRUMENT_CONFIGS.keys()) & expected_symbols

        assert actual_symbols == expected_symbols, f"Missing configs for: {expected_symbols - actual_symbols}"

    def test_config_consistency(self):
        """Тест консистентности конфигураций"""
        for symbol in ["PEPEUSDT", "DOGEUSDT", "SHIBUSDT", "FLOKIUSDT",
                       "BONKUSDT", "WIFUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT"]:
            config = get_config(symbol, use_env=False)
            specs = get_specs(symbol)

            # Проверяем что символы совпадают
            assert config.symbol == specs.symbol == symbol

            # Проверяем что metadata содержит asset_class
            assert "asset_class" in config.metadata
            assert config.metadata["asset_class"] == "crypto"

            # Проверяем базовую валидность
            assert specs.contract_size == 1.0  # Все крипты 1:1
            assert specs.price_decimals >= 2
