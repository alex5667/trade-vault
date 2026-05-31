"""
Unit тесты для handlers/handler_factory.py
"""

import pytest

from handlers.base_orderflow_handler import BaseOrderFlowHandler
from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler
from handlers.handler_factory import OrderFlowHandlerFactory, create_handler
from handlers.xauusd_orderflow_handler_v2 import XAUUSDOrderFlowHandlerV2


class TestOrderFlowHandlerFactory:
    """Тесты для OrderFlowHandlerFactory"""

    def test_create_xauusd_handler(self):
        """Тест создания обработчика для """
        handler = OrderFlowHandlerFactory.create("")

        assert isinstance(handler, BaseOrderFlowHandler)
        assert isinstance(handler, XAUUSDOrderFlowHandlerV2)
        assert handler.symbol == ""
        assert handler.config.symbol == ""

    def test_create_btcusd_handler(self):
        """Тест создания обработчика для BTCUSD"""
        handler = OrderFlowHandlerFactory.create("BTCUSD")

        assert isinstance(handler, BaseOrderFlowHandler)
        assert isinstance(handler, CryptoOrderFlowHandler)
        assert handler.symbol == "BTCUSD"
        assert handler.config.symbol == "BTCUSD"

    def test_create_ethusd_handler(self):
        """Тест создания обработчика для ETHUSD"""
        handler = OrderFlowHandlerFactory.create("ETHUSD")

        assert isinstance(handler, CryptoOrderFlowHandler)
        assert handler.symbol == "ETHUSD"

    def test_create_with_alias(self):
        """Тест создания обработчика через alias"""
        handler = OrderFlowHandlerFactory.create("BTC")

        assert handler.symbol == "BTCUSD"  # Должен развернуться в BTCUSD

    def test_create_unknown_symbol_raises_error(self):
        """Тест на неизвестный символ"""
        with pytest.raises(ValueError, match="No handler registered"):
            OrderFlowHandlerFactory.create("UNKNOWN")

    def test_create_fallback_for_crypto(self):
        """Тест fallback для неизвестной крипты (должен использовать CryptoOrderFlowHandler)"""
        # Создаем обработчик для незарегистрированного crypto символа
        handler = OrderFlowHandlerFactory.create("CUSTOMUSDT")

        # Должен использовать CryptoOrderFlowHandler как fallback
        assert isinstance(handler, CryptoOrderFlowHandler)
        assert handler.symbol == "CUSTOMUSDT"

    def test_is_supported_xauusd(self):
        """Тест проверки поддержки """
        assert OrderFlowHandlerFactory.is_supported("") is True

    def test_is_supported_btcusd(self):
        """Тест проверки поддержки BTCUSD"""
        assert OrderFlowHandlerFactory.is_supported("BTCUSD") is True

    def test_is_supported_alias(self):
        """Тест проверки поддержки через alias"""
        assert OrderFlowHandlerFactory.is_supported("BTC") is True

    def test_is_not_supported_unknown(self):
        """Тест проверки поддержки неизвестного символа"""
        assert OrderFlowHandlerFactory.is_supported("UNKNOWN") is False

    def test_list_supported_symbols(self):
        """Тест получения списка поддерживаемых символов"""
        symbols = OrderFlowHandlerFactory.list_supported_symbols()

        assert isinstance(symbols, list)
        assert len(symbols) > 0
        assert "" in symbols
        assert "BTCUSD" in symbols
        assert "ETHUSD" in symbols

    def test_list_supported_instruments(self):
        """Тест получения структурированного списка"""
        instruments = OrderFlowHandlerFactory.list_supported_instruments()

        assert isinstance(instruments, dict)
        assert "FOREX" in instruments
        assert "CRYPTO" in instruments

        # Проверяем что в каждой категории есть символы
        assert len(instruments["FOREX"]) > 0
        assert len(instruments["CRYPTO"]) > 0

        # Проверяем конкретные символы
        assert "" in instruments["FOREX"]
        assert "BTCUSD" in instruments["CRYPTO"]

    def test_register_custom_handler(self):
        """Тест регистрации пользовательского обработчика"""
        from core.instrument_config import SymbolSpecs
        from handlers.base_orderflow_handler import BaseOrderFlowHandler

        class CustomHandler(BaseOrderFlowHandler):
            def __init__(self, config=None):
                super().__init__("CUSTOM", config)

            def _get_symbol_specs(self):
                return SymbolSpecs(
                    symbol="CUSTOM",
                    contract_size=1.0,
                    price_decimals=2
                )

        # Регистрируем
        OrderFlowHandlerFactory.register_handler("CUSTOM", CustomHandler, "CUSTOM")

        # Проверяем что можем создать
        handler = OrderFlowHandlerFactory.create("CUSTOM")
        assert isinstance(handler, CustomHandler)
        assert handler.symbol == "CUSTOM"

        # Проверяем что в списке
        assert "CUSTOM" in OrderFlowHandlerFactory.list_supported_symbols()

        # Cleanup (удаляем из registry)
        if "CUSTOM" in OrderFlowHandlerFactory._handlers.get("CUSTOM", {}):
            del OrderFlowHandlerFactory._handlers["CUSTOM"]["CUSTOM"]


class TestCreateHandlerHelper:
    """Тесты для вспомогательной функции create_handler"""

    def test_create_handler_xauusd(self):
        """Тест вспомогательной функции для """
        handler = create_handler("")

        assert isinstance(handler, XAUUSDOrderFlowHandlerV2)
        assert handler.symbol == ""

    def test_create_handler_btcusd(self):
        """Тест вспомогательной функции для BTCUSD"""
        handler = create_handler("BTCUSD")

        assert isinstance(handler, CryptoOrderFlowHandler)
        assert handler.symbol == "BTCUSD"


class TestInstrumentTypeDetection:
    """Тесты для автоматического определения типа инструмента"""

    def test_detect_forex_xau(self):
        """Тест определения FOREX (золото)"""
        instrument_type = OrderFlowHandlerFactory._get_instrument_type("")
        assert instrument_type == "FOREX"

    def test_detect_forex_xag(self):
        """Тест определения FOREX (серебро)"""
        instrument_type = OrderFlowHandlerFactory._get_instrument_type("XAGUSD")
        assert instrument_type == "FOREX"

    def test_detect_crypto_btc(self):
        """Тест определения CRYPTO (Bitcoin)"""
        instrument_type = OrderFlowHandlerFactory._get_instrument_type("BTCUSD")
        assert instrument_type == "CRYPTO"

    def test_detect_crypto_usdt(self):
        """Тест определения CRYPTO (USDT пара)"""
        instrument_type = OrderFlowHandlerFactory._get_instrument_type("BTCUSDT")
        assert instrument_type == "CRYPTO"

    def test_detect_unknown(self):
        """Тест определения неизвестного типа"""
        instrument_type = OrderFlowHandlerFactory._get_instrument_type("UNKNOWN")
        assert instrument_type == "UNKNOWN"

