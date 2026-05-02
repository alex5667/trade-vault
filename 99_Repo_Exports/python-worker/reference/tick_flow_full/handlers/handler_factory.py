"""
Фабрика для создания обработчиков Order Flow.

Централизованное управление созданием обработчиков для разных инструментов.
Автоматически определяет тип инструмента и создает соответствующий обработчик.
"""

from typing import Optional, Type, Dict, Any
from .base_orderflow_handler import BaseOrderFlowHandler
from .xauusd_orderflow_handler_v2 import XAUUSDOrderFlowHandlerV2
from .crypto_orderflow_handler import CryptoOrderFlowHandler
from core.instrument_config import OrderFlowConfig
# # from health_metrics import ...
from typing import Optional
from .handler_dependencies import HandlerDependencies


class OrderFlowHandlerFactory:
    """
    Фабрика для создания обработчиков Order Flow.,
    
    Автоматически определяет тип инструмента по символу и создает,
    соответствующий обработчик с нужной конфигурацией.,
    """
    
    # Регистрация обработчиков по типу инструмента
    _handlers: Dict[str, Dict[str, Type[BaseOrderFlowHandler]]] = {
        "FOREX": {
            "XAUUSD": XAUUSDOrderFlowHandlerV2,
            "XAGUSD": XAUUSDOrderFlowHandlerV2,  # Используем тот же обработчик для серебра
        },
        "CRYPTO": {
            "BTCUSD": CryptoOrderFlowHandler,
            "BTCUSDT": CryptoOrderFlowHandler,
            "ETHUSD": CryptoOrderFlowHandler,
            "ETHUSDT": CryptoOrderFlowHandler,
            "BNBUSD": CryptoOrderFlowHandler,
            "BNBUSDT": CryptoOrderFlowHandler,
            "SOLUSD": CryptoOrderFlowHandler,
            "SOLUSDT": CryptoOrderFlowHandler,
            "XRPUSD": CryptoOrderFlowHandler,
            "XRPUSDT": CryptoOrderFlowHandler,
            "ADAUSD": CryptoOrderFlowHandler,
            "ADAUSDT": CryptoOrderFlowHandler,
            "XAUUSDT": CryptoOrderFlowHandler,
            "PEPEUSDT": CryptoOrderFlowHandler,
            "DOGEUSDT": CryptoOrderFlowHandler,
            "SHIBUSDT": CryptoOrderFlowHandler,
            "FLOKIUSDT": CryptoOrderFlowHandler,
            "BONKUSDT": CryptoOrderFlowHandler,
            "WIFUSDT": CryptoOrderFlowHandler,
            "SUIUSDT": CryptoOrderFlowHandler,
            "APTUSDT": CryptoOrderFlowHandler,
            "ARBUSDT": CryptoOrderFlowHandler,
        }
    }
    
    # Маппинг alias символов (для обратной совместимости)
    _symbol_aliases: Dict[str, str] = {
        "BTC": "BTCUSD",
        "ETH": "ETHUSD",
        "BNB": "BNBUSD",
        "SOL": "SOLUSD",
        "ADA": "ADAUSD",
    }
    
    @classmethod
    def create(cls, symbol: str, config: Optional[OrderFlowConfig] = None, health_metrics: Optional[object] = None) -> BaseOrderFlowHandler:
        """
        Создает обработчик для указанного символа.
        
        Автоматически определяет тип инструмента и создает соответствующий
        обработчик с нужной конфигурацией.
        
        Args:
            symbol: Символ инструмента (XAUUSD, BTCUSD и т.д.)
            config: Опциональная конфигурация (если не указана - загрузится из env)
        
        Returns:
            Экземпляр обработчика для указанного символа
            
        Raises:
            ValueError: Если для символа не зарегистрирован обработчик
        """
        # Проверяем alias
        if symbol in cls._symbol_aliases:
            symbol = cls._symbol_aliases[symbol]
        
        # Определяем тип инструмента
        instrument_type = cls._get_instrument_type(symbol)
        
        # Получаем класс обработчика
        handler_class = cls._handlers.get(instrument_type, {}).get(symbol)
        
        if not handler_class:
            # Fallback: пытаемся использовать generic обработчик для типа инструмента
            handler_class = cls._get_fallback_handler(instrument_type, symbol)
            
            if not handler_class:
                raise ValueError(
                    f"No handler registered for {symbol}. "
                    f"Available symbols: {cls.list_supported_symbols()}"
                )
        
        # Resolve dependencies via DI container logic
        deps = cls._resolve_dependencies(symbol, config, health_metrics)
        
        # Создаем экземпляр
        if instrument_type == "CRYPTO":
            # Для крипты передаем symbol в конструктор
            # Read stream names from environment variables
            import os
            tick_stream = os.getenv(f"{symbol}_TICK_STREAM")
            book_stream = os.getenv(f"{symbol}_BOOK_STREAM")
            
            # Note: CryptoOrderFlowHandler MUST accept dependencies kwarg
            handler = handler_class(symbol, config, health_metrics=health_metrics, dependencies=deps)
            
            # Override stream names if provided in environment
            if tick_stream:
                handler.tick_stream = tick_stream
            if book_stream:
                handler.book_stream = book_stream
                
            return handler
        else:
            # Для Forex symbol уже захардкожен в конструкторе
            # Note: XAUUSDOrderFlowHandlerV2 MUST accept dependencies kwarg
            return handler_class(config, health_metrics=health_metrics, dependencies=deps)
            
    @classmethod
    def _get_instrument_type(cls, symbol: str) -> str:
        """Determines instrument type (FOREX, CRYPTO, etc.) based on symbol naming."""
        # USDT pairs are always CRYPTO (check this first to catch XAUUSDT)
        if symbol.endswith("USDT"):
            return "CRYPTO"
            
        # XAUUSD and XAGUSD (without T) are FOREX
        if symbol.startswith("XA") and symbol.endswith("USD") and not symbol.endswith("USDT"):
            return "FOREX"
            
        # Check standard crypto pairs
        if "USDT" in symbol or any(s in symbol for s in ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA"]):
            return "CRYPTO"
            
        # Check if symbol is already registered in some category
        for itype, handlers in cls._handlers.items():
            if symbol in handlers:
                return itype
                
        return "UNKNOWN"

    @classmethod
    def _get_fallback_handler(cls, instrument_type: str, symbol: str) -> Optional[Type[BaseOrderFlowHandler]]:
        """Returns a generic handler for the instrument type if a specific one isn't found."""
        if instrument_type == "CRYPTO":
            return CryptoOrderFlowHandler
        return None
            
    @classmethod
    def _resolve_dependencies(cls, symbol: str, config: Optional[OrderFlowConfig], health_metrics: Optional[Any]) -> HandlerDependencies:
        """
        Factory method to resolve all optional dependencies.
        Attempts to import and instantiate components, handling ImportErrors gracefully.
        """
        deps = HandlerDependencies()
        deps.health_metrics = health_metrics

    @classmethod
    def _resolve_dependencies(cls, symbol: str, config: Optional[OrderFlowConfig], health_metrics: Optional[Any]) -> HandlerDependencies:
        """
        Factory method to resolve all optional dependencies.
        Attempts to import and instantiate components, handling ImportErrors gracefully.
        """
        deps = HandlerDependencies()
        deps.health_metrics = health_metrics

        try:
            from services.health_monitor import HealthMonitorService
            deps.health_monitor = HealthMonitorService()
        except ImportError:
            pass  # Fail-open if service missing, though unlikely
        except ImportError:
            pass  # Fail-open if service missing, though unlikely

        # 0.5 Analysis & Indicators
        try:
            from core.liquidity_analyzer import LiquidityGeometryAnalyzer
            # For now we just pass the Class, or we can instantiate if we had args.
            # BaseOrderFlowHandler expects a class or instance? 
            # Original code instantiates it: self.liquidity_analyzer = LiquidityGeometryAnalyzer(...)
            # So we pass the Class to let handler instantiate using its config?
            # User request: "BaseOrderFlowHandler explicitly instantiates... makes testing difficult"
            # SO we should instantiate here OR pass a Factory.
            # LiquidityGeometryAnalyzer takes (window_ms, min_levels, etc.) which come from Config.
            # Ideally we pass a Factory closure or Partial.
            # For this phase, let's pass the CLASS so we can at least mock it in deps if needed,
            # OR better: Refactor logic to accept instance. 
            deps.liquidity_analyzer = LiquidityGeometryAnalyzer
        except ImportError:
            pass

        try:
            from core.atr import AverageTrueRange
            deps.atr_indicator = AverageTrueRange
        except ImportError:
            pass
            
        try:
            from core.levels_manager import LevelsManager
            deps.levels_manager = LevelsManager
        except ImportError:
            pass

        # Phase 7: Service Classes
        try:
            from services.cooldown import CooldownService
            deps.cooldown_service_cls = CooldownService
        except ImportError:
            pass
        
        try:
             from geometry.calibration import CalibrationService
             deps.calibration_service_cls = CalibrationService
        except ImportError:
             pass

        # 1. L3 / ETA
        try:
            from services.l3_queue_events_proxy import L3QueueEventsProxy
            deps.l3_queue = L3QueueEventsProxy
        except ImportError:
            pass

        try:
            from services.queue_eta_estimator import QueueETAEvaluator
            deps.queue_eta = QueueETAEvaluator
        except ImportError:
            pass
            
        # 2. Burst Tracker
        try:
            from services.burstiness_tracker import BurstinessTracker
            deps.burst_tracker = BurstinessTracker
        except ImportError:
            pass
            
        # 3. Geometry / Extrema
        try:
            from geometry.extrema import LocalExtremaService, LocalExtremaConfig
            # We store the class or a factory tuple
            deps.extrema_service = (LocalExtremaService, LocalExtremaConfig)
        except ImportError:
            pass
            
        # 4. Execution Config
        try:
            from signal_execution.setup_config import ExecutionSetupRepository
            deps.execution_setup = ExecutionSetupRepository
        except ImportError:
            pass
            
        # 5. Outbox
        try:
            from signals.signal_publisher import SignalPublisher
            from core.signal_outbox import SignalOutboxPublisher
            deps.outbox_publisher = (SignalPublisher, SignalOutboxPublisher)
        except ImportError:
            pass
            
        # 6. GPU
        try:
            from gpu.l2_processor import L2GPUProcessor
            deps.gpu_processor = L2GPUProcessor
        except ImportError:
            pass
            
        # 7. Regime
        try:
            from regime.market_regime_service import MarketRegimeService, RegimeConfig
            deps.regime_service = (MarketRegimeService, RegimeConfig)
        except ImportError:
            pass
            
        # 8. Scoring
        try:
            from signal_scoring.engine import SignalScoringEngine, ScoringConfig
            deps.scoring_engine = (SignalScoringEngine, ScoringConfig)
        except ImportError:
            pass
            
        # 9. Unified Pipeline
        try:
            from signals.unified_pipeline import UnifiedSignalPipeline
            deps.unified_pipeline = UnifiedSignalPipeline
        except ImportError:
            pass
            
        # 10. Signal Execution Services (Heavy)
        try:
            from signal_exec import (
                SignalService, ExecutionPlanner, SignalPerformanceTracker,
                SignalRepository, SignalBus
            )
            # Store as a bundle or individual classes
            deps.signal_service = SignalService
            deps.execution_planner = ExecutionPlanner
            deps.signal_repo = SignalRepository
            deps.signal_bus = SignalBus
            deps.performance_tracker = SignalPerformanceTracker
        except ImportError:
            pass

        return deps
    
    @classmethod
    def register_handler(
        cls,
        symbol: str,
        handler_class: Type[BaseOrderFlowHandler],
        instrument_type: str = "CUSTOM"
    ) -> None:
        """
        Регистрирует пользовательский обработчик для символа.
        
        Позволяет добавлять поддержку новых инструментов без изменения
        кода фабрики.
        
        Args:
            symbol: Символ инструмента
            handler_class: Класс обработчика (должен наследовать BaseOrderFlowHandler)
            instrument_type: Тип инструмента (FOREX, CRYPTO, CUSTOM и т.д.)
        
        Examples:
            >>> class MyCustomHandler(BaseOrderFlowHandler):
            ...     pass
            >>> OrderFlowHandlerFactory.register_handler("CUSTOM", MyCustomHandler, "CUSTOM")
        """
        if not issubclass(handler_class, BaseOrderFlowHandler):
            raise TypeError(f"{handler_class} must inherit from BaseOrderFlowHandler")
        
        if instrument_type not in cls._handlers:
            cls._handlers[instrument_type] = {}
        
        cls._handlers[instrument_type][symbol] = handler_class
        print(f"✅ Registered handler for {symbol} (type: {instrument_type})")
    
    @classmethod
    def list_supported_symbols(cls) -> list:
        """
        Возвращает список всех поддерживаемых символов.
        
        Returns:
            Список символов для которых зарегистрированы обработчики
        """
        symbols = []
        for instrument_type, handlers in cls._handlers.items():
            symbols.extend(handlers.keys())
        return sorted(symbols)
    
    @classmethod
    def list_supported_instruments(cls) -> dict:
        """
        Возвращает структурированный список поддерживаемых инструментов.
        
        Returns:
            Словарь {instrument_type: [symbols]}
        """
        return {
            instrument_type: sorted(handlers.keys())
            for instrument_type, handlers in cls._handlers.items()
        }
    
    @classmethod
    def is_supported(cls, symbol: str) -> bool:
        """
        Проверяет, поддерживается ли символ.
        
        Args:
            symbol: Символ инструмента
            
        Returns:
            True если символ поддерживается, False иначе
        """
        # Проверяем alias
        if symbol in cls._symbol_aliases:
            symbol = cls._symbol_aliases[symbol]
        
        # Проверяем прямую регистрацию
        instrument_type = cls._get_instrument_type(symbol)
        if symbol in cls._handlers.get(instrument_type, {}):
            return True
        
        # Проверяем fallback
        if cls._get_fallback_handler(instrument_type, symbol):
            return True
        
        return False


# ═════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═════════════════════════════════════════════════════════════════════

def create_handler(symbol: str, config: Optional[OrderFlowConfig] = None, health_metrics: Optional[Any] = None) -> BaseOrderFlowHandler:
    """
    Вспомогательная функция для создания обработчика.
    
    Alias для OrderFlowHandlerFactory.create()
    
    Args:
        symbol: Символ инструмента
        config: Опциональная конфигурация
        health_metrics: Опциональные метрики здоровья
        
    Returns:
        Экземпляр обработчика
    """
    return OrderFlowHandlerFactory.create(symbol, config, health_metrics=health_metrics)


def list_supported_symbols() -> list:
    """
    Вспомогательная функция для получения списка поддерживаемых символов.
    
    Returns:
        Список символов
    """
    return OrderFlowHandlerFactory.list_supported_symbols()


# ═════════════════════════════════════════════════════════════════════
# CLI UTILITY
# ═════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    """Утилита для вывода информации о поддерживаемых инструментах"""
    import sys
    
    print("═" * 70)
    print("OrderFlow Handler Factory - Supported Instruments")
    print("═" * 70)
    print()
    
    # Список по типам
    instruments = OrderFlowHandlerFactory.list_supported_instruments()
    for instrument_type, symbols in instruments.items():
        print(f"📊 {instrument_type}:")
        for symbol in symbols:
            print(f"   ✓ {symbol}")
        print()
    
    # Общее количество
    total = sum(len(symbols) for symbols in instruments.values())
    print(f"Total supported symbols: {total}")
    print()
    
    # Проверка конкретного символа (если передан аргумент)
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
        print(f"Checking symbol: {symbol}")
        
        if OrderFlowHandlerFactory.is_supported(symbol):
            print(f"✅ {symbol} is supported")
            try:
                handler = OrderFlowHandlerFactory.create(symbol)
                print(f"Handler class: {handler.__class__.__name__}")
                print(f"Config: {handler.config}")
                print(f"Specs: {handler.specs}")
            except Exception as e:
                print(f"❌ Error creating handler: {e}")
        else:
            print(f"❌ {symbol} is NOT supported")
            print(f"Available symbols: {OrderFlowHandlerFactory.list_supported_symbols()}")

