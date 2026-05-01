from __future__ import annotations
"""
Специализированный обработчик Order Flow для XAUUSD (Gold).

Наследует всю логику от BaseOrderFlowHandler, переопределяя только
специфичные для золота параметры.

REFACTORED VERSION - использует унифицированную архитектуру.
"""

from typing import Dict, Optional
from .base_orderflow_handler import BaseOrderFlowHandler, OrderflowSignalContext
from core.instrument_config import SymbolSpecs, OrderFlowConfig, get_specs, get_config
from .handler_dependencies import HandlerDependencies


class XAUUSDOrderFlowHandlerV2(BaseOrderFlowHandler):
    """
    Обработчик Order Flow для XAUUSD (Gold).
    
    Использует стандартную логику из BaseOrderFlowHandler.
    Переопределяет только специфику инструмента.
    """
    
    def __init__(self, config: OrderFlowConfig = None, *, health_metrics: Optional[object] = None, dependencies: Optional[HandlerDependencies] = None):
        """
        Инициализация обработчика для XAUUSD.

        Args:
            config: Конфигурация (опционально, загрузится из env или preset)
            health_metrics: Опциональные метрики здоровья
        """
        symbol = "XAUUSD"
        config = config or get_config(symbol, use_env=True)
        super().__init__(symbol, config, health_metrics=health_metrics, dependencies=dependencies)
    
    def _get_symbol_specs(self) -> SymbolSpecs:
        """Возвращает спецификацию для XAUUSD"""
        return get_specs("XAUUSD")
    
    def _estimate_atr(self, price: float) -> float:
        """
        Оценка типичного ATR для XAUUSD.
        
        Для XAUUSD на 1m timeframe типичный ATR составляет ~0.5-2.0 пункта.
        Используем консервативную оценку 1.2.
        
        Args:
            price: Текущая цена
            
        Returns:
            Оценочное значение ATR
        """
        import os
        atf_tf = os.getenv("ATR_TF", "1m")
        
        if atf_tf == "1m":
            return 1.2  # Типичный ATR для 1m XAUUSD
        elif atf_tf == "5m":
            return 3.5  # Типичный ATR для 5m XAUUSD
        elif atf_tf == "15m":
            return 6.5  # Типичный ATR для 15m XAUUSD
        else:
            return price * 0.0003  # 0.03% от цены для других TF
    
    def _get_default_hlc(self) -> Dict[str, float]:
        """
        Default HLC для XAUUSD.
        
        Используем примерные текущие значения для золота.
        """
        # Получаем текущую цену из последнего тика (если доступно)
        try:
            last_tick = self.redis_client.xrevrange(self.tick_stream, count=1)
            if last_tick:
                fields = last_tick[0][1]
                bid = float(fields.get("bid", 0))
                ask = float(fields.get("ask", 0))
                current_price = (bid + ask) / 2 if (bid and ask) else 3956.0
            else:
                current_price = 3956.0  # Примерная цена XAUUSD
        except Exception:
            current_price = 3956.0
        
        return {
            "H": current_price + 30,  # +30 пипсов
            "L": current_price - 30,  # -30 пипсов
            "C": current_price
        }


# Alias для обратной совместимости (если кто-то импортирует напрямую)
XAUUSDOrderFlowHandler = XAUUSDOrderFlowHandlerV2

