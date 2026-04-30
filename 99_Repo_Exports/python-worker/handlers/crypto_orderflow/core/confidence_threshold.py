"""
Улучшенный фильтр порога уверенности - специфичные для символа гейты уверенности.

Этот модуль реализует более строгие пороги уверенности для высокочастотных пар
таких как BTC/ETH, которые склонны генерировать больше ложных сигналов.

Философия дизайна:
    - Более высокие пороги для мажорных пар (BTC, ETH) снижают шум (churn) сигналов
    - Двойная фильтрация: абсолютная уверенность (0-100) и фактор уверенности (0-1)
    - Специфичные для символа переопределения для гранулярного контроля
    - Fail-open при отсутствии данных (пропускаем сигнал)

ENV Конфигурация:
    MIN_CONF_DEFAULT: Дефолтный минимальный скор уверенности (напр. 70)
    MIN_CONF_BTCUSDT: Специфичный порог для BTC (напр. 75)
    MIN_CONF_ETHUSDT: Специфичный порог для ETH (напр. 72)
    MIN_CONF_FACTOR_DEFAULT: Дефолтный минимальный фактор уверенности (напр. 0.45)
    MIN_CONF_FACTOR_BTCUSDT: Специфичный фактор для BTC (напр. 0.55)
    MIN_CONF_FACTOR_ETHUSDT: Специфичный фактор для ETH (напр. 0.52)

Использование:
    filter = ConfidenceThresholdFilter.from_env()
    result = filter.evaluate(confidence_pct=72.0, conf_factor=0.48, symbol="BTCUSDT")
    if not result.passed:
        # Отклоняем сигнал
        logger.info(f"Confidence veto: {result.veto_reason}")
"""

from __future__ import annotations

import os
import math
from typing import Any, Optional
from dataclasses import dataclass


@dataclass
class ConfidenceThresholdConfig:
    """Конфигурация для фильтра порога уверенности."""
    
    # Дефолтные пороги
    min_conf_default: float = 70.0  # Абсолютная уверенность (0-100)
    min_conf_factor_default: float = 0.45  # Фактор уверенности (0-1)
    
    # Специфичные для символа пороги
    min_conf_by_symbol: dict[str, float] = None  # Переопределения абсолютной уверенности
    min_conf_factor_by_symbol: dict[str, float] = None  # Переопределения фактора
    
    def __post_init__(self):
        if self.min_conf_by_symbol is None:
            self.min_conf_by_symbol = {}
        if self.min_conf_factor_by_symbol is None:
            self.min_conf_factor_by_symbol = {}
    
    @classmethod
    def from_env(cls) -> ConfidenceThresholdConfig:
        """Создает конфигурацию из переменных окружения."""
        
        # Парсим дефолтные пороги
        min_conf_default = float(os.getenv("MIN_CONF_DEFAULT", "50.0"))
        min_conf_factor_default = float(os.getenv("MIN_CONF_FACTOR_DEFAULT", "0.45"))
        
        # Парсим специфичные для символа пороги уверенности
        min_conf_by_symbol = {}
        for key, value in os.environ.items():
            if key.startswith("MIN_CONF_") and not key.startswith("MIN_CONF_FACTOR_") and key != "MIN_CONF_DEFAULT":
                symbol = key.replace("MIN_CONF_", "")
                try:
                    min_conf_by_symbol[symbol] = float(value)
                except (ValueError, TypeError):
                    pass
        
        # Парсим специфичные для символа пороги фактора уверенности
        min_conf_factor_by_symbol = {}
        for key, value in os.environ.items():
            if key.startswith("MIN_CONF_FACTOR_") and key != "MIN_CONF_FACTOR_DEFAULT":
                symbol = key.replace("MIN_CONF_FACTOR_", "")
                try:
                    min_conf_factor_by_symbol[symbol] = float(value)
                except (ValueError, TypeError):
                    pass
        
        return cls(
            min_conf_default=min_conf_default
            min_conf_factor_default=min_conf_factor_default
            min_conf_by_symbol=min_conf_by_symbol
            min_conf_factor_by_symbol=min_conf_factor_by_symbol
        )


@dataclass
class ConfidenceThresholdResult:
    """Результат оценки порога уверенности."""
    
    passed: bool  # True если сигнал проходит оба фильтра
    
    # Значения уверенности
    confidence_pct: float  # Фактическая уверенность (0-100)
    conf_factor: float  # Фактический фактор уверенности (0-1)
    
    # Примененные пороги
    min_conf_threshold: float  # Требуемая уверенность (0-100)
    min_conf_factor_threshold: float  # Требуемый фактор (0-1)
    
    # Pass/fail per filter
    conf_pct_passed: bool
    conf_factor_passed: bool
    
    # Metadata
    symbol: str
    veto_reason: Optional[str] = None


class ConfidenceThresholdFilter:
    """
    Фильтр, отклоняющий сигналы с недостаточной уверенностью.
    
    Применяет двойную фильтрацию:
        1. Абсолютный скор уверенности (шкала 0-100)
        2. Фактор уверенности (нормализованная шкала 0-1)
    
    Оба фильтра должны пройти, чтобы сигнал был принят.
    Специфичные для символа пороги позволяют более строгую фильтрацию для высокочастотных пар.
    
    Использование:
        filter = ConfidenceThresholdFilter.from_env()
        result = filter.evaluate(
            confidence_pct=72.0
            conf_factor=0.48
            symbol="BTCUSDT"
        )
        if not result.passed:
            logger.info(f"Confidence veto: {result.veto_reason}")
    """
    
    def __init__(self, config: ConfidenceThresholdConfig):
        self.config = config
    
    @classmethod
    def from_env(cls) -> ConfidenceThresholdFilter:
        """Создает фильтр из переменных окружения."""
        return cls(ConfidenceThresholdConfig.from_env())
    
    def _get_min_conf_pct(self, symbol: str) -> float:
        """Возвращает минимальный порог уверенности для символа."""
        return self.config.min_conf_by_symbol.get(symbol, self.config.min_conf_default)
    
    def _get_min_conf_factor(self, symbol: str) -> float:
        """Возвращает минимальный порог фактора уверенности для символа."""
        return self.config.min_conf_factor_by_symbol.get(
            symbol, 
            self.config.min_conf_factor_default
        )
    
    def evaluate(
        self
        confidence_pct: Optional[float]
        conf_factor: Optional[float]
        symbol: str
    ) -> ConfidenceThresholdResult:
        """
        Оценивает, соответствует ли сигнал порогам уверенности.
        
        Args:
            confidence_pct: Абсолютный скор уверенности (0-100), может быть None
            conf_factor: Фактор уверенности (0-1), может быть None
            symbol: Торговый символ (напр., "BTCUSDT")
            
        Returns:
            ConfidenceThresholdResult с решением pass/fail и детализацией
        """
        
        # Получаем пороги для этого символа
        min_conf_pct = self._get_min_conf_pct(symbol)
        min_conf_factor = self._get_min_conf_factor(symbol)
        
        # Санитизируем входы (fail-closed при отсутствии данных: сигналы без валидного скора отклоняются)
        conf_pct = safe_float(confidence_pct, 0.0)  # Будет отклонено (0.0 < min_conf)
        conf_fac = safe_float(conf_factor, 0.0)  # Будет отклонено (0.0 < min_factor)
        
        # Оцениваем каждый фильтр
        conf_pct_passed = conf_pct >= min_conf_pct
        conf_factor_passed = conf_fac >= min_conf_factor
        
        # Оба должны пройти
        passed = conf_pct_passed and conf_factor_passed
        
        # Генерируем причину вето при неудаче
        veto_reason = None
        if not passed:
            failures = []
            if not conf_pct_passed:
                failures.append(
                    f"confidence={conf_pct:.1f} < min={min_conf_pct:.1f}"
                )
            if not conf_factor_passed:
                failures.append(
                    f"conf_factor={conf_fac:.3f} < min={min_conf_factor:.3f}"
                )
            veto_reason = "; ".join(failures)
        
        return ConfidenceThresholdResult(
            passed=passed
            confidence_pct=conf_pct
            conf_factor=conf_fac
            min_conf_threshold=min_conf_pct
            min_conf_factor_threshold=min_conf_factor
            conf_pct_passed=conf_pct_passed
            conf_factor_passed=conf_factor_passed
            symbol=symbol
            veto_reason=veto_reason
        )


def safe_float(val: Any, default: float = 0.0) -> float:
    """Безопасно конвертирует значение в float."""
    try:
        f = float(val) if val is not None else default
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default

