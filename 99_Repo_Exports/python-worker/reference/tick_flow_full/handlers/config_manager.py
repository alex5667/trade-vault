# config_manager.py
"""
Функционал управления конфигурацией, извлеченный из base_orderflow_handler.py
"""

from __future__ import annotations

from typing import Optional, Dict, Any
import os

# from common.log import setup_logger

def setup_logger(name):
    import logging
    return logging.getLogger(name)

class ConfigManager:
    """
    Управляет конфигурацией и настройками для orderflow handler.
    """

    def __init__(self, symbol: str, *, signal_stream_prefix: Optional[str] = None, strategy_key: str = "orderflow"):
        self.symbol = symbol
        self._signal_stream_prefix = signal_stream_prefix
        self._strategy_key = strategy_key
        self.logger = setup_logger(f"ConfigManager:{symbol}")

    def _get_source_name(self) -> str:
        """Получение имени источника для этого хендлера."""
        return "OrderFlow"

    def _get_strategy_key(self) -> str:
        """Получение ключа стратегии для именования стримов."""
        return self._strategy_key

    def _get_signal_stream(self) -> str:
        """Получение имени стрима сигналов."""
        env_stream = os.getenv("ORDERFLOW_SIGNAL_STREAM")
        if env_stream:
            return env_stream
        # Предпочтение явному префиксу от хендлера (например, signals:cryptoorderflow)
        if self._signal_stream_prefix:
            p = self._signal_stream_prefix.rstrip(":")
            return f"{p}:{self.symbol}"
        return f"signals:{self._get_strategy_key()}:{self.symbol}"

    def _get_min_confidence_for_symbol(self, symbol: str | None) -> float:
        """
        Получение минимального порога уверенности для символа (шкала 0..1).
        """
        # Env поддерживает и 0..1, и 0..100 для обратной совместимости:
        # - если значение > 1 -> считаем процентами и конвертируем в 0..1
        raw = float(os.getenv("MIN_SIGNAL_CONFIDENCE", "0.30"))
        base_min_conf = (raw / 100.0) if raw > 1.0 else raw
        # ограничение диапазона
        if base_min_conf < 0.0:
            base_min_conf = 0.0
        if base_min_conf > 1.0:
            base_min_conf = 1.0

        if not symbol:
            return base_min_conf

        sym = symbol.upper()

        # Конфигурация специфических порогов по символам
        # Формат: префикс/паттерн -> порог confidence (in 0..1 scale)
        symbol_confidence_overrides = {
            "XAU": 0.20,  # Все варианты золота
        }

        # Проверяем точное совпадение префикса
        for prefix, threshold in symbol_confidence_overrides.items():
            if sym.startswith(prefix):
                # Семантика: жесткая перезапись (не min/max).
                # Если нужен "потолок", используйте min(base_min_conf, threshold)
                return float(threshold)

        return base_min_conf

    def get_min_confidence_bar(self, symbol: str | None = None) -> float:
        """
        Минимальная уверенность для барных сигналов (1 минута).
        Барные сигналы более качественные, поэтому порог может быть ниже.
        """
        base = self._get_min_confidence_for_symbol(symbol)
        # Барные сигналы получают скидку, так как они качественнее
        return max(0.1, base * 0.8)  # минимум 0.1, скидка 20%

    def get_min_confidence_bucket(self, symbol: str | None = None) -> float:
        """
        Минимальная уверенность для бакетных сигналов.
        Бакетные сигналы чаще, поэтому порог должен быть выше.
        """
        base = self._get_min_confidence_for_symbol(symbol)
        # Бакетные сигналы требуют более высокого порога из-за частоты
        return min(0.9, base * 1.5)  # максимум 0.9, наценка 50%

    def get_min_burst_ratio(self, signal_type: str = "bar") -> float:
        """
        Минимальный burst ratio для гейта качества.
        Бакетные сигналы требуют более строгих условий burst.
        """
        base = float(os.getenv("MIN_BURST_RATIO", "1.6"))
        if signal_type.lower() == "bucket":
            return base * 1.2  # на 20% строже для бакетов
        return base

    def get_min_imbalance(self, signal_type: str = "bar") -> float:
        """
        Минимальный OBI imbalance для гейта качества.
        Бакетные сигналы требуют более строгих условий imbalance.
        """
        base = float(os.getenv("MIN_OBI_IMBALANCE", "0.20"))
        if signal_type.lower() == "bucket":
            return base * 1.3  # на 30% строже для бакетов
        return base

    def get_min_trades_breakout(self, signal_type: str = "bar") -> int:
        """
        Минимальное кол-во сделок для гейта качества breakout.
        Бакетные сигналы могут требовать больше сделок из-за короткого таймфрейма.
        """
        base = int(os.getenv("MIN_TRADES_BREAKOUT", "20"))
        if signal_type.lower() == "bucket":
            return max(5, base // 2)  # Минимум 5, но половина от требований бара
        return base

    def _get_calibrated_trailing_params(self) -> Dict[str, Any]:
        """
        Получение откалиброванных параметров трейлинга из Redis или дефолтных.
        """
        # Обычно это загружается из Redis calibration store
        # Пока возвращаем дефолтные значения
        return {
            'trailing_offset_pct': 0.001,  # 0.1%
            'trailing_increment_pct': 0.0005,  # 0.05%
            'max_trailing_distance_pct': 0.005,  # 0.5%
            'min_trailing_distance_pct': 0.0005,  # 0.02%
        }

    def _parse_rr_levels(self, rr_str: str) -> list[float]:
        """
        Парсинг строки уровней риск-риворд в список float.
        """
        if not rr_str:
            return []

        try:
            # Поддержка разных форматов: "2.0,3.0,5.0" или "2.0:3.0:5.0"
            separators = [',', ':', ';', '|']
            for sep in separators:
                if sep in rr_str:
                    parts = rr_str.split(sep)
                    vals = [float(x.strip()) for x in parts if x.strip()]
                    vals = [v for v in vals if v > 0.0]
                    vals = sorted(set(vals))
                    return vals

            # Одиночное значение
            v = float(rr_str.strip())
            return [v] if v > 0.0 else []

        except (ValueError, AttributeError):
            self.logger.warning(f"Failed to parse RR levels: {rr_str}")
            return []

    def get_min_confidence_for_symbol(self, symbol: str | None) -> float:
        """
        Получение минимального порога уверенности для символа (шкала 0..1).
        """
        return self._get_min_confidence_for_symbol(symbol)

    # Алиас для обратной совместимости, используемый SignalGenerator.generate()
    def min_confidence(self, symbol: str | None) -> float:
        return self.get_min_confidence_for_symbol(symbol)

    def signal_stream(self) -> str:
        return self._get_signal_stream()

    def signal_stream(self) -> str:
        """Имя стрима сигналов для публикации."""
        return self._get_signal_stream()

    def strategy_key(self) -> str:
        """Ключ стратегии для этой конфигурации."""
        return self._get_strategy_key()

    def source_name(self) -> str:
        """Получение имени источника для этого хендлера."""
        return self._get_source_name()

    def min_confidence(self, symbol: str | None = None) -> float:
        """Минимальный порог уверенности (шкала 0..1)."""
        return self.get_min_confidence_for_symbol(symbol or self.symbol)

    def trailing_params(self) -> Dict[str, Any]:
        """Откалиброванные параметры трейлинга."""
        return self._get_calibrated_trailing_params()

    def get_config_summary(self) -> Dict[str, Any]:
        """Сводка текущей конфигурации."""
        return {
            'symbol': self.symbol
            'source_name': self._get_source_name()
            'strategy_key': self._get_strategy_key()
            'signal_stream': self.signal_stream()
            'min_confidence': self.min_confidence(self.symbol)
            'trailing_params': self._get_calibrated_trailing_params()
        }
