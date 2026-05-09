"""
ATR (Average True Range) расчет для анализа волатильности.

ФУНКЦИОНАЛ:
- ATR(14) расчет на основе тиковых данных
- Агрегация тиков в 1-минутные бары
- Online расчет без необходимости хранения полной истории

ИСПОЛЬЗОВАНИЕ:
- atr = ATR(period=14)
- atr.feed_tick(price, timestamp_ms)
- value = atr.value()
"""

from collections import deque
from dataclasses import dataclass


@dataclass
class Bar:
    """Свеча (бар) для расчета ATR."""
    t_open: int      # Время открытия (мс)
    o: float         # Open
    h: float         # High
    l: float         # Low
    c: float         # Close


class ATR:
    """
    Класс для расчета ATR (Average True Range).
    
    Принимает тиковые данные, агрегирует их в минутные бары
    и вычисляет ATR на основе этих баров.
    """

    def __init__(self, period: int = 14):
        """
        Инициализация ATR.
        
        Args:
            period: Период для расчета ATR (по умолчанию 14)
        """
        self.period = period
        self.prev_close = None
        self.window = deque(maxlen=period)
        self._value = None

        # Для агрегации тиков в минутные бары
        self._current_bar = None

    def feed_tick(self, price: float, ts: int) -> None:
        """
        Обработка тика для расчета ATR.
        
        Args:
            price: Цена тика
            ts: Timestamp в миллисекундах
        """
        # Определяем минуту текущего тика
        minute = ts // 60_000

        if self._current_bar is None or minute != self._current_bar.t_open // 60_000:
            # Новая минута - закрываем предыдущий бар и создаем новый
            if self._current_bar is not None:
                self._on_close_bar(self._current_bar)

            self._current_bar = Bar(
                t_open=minute * 60_000,
                o=price,
                h=price,
                l=price,
                c=price
            )
        else:
            # Обновляем текущий бар
            self._current_bar.h = max(self._current_bar.h, price)
            self._current_bar.l = min(self._current_bar.l, price)
            self._current_bar.c = price

    def update(self, high: float, low: float, close: float) -> float | None:
        """
        Обновление ATR данными закрытого бара.
        Compatible with data_processor usage.
        """
        bar = Bar(t_open=0, o=0.0, h=high, l=low, c=close)
        self._on_close_bar(bar)
        return self._value


    def _on_close_bar(self, bar: Bar) -> None:
        """
        Обработка закрытого бара для расчета True Range.
        
        Args:
            bar: Закрытый бар
        """
        # Вычисляем True Range
        if self.prev_close is None:
            # Первый бар - TR = High - Low
            tr = bar.h - bar.l
        else:
            # True Range = max(H-L, |H-prevC|, |L-prevC|)
            tr = max(
                bar.h - bar.l,
                abs(bar.h - self.prev_close),
                abs(bar.l - self.prev_close)
            )

        self.prev_close = bar.c
        self.window.append(tr)

        # Вычисляем ATR когда накопилось достаточно данных
        if len(self.window) == self.window.maxlen:
            self._value = sum(self.window) / len(self.window)

    def value(self) -> float:
        """
        Получить текущее значение ATR.
        
        Returns:
            Значение ATR или None, если недостаточно данных
        """
        return self._value

    def is_ready(self) -> bool:
        """
        Проверка готовности ATR (достаточно ли данных).
        
        Returns:
            True, если ATR готов к использованию
        """
        return self._value is not None

    def reset(self) -> None:
        """Сброс всех данных ATR."""
        self.prev_close = None
        self.window.clear()
        self._value = None
        self._current_bar = None

