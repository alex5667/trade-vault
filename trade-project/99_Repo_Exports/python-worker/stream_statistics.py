#!/usr/bin/env python3
"""
Stream Statistics
Модуль для сбора и отображения статистики Redis Streams Consumer.
"""

import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any

log = logging.getLogger(__name__)


class StreamStatistics:
    """Класс для сбора и отображения статистики обработки стримов."""

    __slots__ = ("_total_messages", "_messages_by_stream", "_last_message_time",
                 "_errors", "_start_time")

    def __init__(self) -> None:
        """Инициализация статистики."""
        self._total_messages: int = 0
        self._messages_by_stream: Dict[str, int] = {}
        self._last_message_time: float | None = None
        self._errors: int = 0
        self._start_time: float = time.time()

    # ------------------------------------------------------------------
    # Mutators
    # ------------------------------------------------------------------
    def update_stats(self, stream_name: str, message_id: str) -> None:  # noqa: ARG002
        """Обновление статистики при получении сообщения.

        Args:
            stream_name: Имя стрима.
            message_id: ID сообщения (используется для будущего per-id трекинга).
        """
        self._total_messages += 1
        self._messages_by_stream[stream_name] = (
            self._messages_by_stream.get(stream_name, 0) + 1
        )
        self._last_message_time = time.time()

    def increment_errors(self) -> None:
        """Увеличение счетчика ошибок."""
        self._errors += 1

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------
    def get_total_messages(self) -> int:
        """Возвращает общее количество сообщений."""
        return self._total_messages

    def get_errors_count(self) -> int:
        """Возвращает количество ошибок."""
        return self._errors

    def get_messages_by_stream(self) -> Dict[str, int]:
        """Возвращает копию статистики по стримам."""
        return dict(self._messages_by_stream)

    def get_last_message_time(self) -> float | None:
        """Возвращает время последнего сообщения (UNIX timestamp)."""
        return self._last_message_time

    def get_uptime(self) -> float:
        """Возвращает время работы в секундах."""
        return time.time() - self._start_time

    def get_messages_per_second(self) -> float:
        """Возвращает количество сообщений в секунду."""
        uptime = self.get_uptime()
        return self._total_messages / uptime if uptime > 0 else 0.0

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------
    def print_stats(self) -> None:
        """Вывод статистики (через print для видимости в operator-логах)."""
        print(f"\n📊 СТАТИСТИКА СТРИМОВ:")
        print(f"   📈 Всего сообщений: {self._total_messages}")
        print(f"   ❌ Ошибок: {self._errors}")

        if self._messages_by_stream:
            print("   📋 По стримам:")
            for stream_name, count in self._messages_by_stream.items():
                print(f"      • {stream_name}: {count}")

        if self._last_message_time is not None:
            dt = datetime.fromtimestamp(self._last_message_time, tz=timezone.utc)
            print(f"   🕐 Последнее сообщение: {dt.strftime('%H:%M:%S UTC')}")

        uptime = self.get_uptime()
        print(f"   ⏱️ Время работы: {self._format_uptime(uptime)}")

        mps = self.get_messages_per_second()
        if mps > 0:
            print(f"   🚀 Сообщений/сек: {mps:.2f}")

        if self._total_messages > 0:
            error_pct = self._errors / self._total_messages * 100
            print(f"   ⚠️ Процент ошибок: {error_pct:.2f}%")

    @staticmethod
    def _format_uptime(seconds: float) -> str:
        """Форматирование времени работы."""
        if seconds < 60:
            return f"{seconds:.0f}с"
        if seconds < 3600:
            return f"{seconds / 60:.0f}м"
        return f"{seconds / 3600:.1f}ч"

    # ------------------------------------------------------------------
    # Reset / Summary
    # ------------------------------------------------------------------
    def reset_stats(self) -> None:
        """Сброс статистики."""
        self._total_messages = 0
        self._messages_by_stream = {}
        self._last_message_time = None
        self._errors = 0
        self._start_time = time.time()

    def get_stats_summary(self) -> Dict[str, Any]:
        """Возвращает краткую сводку статистики (backward-compatible)."""
        return {
            "total_messages": self._total_messages,
            "errors": self._errors,
            "uptime": self.get_uptime(),
            "messages_per_second": self.get_messages_per_second(),
            "streams_count": len(self._messages_by_stream),
        }