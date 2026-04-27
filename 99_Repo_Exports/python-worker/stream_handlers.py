#!/usr/bin/env python3
"""
Обработчики сообщений стримов (универсальные хендлеры для StreamConsumer).

Назначение:
- Инкапсулируют логику обработки сообщений из различных стримов (volatility, топы, новые пары и т.д.).
- Вызываются универсальным потребителем `stream_consumer.StreamConsumer`.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any

log = logging.getLogger(__name__)

# Message types recognised by this handler.
_KNOWN_TYPES = frozenset({
    "volatilityRange",
    "volatility",
    "volatilitySpike",
    "top-gainers",
    "top-losers",
    "ws-new-pairs",
    "bulk",
})


class StreamMessageHandler:
    """Обработчик сообщений из Redis Streams."""

    __slots__ = ("message_count",)

    def __init__(self) -> None:
        """Инициализация обработчика сообщений."""
        self.message_count: int = 0

    def process_stream_message(
        self,
        stream_name: str,
        message_id: str,
        fields: Dict[str, str],
    ) -> None:
        """Обработка полученного сообщения из стрима.

        Args:
            stream_name: Имя стрима.
            message_id: ID сообщения.
            fields: Поля сообщения.
        """
        try:
            if "data" not in fields:
                print(f"⚠️ Сообщение {message_id} не содержит поле 'data'")
                return

            message_data: Any = json.loads(fields["data"])

            if isinstance(message_data, list):
                print(f"ℹ️ Сообщение {message_id} содержит массив из {len(message_data)} элементов")
                message_data = {
                    "type": "bulk",
                    "items": message_data,
                    "count": len(message_data),
                }
            elif not isinstance(message_data, dict):
                print(f"⚠️ Неожиданный тип данных в 'data': {type(message_data).__name__}")
                message_data = {"type": "unknown", "raw": message_data}

            message_type: str = message_data.get("type", "unknown")
            self.message_count += 1

            self._print_message_info(stream_name, message_id, message_data, message_type)
            self._handle_specific_message_type(message_type, message_data)

            if self.message_count % 10 == 0:
                print(f"📊 Обработано сообщений: {self.message_count}")

        except json.JSONDecodeError as exc:
            print(f"❌ Ошибка парсинга JSON в сообщении {message_id}: {exc}")
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка обработки сообщения {message_id}: {exc}")
            log.exception("process_stream_message unexpected error msg_id=%s", message_id)

    def _print_message_info(
        self,
        stream_name: str,
        message_id: str,
        message_data: Dict[str, Any],
        message_type: str,
    ) -> None:
        """Вывод основной информации о сообщении."""
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        symbol = message_data.get("symbol", "N/A")

        print(f"\n🚨 [{timestamp}] СООБЩЕНИЕ ИЗ СТРИМА:")
        print(f"   📡 Стрим: {stream_name}")
        print(f"   🆔 ID: {message_id}")
        print(f"   🔍 Тип: {message_type}")
        print(f"   💱 Символ: {symbol}")

    def _handle_specific_message_type(
        self,
        message_type: str,
        message_data: Dict[str, Any],
    ) -> None:
        """Диспетчер типов сообщений."""
        if message_type == "volatilityRange":
            self._handle_volatility_range(message_data)
        elif message_type in ("volatility", "volatilitySpike"):
            self._handle_volatility_spike(message_data)
        elif message_type == "top-gainers":
            self._handle_gainers(message_data)
        elif message_type == "top-losers":
            self._handle_losers(message_data)
        elif message_type == "ws-new-pairs":
            self._handle_new_pairs(message_data)
        elif message_type == "bulk":
            self._handle_bulk(message_data)
        # Unknown types are silently ignored — already logged by caller via print.

    # ------------------------------------------------------------------
    # Specific handlers
    # ------------------------------------------------------------------
    def _handle_volatility_range(self, message_data: Dict[str, Any]) -> None:
        """Обработка сигнала волатильности по диапазону."""
        print(f"   📊 Диапазон: {message_data.get('range', 'N/A')}")
        print(f"   📈 Средний диапазон: {message_data.get('avgRange', 'N/A')}")
        print(f"   ⚡ Волатильность: {message_data.get('volatility', 'N/A')}%")

    def _handle_volatility_spike(self, message_data: Dict[str, Any]) -> None:
        """Обработка сигнала всплеска волатильности."""
        print(f"   ⚡ Волатильность: {message_data.get('volatility', 'N/A')}%")
        print(f"   🎯 Порог: {message_data.get('threshold', 'N/A')}%")

    def _handle_gainers(self, message_data: Dict[str, Any]) -> None:
        """Обработка сигнала растущих активов."""
        print(f"   📈 Изменение: {message_data.get('priceChangePercent', 'N/A')}%")
        print(f"   📊 Объем: {message_data.get('volume', 'N/A')}")

    def _handle_losers(self, message_data: Dict[str, Any]) -> None:
        """Обработка сигнала падающих активов."""
        print(f"   📉 Изменение: {message_data.get('priceChangePercent', 'N/A')}%")
        print(f"   📊 Объем: {message_data.get('volume', 'N/A')}")

    def _handle_new_pairs(self, message_data: Dict[str, Any]) -> None:
        """Обработка сигнала новых торговых пар."""
        pairs = message_data.get("pairs", [])
        count = len(pairs) if isinstance(pairs, list) else 0
        print(f"   🆕 Новых пар: {count}")
        for pair in pairs[:5]:
            print(f"      • {pair}")
        if count > 5:
            print(f"      • ... и еще {count - 5} пар")

    def _handle_bulk(self, message_data: Dict[str, Any]) -> None:
        """Обработка bulk-сообщений (когда 'data' пришёл массивом)."""
        items = message_data.get("items", [])
        count = message_data.get("count", len(items))
        print(f"   📦 Bulk-сообщение: {count} элементов")
        for idx, item in enumerate(items[:3], start=1):
            preview = item if isinstance(item, dict) else str(item)
            print(f"      {idx}. {str(preview)[:200]}")

    # ------------------------------------------------------------------
    # Counter helpers
    # ------------------------------------------------------------------
    def get_message_count(self) -> int:
        """Возвращает количество обработанных сообщений."""
        return self.message_count

    def reset_message_count(self) -> None:
        """Сбрасывает счетчик сообщений."""
        self.message_count = 0