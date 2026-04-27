#!/usr/bin/env python3
"""
Stream Utils — Утилиты для работы с Redis Streams.

Содержит вспомогательные методы для:
- создания consumer group-ов сразу для нескольких стримов,
- проверки и логирования pending-сообщений (диагностическая утилита),
- форматирования сообщений для логов,
- получения информации о стриме и обрезки стрима,
- проверки соединения с Redis.
"""

import json
import logging
import time
from typing import Dict, Any, List, Optional

log = logging.getLogger(__name__)


class StreamUtils:
    """Утилиты для работы с Redis Streams."""

    @staticmethod
    def create_consumer_groups(
        redis_client: Any,
        streams: List[str],
        consumer_group: str,
    ) -> bool:
        """Создание consumer groups для всех стримов.

        Args:
            redis_client: Клиент Redis.
            streams: Список стримов.
            consumer_group: Имя группы потребителей.

        Returns:
            True если все группы созданы успешно.
        """
        import redis as _redis  # local import to avoid hard dependency at module level

        success = True
        for stream_name in streams:
            max_retries = 30  # ~2.5 minutes with linear backoff
            for retry in range(max_retries):
                try:
                    redis_client.xgroup_create(
                        stream_name,
                        consumer_group,
                        id="$",       # only new messages
                        mkstream=True,
                    )
                    print(f"✅ Consumer group {consumer_group} создана для стрима {stream_name}")
                    break
                except _redis.exceptions.ResponseError as exc:
                    err = str(exc)
                    if "BUSYGROUP" in err:
                        print(f"ℹ️ Consumer group {consumer_group} уже существует для стрима {stream_name}")
                        break
                    if "Redis is loading the dataset in memory" in err:
                        wait = min(5 * (retry + 1), 30)
                        print(
                            f"⚠️ Redis загружает данные (попытка {retry + 1}/{max_retries}), "
                            f"ждём {wait} сек..."
                        )
                        time.sleep(wait)
                        continue
                    print(f"❌ Ошибка создания consumer group для {stream_name}: {exc}")
                    success = False
                    break
                except Exception as exc:  # noqa: BLE001
                    print(f"❌ Неожиданная ошибка при создании consumer group для {stream_name}: {exc}")
                    log.exception("create_consumer_groups unexpected error stream=%s", stream_name)
                    success = False
                    break
            else:
                # exhausted all retries
                print(
                    f"❌ Не удалось создать consumer group для {stream_name} "
                    f"после {max_retries} попыток: Redis всё ещё загружает данные"
                )
                success = False

        return success

    @staticmethod
    def process_pending_messages(
        redis_client: Any,
        stream_name: str,
        consumer_group: str,
    ) -> None:
        """Диагностическая утилита: отображает pending-сообщения стрима.

        NOTE: Не вызывается из горячего пути (StreamConsumer). Используется
        для ручной диагностики через CLI или управляющие скрипты.

        Args:
            redis_client: Клиент Redis.
            stream_name: Имя стрима.
            consumer_group: Имя группы потребителей.
        """
        try:
            print("🔄 Проверка pending сообщений...")
            pending = redis_client.xpending_range(stream_name, consumer_group, "-", "+", 100)
            if pending:
                print(f"📦 Найдено {len(pending)} pending сообщений")
                for msg in pending:
                    print(f"   • ID: {msg['message_id']}, Время: {msg['time_since_delivered']}мс")
            else:
                print("✅ Pending сообщений не найдено")
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка при обработке pending сообщений: {exc}")

    @staticmethod
    def validate_stream_data(data: Dict[str, Any]) -> bool:
        """Базовая валидация полей сообщения стрима.

        Args:
            data: Данные для валидации.

        Returns:
            True если данные валидны.
        """
        return isinstance(data, dict) and "data" in data

    @staticmethod
    def format_message_for_logging(message_id: str, fields: Dict[str, Any]) -> str:
        """Форматирование сообщения для удобного логирования.

        Args:
            message_id: ID сообщения.
            fields: Поля сообщения.

        Returns:
            Отформатированная строка для логирования.
        """
        try:
            message_type = "unknown"
            symbol = "N/A"
            if "data" in fields:
                data = json.loads(fields["data"])
                message_type = data.get("type", "unknown")
                symbol = data.get("symbol", "N/A")
            return f"ID: {message_id}, Type: {message_type}, Symbol: {symbol}"
        except Exception as exc:  # noqa: BLE001
            return f"ID: {message_id}, Error: {exc}"

    @staticmethod
    def get_stream_info(redis_client: Any, stream_name: str) -> Optional[Dict[str, Any]]:
        """Получение информации о стриме (XINFO STREAM).

        Args:
            redis_client: Клиент Redis.
            stream_name: Имя стрима.

        Returns:
            Информация о стриме или None.
        """
        try:
            return redis_client.xinfo_stream(stream_name)
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка получения информации о стриме {stream_name}: {exc}")
            return None

    @staticmethod
    def trim_stream(redis_client: Any, stream_name: str, max_len: int) -> bool:
        """Обрезка стрима до указанного размера (XTRIM ~ MAXLEN).

        Args:
            redis_client: Клиент Redis.
            stream_name: Имя стрима.
            max_len: Максимальная длина стрима.

        Returns:
            True если обрезка успешна.
        """
        try:
            redis_client.xtrim(stream_name, maxlen=max_len, approximate=True)
            print(f"🧹 Стрим {stream_name} обрезан до {max_len} сообщений")
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка обрезки стрима {stream_name}: {exc}")
            return False

    @staticmethod
    def check_redis_connection(redis_client: Any) -> bool:
        """Проверка подключения к Redis (PING).

        Args:
            redis_client: Клиент Redis.

        Returns:
            True если подключение активно.
        """
        try:
            redis_client.ping()
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка подключения к Redis: {exc}")
            return False

    @staticmethod
    def format_stream_list(streams: List[str]) -> str:
        """Форматирование списка стримов для вывода в лог.

        Args:
            streams: Список стримов.

        Returns:
            Отформатированная строка.
        """
        if not streams:
            return "нет"
        if len(streams) <= 3:
            return ", ".join(streams)
        return f"{', '.join(streams[:3])} и еще {len(streams) - 3}"