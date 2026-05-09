from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
Redis Stream Consumer for MT5 Bridge

Читает ExecutionPlan из Redis Streams stream:signals:plans.
Парсит планы и предоставляет их для исполнения в MT5.
"""


import json

import redis

from .models import Mt5ExecutionPlan, plan_from_dict


class PlansStreamConsumer:
    """
    Consumer для Redis Streams - читает новые планы исполнения.

    Читает из stream:signals:plans новые сообщения с ExecutionPlan.
    Предоставляет список Mt5ExecutionPlan для обработки executor'ом.

    Формат сообщений в stream:
    {
        "signal_id": "-breakout-123"
        "symbol": ""
        "setup_type": "breakout_R1",
        "side": "long",
        "ts_signal": "2025-12-15T12:34:56.123456+00:00",
        "payload": "{ \"ctx\": {...}, \"plan\": {...} }"
    }

    Где payload - JSON строка с:
    {
        "ctx": { ... SignalContext.to_dict() ... },
        "plan": { ... ExecutionPlan dict ... }
    }
    """

    def __init__(self, redis_dsn: str, stream_key: str = RS.SIGNAL_PLANS):
        """
        Args:
            redis_dsn: Redis connection string, e.g. "redis://localhost:6379/0"
            stream_key: Название Redis stream для чтения
        """
        self._r = redis.from_url(redis_dsn, decode_responses=True)
        self.stream_key = stream_key
        # Начинаем с '$' - только новые сообщения после подключения
        self._last_id = "$"

    def poll(self, block_ms: int = 500, count: int = 10) -> list[Mt5ExecutionPlan]:
        """
        Читает новые сообщения из stream (блокирующе).

        Args:
            block_ms: Время блокировки в мс при ожидании новых сообщений
            count: Максимальное количество сообщений для чтения за раз

        Returns:
            List[Mt5ExecutionPlan]: Список новых планов для исполнения
        """
        # XREAD BLOCK <ms> COUNT <count> STREAMS key last_id
        resp = self._r.xread(
            {self.stream_key: self._last_id},
            block=block_ms,
            count=count,
        )

        plans: list[Mt5ExecutionPlan] = []

        if not resp:
            return plans

        for stream_name, messages in resp:
            for msg_id, fields in messages:
                # Обновляем last_id для следующего чтения
                self._last_id = msg_id

                payload_raw = fields.get("payload")
                if not payload_raw:
                    continue

                try:
                    # Парсим JSON payload
                    payload = json.loads(payload_raw)
                    plan_dict = payload["plan"]

                    # Конвертируем в Mt5ExecutionPlan
                    plan = plan_from_dict(plan_dict)
                    plans.append(plan)

                except json.JSONDecodeError as e:
                    print(f"[PlansStreamConsumer] JSON parse error in msg {msg_id}: {e}")
                except KeyError as e:
                    print(f"[PlansStreamConsumer] Missing key in msg {msg_id}: {e}")
                except Exception as e:
                    print(f"[PlansStreamConsumer] Unexpected error parsing msg {msg_id}: {e}")

        return plans

    def reset(self) -> None:
        """
        Сбрасывает позицию чтения на начало stream.
        Следующий poll() прочитает все сообщения с самого начала.
        """
        self._last_id = "0"

    def get_last_id(self) -> str:
        """
        Возвращает текущую позицию чтения в stream.

        Returns:
            str: ID последнего прочитанного сообщения
        """
        return self._last_id

    def set_last_id(self, last_id: str) -> None:
        """
        Устанавливает позицию чтения в stream.

        Args:
            last_id: ID сообщения, с которого продолжить чтение
        """
        self._last_id = last_id
