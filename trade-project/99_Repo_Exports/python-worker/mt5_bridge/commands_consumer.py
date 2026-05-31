from __future__ import annotations

"""
Commands Consumer - обработка команд из Redis Streams

Слушает stream:signals:exec_events и реагирует на команды,
такие как CLOSE_REQUEST для принудительного закрытия позиций.
"""


import json

import MetaTrader5 as mt5
import redis

from .mt5_client import Mt5Client
from core.redis_keys import RedisStreams as RS


class ExecCommandsConsumer:
    """
    Слушает stream:signals:exec_events и реагирует только на записи с kind='command'
    и venue='mt5' — например, CLOSE_REQUEST по конкретному signal_id.

    Позволяет глобальному риск-менеджеру принудительно закрывать позиции
    на MT5 раньше тейков/стопов.
    """

    def __init__(self, redis_dsn: str, mt5_client: Mt5Client, stream_key: str = RS.SIGNAL_EXEC_EVENTS):
        """
        Args:
            redis_dsn: Redis connection string
            mt5_client: Подключенный MT5 клиент
            stream_key: Название stream для чтения команд
        """
        self._r = redis.from_url(redis_dsn, decode_responses=True)
        self._mt5 = mt5_client
        self.stream_key = stream_key
        self._last_id = "$"  # Начинаем с новых сообщений

    def _close_positions_for_signal(self, signal_id: str) -> None:
        """
        Закрываем все позиции, у которых в comment есть "sig=<signal_id>".

        Args:
            signal_id: ID сигнала для закрытия позиций
        """
        positions = mt5.positions_get()
        if positions is None:
            return

        for pos in positions:
            comment = pos.comment or ""
            if f"sig={signal_id}" not in comment:
                continue

            # Определяем направление для закрытия (обратное открытой позиции)
            is_buy = pos.type == mt5.POSITION_TYPE_BUY
            close_volume = pos.volume

            symbol = pos.symbol

            # Для закрытия: открываем обратную сделку тем же объёмом
            try:
                self._mt5.send_market_order(
                    symbol=symbol,
                    is_buy=not is_buy,  # обратное направление
                    volume_lots=close_volume,
                    sl_price=None,
                    tp_price=None,
                    comment=f"sig={signal_id} close",
                )
            except Exception as e:
                # Логируем ошибку, но продолжаем
                print(f"[CommandsConsumer] Error closing position for {signal_id}: {e}")

    def step(self) -> None:
        """
        Один шаг обработки команд.

        Читает новые сообщения из stream и обрабатывает команды.
        """
        resp = self._r.xread({self.stream_key: self._last_id}, block=100, count=20)
        if not resp:
            return

        for stream_name, messages in resp:
            for msg_id, fields in messages:
                self._last_id = msg_id
                payload_raw = fields.get("payload")
                if not payload_raw:
                    continue

                try:
                    payload = json.loads(payload_raw)
                except json.JSONDecodeError:
                    continue

                # Проверяем что это команда для MT5
                if payload.get("venue") != "mt5":
                    continue
                if payload.get("kind") != "command":
                    continue

                event_type = payload.get("event_type")
                signal_id = payload.get("signal_id")

                if event_type == "CLOSE_REQUEST" and signal_id:
                    self._close_positions_for_signal(signal_id)
