# -*- coding: utf-8 -*-
"""
TradeFeedAdapter — адаптер для публикации принтов в Redis Stream.
Используется для интеграции различных источников принтов (DOM, py-obi, WebSocket и т.д.)
"""

import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

import redis
from typing_extensions import TypedDict


class StatsDict(TypedDict):
    """Структура статистики адаптера."""

    trades_published: int
    errors: int
    last_trade_ts: int


def _decode_field(data: dict, key: str, default: Any = None) -> Any:
    """
    Извлекает поле из словаря Redis, поддерживая как bytes-, так и str-ключи.

    Redis-py возвращает bytes при decode_responses=False и str при decode_responses=True.
    Оба варианта встречаются на практике, поэтому нормализация необходима.

    Args:
        data:    сырой словарь сообщения Redis Stream.
        key:     имя поля (str).
        default: значение по умолчанию, если поле отсутствует.

    Returns:
        str-значение поля или default.
    """
    # Пробуем str-ключ первым (decode_responses=True — типичный случай)
    val = data.get(key)
    if val is None:
        # Пробуем bytes-ключ (decode_responses=False)
        val = data.get(key.encode())
    if val is None:
        return default
    return val.decode() if isinstance(val, (bytes, bytearray)) else str(val)


@dataclass(slots=True)
class Trade:
    """Модель принта/сделки."""

    price: float
    qty: float
    side: str  # 'buy' или 'sell' (сторона агрессора)
    ts: int    # timestamp в миллисекундах
    symbol: str


class TradeFeedAdapter:
    """
    Адаптер для публикации принтов в Redis Stream.

    Публикует в stream: trades:{SYMBOL}
    Формат: {price, qty, side, ts, symbol}
    """

    def __init__(
        self,
        r: redis.Redis,  # type: ignore[type-arg]
        symbol: str,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.r = r
        self.symbol = symbol
        self.stream_key = f"trades:{symbol}"
        self.log = logger or logging.getLogger(__name__)

        self.stats: StatsDict = {
            "trades_published": 0,
            "errors": 0,
            "last_trade_ts": 0,
        }

    def publish_trade(self, trade: Trade) -> bool:
        """
        Публикует принт в Redis Stream.

        Args:
            trade: объект Trade с данными сделки.

        Returns:
            True если успешно, False при ошибке.
        """
        try:
            fields: dict[str, str] = {
                "price": str(trade.price),
                "qty": str(trade.qty),
                "side": trade.side.lower(),
                "ts": str(trade.ts),
                "symbol": trade.symbol,
            }
            # redis-py xadd принимает dict[str, str]; стабы слишком строги
            self.r.xadd(self.stream_key, fields, maxlen=10_000)  # type: ignore[arg-type]

            self.stats["trades_published"] += 1
            self.stats["last_trade_ts"] = trade.ts
            return True

        except Exception as e:
            self.log.error("Error publishing trade: %s", e)
            self.stats["errors"] += 1
            return False

    def publish_from_dict(self, data: dict[str, Any]) -> bool:
        """
        Публикует принт из словаря.

        Args:
            data: словарь с ключами price, qty, side, ts (опционально), symbol (опционально).

        Returns:
            True если успешно, False при ошибке.
        """
        try:
            trade = Trade(
                price=float(data["price"]),
                qty=float(data.get("qty", data.get("volume", 1.0))),
                side=str(data.get("side", "buy")).lower(),
                ts=int(data.get("ts", data.get("timestamp", int(time.time() * 1000)))),
                symbol=str(data.get("symbol", self.symbol)),
            )
            return self.publish_trade(trade)
        except Exception as e:
            self.log.error("Error parsing trade data: %s", e)
            self.stats["errors"] += 1
            return False

    def get_stats(self) -> StatsDict:
        """Возвращает копию статистики адаптера."""
        return self.stats.copy()

    def reset_stats(self) -> None:
        """Сбрасывает статистику."""
        self.stats = {
            "trades_published": 0,
            "errors": 0,
            "last_trade_ts": 0,
        }


class TradeStreamReader:
    """
    Читатель принтов из Redis Stream.
    Используется для тестирования и отладки.
    """

    def __init__(
        self,
        r: redis.Redis,  # type: ignore[type-arg]
        symbol: str,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.r = r
        self.symbol = symbol
        self.stream_key = f"trades:{symbol}"
        self.log = logger or logging.getLogger(__name__)
        self.last_id: str = "0-0"

    def read_trades(self, count: int = 100, block_ms: int = 0) -> list[Trade]:
        """
        Читает новые принты из stream.

        Args:
            count:    максимальное количество сообщений.
            block_ms: таймаут блокировки (0 = без блокировки).

        Returns:
            список объектов Trade.
        """
        trades: list[Trade] = []

        try:
            result: list | None = self.r.xread(  # type: ignore[assignment]
                {self.stream_key: self.last_id},
                count=count,
                block=block_ms,
            )
            if not result:
                return trades

            for _stream_name, messages in result:
                for msg_id, data in messages:
                    try:
                        trade = Trade(
                            price=float(_decode_field(data, "price", 0)),
                            qty=float(_decode_field(data, "qty", 0)),
                            side=_decode_field(data, "side", "").lower(),
                            ts=int(_decode_field(data, "ts", 0)),
                            symbol=_decode_field(data, "symbol", self.symbol),
                        )
                        trades.append(trade)
                    except Exception as e:
                        self.log.warning("Error parsing trade: %s", e)

                    self.last_id = (
                        msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else msg_id
                    )

        except Exception as e:
            self.log.error("Error reading trades stream: %s", e)

        return trades


# ============================================================================
# Пример использования
# ============================================================================

if __name__ == "__main__":
    import os

    logging.basicConfig(level=logging.INFO)
    _logger = logging.getLogger("trade_feed")

    # Подключение к Redis
    _r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))  # type: ignore[assignment]
    try:
        _r.ping()  # type: ignore
    except redis.ConnectionError as _e:
        _logger.error("Cannot connect to Redis: %s", _e)
        raise SystemExit(1) from _e

    # Создаём адаптер
    _adapter = TradeFeedAdapter(_r, "XAUUSD", _logger)  # type: ignore[arg-type]

    # Симуляция принтов
    _logger.info("Симуляция потока принтов...")

    _base_price = 2650.0
    for _i in range(10):
        _trade = Trade(
            price=_base_price + _i * 0.1,
            qty=1.5 + (_i % 3) * 0.5,
            side="buy" if _i % 2 == 0 else "sell",
            ts=int(time.time() * 1000) + _i * 1000,
            symbol="XAUUSD",
        )
        _success = _adapter.publish_trade(_trade)
        _logger.info("Published trade %d: %s %.2f@%.2f — %s", _i + 1, _trade.side, _trade.qty, _trade.price, _success)
        time.sleep(0.1)

    # Статистика
    _stats = _adapter.get_stats()
    _logger.info("Stats: %s", _stats)

    # Чтение обратно
    _logger.info("\nЧтение принтов из stream...")
    _reader = TradeStreamReader(_r, "XAUUSD", _logger)  # type: ignore[arg-type]
    _read_trades = _reader.read_trades(count=20)

    _logger.info("Read %d trades:", len(_read_trades))
    for _t in _read_trades:
        _logger.info("  %s %.2f@%.2f at %d", _t.side, _t.qty, _t.price, _t.ts)
