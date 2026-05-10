import json
import logging
from typing import Any

from common.normalization import generate_signal_id, normalize_side_3
from core.redis_keys import RedisStreams as RS
from redis.exceptions import RedisError

logger = logging.getLogger("crypto_order_payload_builder")

class OrderPayloadBuilder:
    def __init__(self, facade: Any):
        self.facade = facade
        self.redis = facade.redis
        self.orders_queue_mt5 = facade.orders_queue_mt5
        self.orders_queue_binance = facade.orders_queue_binance

    async def publish_orders_queue(self, runtime: Any, signal: dict[str, Any]) -> None:
        """
        Публикует команду в очередь ордеров (MT5=Stream, Binance=List).
        Схема: order_creation.md (минимально необходимый payload).
        """
        symbol = signal.get("symbol") or runtime.symbol
        ts_value = signal.get("tick_ts") or signal.get("ts_event_ms") or signal.get("generated_at")
        if not ts_value:
            logger.warning("⚠️ (%s) Нет временной метки сигнала, пропускаем orders:queue", runtime.symbol)
            return

        # Unified side normalization (P0)
        side_norm = normalize_side_3(signal.get("direction") or signal.get("side") or "")
        direction = side_norm.side.value.lower() # buy/sell
        venue = (signal.get("venue") or "mt5").lower()

        reason = signal.get("reason") or "delta_spike"

        # Signal ID generation (P0)
        signal_id = generate_signal_id(
            kind=(signal.get("kind") or "spike"),
            symbol=symbol,
            ts_ms=int(ts_value),
            direction=side_norm.direction.value
        )

        order_cmd = {
            "id": f"order-{symbol}-{ts_value}",
            "sid": signal_id,
            "signal_id": signal_id,
            "symbol": symbol,
            "type": "market",
            "direction": direction,
            "side": side_norm.side.value,
            "side_int": side_norm.side_int,
            "source": "CryptoOrderFlow",
            "venue": venue,
            "reason": reason,
        }

        try:
            if venue == "mt5":
                if not self.orders_queue_mt5:
                    logger.warning("⚠️ (%s) orders_queue_mt5 не задан, пропуск", runtime.symbol)
                    return
                # MT5 uses Redis Stream
                await self.redis.xadd(self.orders_queue_mt5, {k: str(v) for k, v in order_cmd.items()}, maxlen=1000, approximate=True)
            else:
                # Binance uses Redis List
                queue = self.orders_queue_binance or RS.ORDERS_QUEUE_BINANCE
                _ = await self.redis.lpush(queue, json.dumps(order_cmd)) # type: ignore[misc]
        except RedisError as exc:
            logger.warning("⚠️ (%s) Не удалось отправить в очередь ордеров (%s): %s", runtime.symbol, venue, exc)
