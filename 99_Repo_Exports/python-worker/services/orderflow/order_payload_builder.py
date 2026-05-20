import json
import logging
from typing import Any

from common.normalization import generate_signal_id, normalize_side_3
from core.mt5_kill_switch import mt5_enabled
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
        try:
            side_norm = normalize_side_3(signal.get("direction") or signal.get("side") or "")
        except ValueError:
            logger.warning("⚠️ (%s) unknown direction=%r side=%r — skip orders:queue",
                           symbol, signal.get("direction"), signal.get("side"))
            return
        direction = side_norm.side.value.lower()  # buy/sell
        # Default venue switched mt5→binance (2026-05-19): no MT5 consumer is
        # deployed; signals without explicit venue were piling into
        # orders:queue:mt5 unread (PEL/maxlen growth). MT5 path remains
        # available behind MT5_ENABLED=1 — see core/mt5_kill_switch.py.
        venue = (signal.get("venue") or "binance").lower()

        reason = signal.get("reason") or "delta_spike"

        # Prefer existing sid from upstream pipeline (preserves of:SYM:TS:LONG format).
        # Fall back to generate only when the signal has no identity yet.
        signal_id = (
            signal.get("sid")
            or signal.get("signal_id")
            or generate_signal_id(
                kind=(signal.get("kind") or "spike"),
                symbol=symbol,
                ts_ms=int(ts_value),
                direction=side_norm.direction.value,
            )
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
                if not mt5_enabled():
                    # MT5 path is disabled (MT5_ENABLED=0 — default).  Silently
                    # drop the publish; the code path itself is preserved so
                    # `MT5_ENABLED=1` restores the original behaviour.
                    logger.info(
                        "ℹ️ (%s) MT5 venue requested but MT5_ENABLED=0 — order publish skipped (sid=%s)",
                        runtime.symbol, signal_id,
                    )
                    return
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
