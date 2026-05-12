"""
MT5 Trailing Move Logger - Логирование движений trailing stop из MT5.

Этот модуль должен вызываться из:
1. MT5 EA - при каждом движении trailing stop
2. Paper Executor - при симуляции трейлинга
3. Backtest - при бэктестировании

Использование в MT5 EA:
```mql5
// При движении trailing stop
if (new_sl != old_sl) {
    PublishTrailingMove(signal_id, new_sl, current_price);
}
```

Использование в Python:
```python
from typing import Dict
from typing import Dict
from typing import Dict
from services.mt5_trailing_move_logger import MT5TrailingMoveLogger

logger = MT5TrailingMoveLogger()
logger.log_move(
    sid="signal--123",
    symbol="",
    new_sl=2771.4,
    current_price=2776.5,
    profile="rocket_v1",
)
```
"""

import time

from common.log import setup_logger
from services.trade_events_logger import TradeEventsLogger

log = setup_logger("mt5_trailing_move_logger")


class MT5TrailingMoveLogger:
    """
    Логгер движений trailing stop для анализа trade_back.
    
    Записывает каждое движение SL для последующего анализа:
    - Как далеко удалось утащить
    - Эффективность профилей
    - Статистика по волатильности
    """

    def __init__(self, redis_url: str | None = None):
        """
        Args:
            redis_url: URL Redis (если None, берётся из REDIS_URL env)
        """
        self.events_logger = TradeEventsLogger(redis_url)
        self.redis_url = self.events_logger.redis_url
        self.r = self.events_logger.r

        # Кэш для отслеживания последнего SL (чтобы не дублировать)
        self.last_sl_cache = {}  # {sid: last_sl}

        log.info("✅ MT5TrailingMoveLogger initialized")

    def log_move(
        self,
        sid: str,
        symbol: str,
        new_sl: float,
        current_price: float | None = None,
        profile: str = "unknown",
        position_id: str | None = None,
        atr: float | None = None
    ) -> bool:
        """
        Записать движение trailing stop.
        
        Args:
            sid: Signal ID
            symbol: Символ
            new_sl: Новый уровень SL
            current_price: Текущая рыночная цена
            profile: Профиль трейлинга
            position_id: ID позиции MT5
            atr: Текущий ATR
            
        Returns:
            True если записано, False если skip (duplicate)
        """
        # Проверяем дубликат
        last_sl = self.last_sl_cache.get(sid)
        if last_sl is not None and abs(new_sl - last_sl) < 0.01:
            # SL не изменился или изменился незначительно
            return False

        # Получаем исходный сигнал для расчёта distance_from_entry
        distance_from_entry = None
        try:
            signal_key = f"signals:{sid}"
            signal_data = self.r.get(signal_key)
            if signal_data:
                import json
                signal = json.loads(signal_data)  # type: ignore
                entry = signal.get("entry")
                side = signal.get("side")

                if entry and side:
                    if side == "LONG":
                        distance_from_entry = new_sl - entry
                    else:
                        distance_from_entry = entry - new_sl
        except Exception as e:
            log.debug("Could not calculate distance_from_entry: %s", e)

        # Логируем событие
        success = self.events_logger.log_trailing_move(
            sid=sid,
            symbol=symbol,
            new_sl=new_sl,
            current_price=current_price,
            profile=profile,
            position_id=position_id,
            distance_from_entry=distance_from_entry,
            atr=atr
        )

        if success:
            # Обновляем кэш
            self.last_sl_cache[sid] = new_sl

            log.info(
                "📈 Trailing move: sid=%s new_sl=%.2f distance=%.2f profile=%s",
                sid, new_sl, distance_from_entry or 0.0, profile
            )

        return success  # type: ignore

    def get_trailing_distance(self, sid: str) -> float | None:
        """
        Получить максимальное расстояние, на которое удалось утащить SL.
        
        Args:
            sid: Signal ID
            
        Returns:
            Максимальное расстояние от entry или None
        """
        try:
            trailing_history = self.events_logger.get_trailing_history(sid)
            if not trailing_history:
                return None

            max_distance = 0.0
            for event in trailing_history:
                metadata = event.get("metadata", {})
                if isinstance(metadata, str):
                    import json
                    metadata = json.loads(metadata)

                distance = metadata.get("distance_from_entry")
                if distance and distance > max_distance:
                    max_distance = distance

            return max_distance if max_distance > 0 else None

        except Exception as e:
            log.error("Failed to get trailing distance for %s: %s", sid, str(e))
            return None

    def get_trailing_stats(self, sid: str) -> dict | None:
        """
        Получить статистику trailing для сигнала.
        
        Args:
            sid: Signal ID
            
        Returns:
            Dict со статистикой или None
        """
        try:
            trailing_history = self.events_logger.get_trailing_history(sid)
            if not trailing_history:
                return None

            sl_values = []
            for event in trailing_history:
                new_sl = event.get("new_sl")
                if new_sl:
                    sl_values.append(new_sl)

            if not sl_values:
                return None

            return {
                "moves_count": len(sl_values),
                "first_sl": sl_values[0],
                "last_sl": sl_values[-1],
                "max_sl": max(sl_values),
                "min_sl": min(sl_values),
                "total_movement": sl_values[-1] - sl_values[0],
                "avg_sl": sum(sl_values) / len(sl_values)
            }

        except Exception as e:
            log.error("Failed to get trailing stats for %s: %s", sid, str(e))
            return None


if __name__ == "__main__":
    # Тестирование

    logger = MT5TrailingMoveLogger()

    test_sid = f"test-trailing-{int(time.time())}"

    print(f"\n=== Testing MT5TrailingMoveLogger with {test_sid} ===\n")

    # Создаём тестовый сигнал
    import json
    signal = {
        "sid": test_sid,
        "symbol": "",
        "side": "LONG",
        "entry": 2765.5,
        "sl": 2758.7,
        "tp_levels": [2769.9, 2773.1, 2776.3]
    }
    logger.r.set(f"signals:{test_sid}", json.dumps(signal), ex=3600)

    # Симулируем движения trailing
    print("Симулируем движения trailing stop...\n")

    moves = [
        (2762.0, 2772.0),  # SL подтянут до 2762.0, цена на 2772.0
        (2764.5, 2774.5),  # SL → 2764.5
        (2767.2, 2777.2),  # SL → 2767.2
        (2769.0, 2779.0),  # SL → 2769.0 (почти на уровне entry!)
        (2770.5, 2780.5),  # SL → 2770.5 (уже в прибыли!)
    ]

    for i, (new_sl, current_price) in enumerate(moves, 1):
        success = logger.log_move(
            sid=test_sid,
            symbol="",
            new_sl=new_sl,
            current_price=current_price,
            profile="rocket_v1",
            atr=2.5
        )

        if success:
            distance = new_sl - signal["entry"]
            print(f"Move {i}: SL={new_sl:.2f} price={current_price:.2f} distance={distance:+.2f}")

        time.sleep(0.1)

    # Показываем результаты
    print("\n=== Trailing History ===")
    history = logger.events_logger.get_trailing_history(test_sid)
    print(f"Total moves: {len(history)}")

    for i, event in enumerate(history, 1):
        metadata = event.get("metadata", {})
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        distance = metadata.get("distance_from_entry", 0)
        print(f"{i}. SL={event['new_sl']:.2f} distance={distance:+.2f}")

    # Статистика
    print("\n=== Trailing Stats ===")
    stats = logger.get_trailing_stats(test_sid)
    if stats:
        print(json.dumps(stats, indent=2))

    # Максимальное расстояние
    max_dist = logger.get_trailing_distance(test_sid)
    print(f"\nMax distance from entry: {max_dist:+.2f} pips")

    print("\n✅ Test complete")

