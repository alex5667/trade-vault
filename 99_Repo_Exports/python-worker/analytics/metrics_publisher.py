from __future__ import annotations

"""
Metrics Publisher - Публикация метрик для Grafana и мониторинга.

Функции:
- Публикация метрик в Redis keys
- Публикация в Redis streams для Grafana
- Агрегация метрик по стратегиям
- Temporal series для графиков

Redis схема:
- metrics:last:{strategy}:{symbol} = JSON с последними метриками
- metrics:strategy_perf stream = временной ряд для Grafana
"""

import json
import os
import time
from typing import Any

import redis

from common.log import setup_logger


class MetricsPublisher:
    """
    Публикация метрик для Grafana и других систем мониторинга.
    
    Публикует:
    - keys: metrics:last:{strategy}:{symbol}
    - stream: metrics:strategy_perf
    """

    def __init__(self, redis_url: str | None = None):
        """
        Инициализация Metrics Publisher.
        
        Args:
            redis_url: URL Redis (опционально)
        """
        self.logger = setup_logger("MetricsPublisher")

        redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis.from_url(redis_url, decode_responses=True)

        try:
            self.r.ping()
            self.logger.info("✅ Redis подключение установлено")
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к Redis: {e}")
            raise

        self.stream = os.getenv("STRATEGY_METRICS_STREAM", "metrics:strategy_perf")

    def publish(
        self,
        *,
        strategy: str,
        symbol: str,
        metrics: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Публикация метрик для стратегии/символа.
        
        Args:
            strategy: Название стратегии
            symbol: Символ
            metrics: Словарь с метриками
            
        Returns:
            Опубликованный payload
        """
        try:
            key = f"metrics:last:{strategy}:{symbol}"

            # Формируем payload
            payload = dict(metrics)
            payload["strategy"] = strategy
            payload["symbol"] = symbol
            payload["ts"] = time.time()

            # Сохраняем в key (последнее значение)
            self.r.set(key, json.dumps(payload))

            # Публикуем в stream (временной ряд)
            stream_fields = {
                "symbol": symbol,
                "strategy": strategy,
                "winrate": round(float(metrics.get("winrate", 0.0)), 4),
                "avg_pnl": round(float(metrics.get("avg_pnl_usd", 0.0)), 4),
                "total_pnl": round(float(metrics.get("total_pnl", 0.0)), 4),
                "total_trades": int(metrics.get("total_trades", 0)),
                "auc": round(float(metrics.get("auc", 0.0)), 4),
                "thr": metrics.get("thr", ""),
                "ts": time.time()
            }

            self.r.xadd(
                self.stream,
                stream_fields,
                maxlen=5000,
                approximate=True
            )

            self.logger.info(
                f"✅ Метрики опубликованы: {strategy}/{symbol} | "
                f"WR={stream_fields['winrate']:.1%} | "
                f"Avg P/L={stream_fields['avg_pnl']:.2f}"
            )

            return payload

        except Exception as e:
            self.logger.error(f"❌ Ошибка публикации метрик: {e}", exc_info=True)
            return {}

    def get_latest(self, strategy: str, symbol: str) -> dict[str, Any] | None:
        """
        Получение последних опубликованных метрик.
        
        Args:
            strategy: Название стратегии
            symbol: Символ
            
        Returns:
            Словарь с метриками или None
        """
        try:
            key = f"metrics:last:{strategy}:{symbol}"
            data = self.r.get(key)

            if not data:
                return None

            return json.loads(data)

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения метрик: {e}")
            return None

    def get_timeseries(
        self,
        count: int = 100,
        strategy: str | None = None,
        symbol: str | None = None
    ) -> list[dict[str, Any]]:
        """
        Получение временного ряда метрик.
        
        Args:
            count: Количество записей
            strategy: Фильтр по стратегии (опционально)
            symbol: Фильтр по символу (опционально)
            
        Returns:
            Список метрик
        """
        try:
            messages = self.r.xrevrange(self.stream, count=count)

            metrics = []

            for msg_id, data in messages:
                # Применяем фильтры
                if strategy and data.get("strategy") != strategy:
                    continue
                if symbol and data.get("symbol") != symbol:
                    continue

                metrics.append({
                    "id": msg_id,
                    **data
                })

            return metrics

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения временного ряда: {e}")
            return []

