"""
ROC Store - Хранение ROC кривых и метрик в Redis.

Функции:
- Сохранение ROC точек (threshold, TPR, FPR, Precision, Recall, F1)
- Публикация в metrics:roc stream
- Хранение в analytics:roc:{strategy}:{symbol} hash
- Доступ к историческим ROC данным

Использование:
- Интеграция с ThresholdTuner
- Доступ через Grafana
- Визуализация ROC кривых
"""

from __future__ import annotations
import os
import json
import time
from typing import List, Dict, Any, Optional

import redis

from common.log import setup_logger


class ROCStore:
    """
    Хранилище ROC кривых и метрик.
    
    Redis схема:
    - analytics:roc:{strategy}:{symbol} = JSON с точками ROC
    - metrics:roc stream = события публикации ROC
    """

    def __init__(self, redis_url: Optional[str] = None):
        """
        Инициализация ROC Store.
        
        Args:
            redis_url: URL Redis (опционально)
        """
        self.logger = setup_logger("ROCStore")

        redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis.from_url(redis_url, decode_responses=True)

        try:
            self.r.ping()
            self.logger.info("✅ Redis подключение установлено")
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к Redis: {e}")
            raise

        self.stream = os.getenv("ROC_METRICS_STREAM", "metrics:roc")

    def save(
        self
        strategy: str
        symbol: str
        points: List[Dict[str, Any]]
        auc: float
    ) -> Dict[str, Any]:
        """
        Сохранение ROC точек и метрик.
        
        Args:
            strategy: Название стратегии
            symbol: Символ
            points: Список точек ROC [{thr, tpr, fpr, prec, rec, f1, support}, ...]
            auc: Area Under Curve
            
        Returns:
            Сохранённый payload
        """
        try:
            key = f"analytics:roc:{strategy}:{symbol}"

            ts_now = time.time()
            payload = {
                "points": points
                "auc": round(float(auc), 4)
                "ts": ts_now
                "strategy": strategy
                "symbol": symbol
                "num_points": len(points)
            }

            # Сохраняем в key
            self.r.set(key, json.dumps(payload))

            # Публикуем summary в stream для мониторинга
            if points:
                self.r.xadd(
                    self.stream
                    {
                        "symbol": symbol
                        "strategy": strategy
                        "auc": payload["auc"]
                        "n": len(points)
                        "ts": ts_now
                    }
                    maxlen=2000
                    approximate=True
                )

            self.logger.info(
                f"✅ ROC сохранён: {strategy}/{symbol} | "
                f"AUC={payload['auc']:.3f} | Points={len(points)}"
            )

            return payload

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения ROC: {e}", exc_info=True)
            return {}

    def load(self, strategy: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Загрузка ROC данных.
        
        Args:
            strategy: Название стратегии
            symbol: Символ
            
        Returns:
            Словарь с ROC данными или None
        """
        try:
            key = f"analytics:roc:{strategy}:{symbol}"
            data = self.r.get(key)

            if not data:
                return None

            return json.loads(data)

        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки ROC: {e}")
            return None

    def get_recent_roc_events(self, count: int = 100) -> List[Dict[str, Any]]:
        """
        Получение последних событий ROC из stream.
        
        Args:
            count: Количество событий
            
        Returns:
            Список событий
        """
        try:
            messages = self.r.xrevrange(self.stream, count=count)

            events = []
            for msg_id, data in messages:
                events.append({
                    "id": msg_id
                    **data
                })

            return events

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения событий ROC: {e}")
            return []

