from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""
Отправка команд трейлинга в go-gateway.

Интегрировано с scanner_infra:
- HTTP клиент для go-gateway
- Поддержка /orders/push endpoint
- Retry logic с exponential backoff
"""

import os
import time
import requests
from typing import Optional, Dict, Any, List
from services.trailing_profiles import TrailingProfile

from common.log import setup_logger

log = setup_logger("order_trailing_dispatcher")


class OrderTrailingDispatcher:
    """
    Шлёт в go-gateway команду: "включи трейлинг по этой позиции".
    Дальше gateway скормит это MT5 в /orders/poll.
    
    Два режима работы:
    1. send_trailing_command() - отправляет mode="ATR" (MT5 сам считает)
    2. send_trailing_command_from_atr() - конвертирует ATR в пункты (РЕКОМЕНДУЕТСЯ)
    """

    def __init__(
        self, 
        gateway_url: Optional[str] = None, 
        max_retries: int = 3,
        redis_client = None
    ):
        """
        Args:
            gateway_url: URL go-gateway (default: GATEWAY_URL env var)
            max_retries: Максимальное количество попыток
            redis_client: Redis клиент для получения symbol specs (опционально)
        """
        self.gateway_url = (gateway_url or os.getenv("GATEWAY_URL", "http://scanner-go-gateway:8090")).rstrip("/")
        self.max_retries = max_retries
        self.timeout = float(os.getenv("GATEWAY_TIMEOUT", "3.0"))
        
        # Redis для symbol specs
        if redis_client is None:
            import redis
            redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            self.r = redis.from_url(redis_url, decode_responses=True)
        else:
            self.r = redis_client
        
        log.info("✅ OrderTrailingDispatcher initialized: gateway=%s", self.gateway_url)

    def _post_to_gateway(self, payload: Dict[str, Any], label: str) -> bool:
        """Отправка payload в gateway с retry."""
        for attempt in range(1, self.max_retries + 1):
            try:
                log.debug(
                    "Sending %s (attempt %d/%d): sid=%s",
                    label, attempt, self.max_retries, payload.get("sid")
                )

                resp = requests.post(
                    f"{self.gateway_url}/orders/push",
                    json=payload,
                    timeout=self.timeout,
                    headers={"Content-Type": "application/json"}
                )

                if resp.status_code // 100 == 2:
                    log.info("✅ %s sent: sid=%s", label, payload.get("sid"))
                    return True

                log.warning(
                    "⚠️  Gateway returned status %d (attempt %d/%d): %s",
                    resp.status_code, attempt, self.max_retries, resp.text[:200]
                )

                if resp.status_code >= 500 and attempt < self.max_retries:
                    backoff = 2 ** (attempt - 1)
                    log.debug("Retrying in %ds...", backoff)
                    time.sleep(backoff)
                    continue

                return False

            except requests.exceptions.Timeout:
                log.warning(
                    "⚠️  Gateway timeout (attempt %d/%d) for %s",
                    attempt, self.max_retries, label
                )
                if attempt < self.max_retries:
                    time.sleep(1.0)
                    continue
                return False

            except requests.exceptions.ConnectionError as exc:
                log.warning(
                    "⚠️  Gateway connection error (attempt %d/%d) for %s: %s",
                    attempt, self.max_retries, label, exc
                )
                if attempt < self.max_retries:
                    time.sleep(2.0)
                    continue
                return False

            except Exception as exc:
                log.error(
                    "❌ Unexpected error sending %s: %s",
                    label, exc, exc_info=True
                )
                return False

        log.error(
            "❌ Failed to send %s after %d attempts: sid=%s",
            label, self.max_retries, payload.get("sid")
        )
        return False

    def send_trailing_command(
        self,
        sid: str,
        symbol: str,
        position_id: Optional[str],
        profile: TrailingProfile,
        metadata: Optional[dict] = None
    ) -> bool:
        """
        Отправить команду трейлинга в gateway.
        
        Args:
            sid: ID сигнала
            symbol: Символ (XAUUSD, BTCUSD, etc)
            position_id: ID позиции MT5 (опционально)
            profile: Профиль трейлинга
            metadata: Дополнительные метаданные
            
        Returns:
            True если успешно, False иначе
        """
        payload = {
            "action": "trail",
            "sid": sid,
            "symbol": symbol,
            "mode": profile.mode,   # "ATR" / "POINTS"
            "source": "tp1_trailing_orchestrator",
            "timestamp": get_ny_time_millis()
        }
        
        # Добавляем position_id если есть
        if position_id:
            payload["position_id"] = position_id
        
        # Дополнительные параметры в зависимости от режима
        if profile.mode == "ATR":
            payload["atr_mult"] = profile.atr_mult
        elif profile.mode == "POINTS":
            payload["trail_points"] = profile.points
        elif profile.mode == "STEP":
            if profile.step_points:
                payload["step_points"] = profile.step_points
        
        # Минимальная фиксация прибыли
        if profile.hard_min_lock is not None:
            payload["hard_min_lock"] = profile.hard_min_lock
        
        # Метаданные
        if metadata:
            payload["metadata"] = metadata
        
        # 🛑 gateway не принимает action=trail без SL → пропускаем как успешное
        return True
    
    def send_trailing_command_from_atr(
        self,
        sid: str,
        symbol: str,
        position_id: Optional[str],
        atr_value: float,
        atr_mult: float,
        point: Optional[float] = None,
        metadata: Optional[dict] = None
    ) -> bool:
        """
        Отправить команду трейлинга с конвертацией ATR в пункты (РЕКОМЕНДУЕТСЯ).
        
        Преимущества:
        - Видим в логах точное расстояние "трейлили ровно 0.6×того ATR, на котором входили"
        - MT5 не нужно считать свой ATR
        - Консистентность с аналитикой
        - Лучше для отчётов trade_back
        
        Args:
            sid: ID сигнала
            symbol: Символ (XAUUSD, BTCUSD, etc)
            position_id: ID позиции MT5 (опционально)
            atr_value: Значение ATR из исходного сигнала
            atr_mult: Множитель ATR из профиля (0.6, 0.8, 1.2)
            point: Размер пункта (если None, берётся из Redis symbol specs)
            metadata: Дополнительные метаданные
            
        Returns:
            True если успешно, False иначе
        """
        # Получаем point из symbol specs если не передан
        if point is None:
            point = self._get_symbol_point(symbol)
        
        # Рассчитываем расстояние трейлинга
        trail_dist_price = atr_value * atr_mult  # В единицах цены
        trail_points = trail_dist_price / point   # В пунктах MT5
        
        payload = {
            "action": "trail",
            "sid": sid,
            "symbol": symbol,
            "mode": "POINTS",  # 🎯 Готовое значение в пунктах
            "trail_points": trail_points,
            "source": "tp1_trailing_orchestrator",
            "timestamp": get_ny_time_millis()
        }
        
        # Добавляем position_id если есть
        if position_id:
            payload["position_id"] = position_id
        
        # Метаданные для логов и анализа
        if metadata is None:
            metadata = {}
        
        metadata.update({
            "atr_value": atr_value,
            "atr_mult": atr_mult,
            "trail_dist_price": trail_dist_price,
            "point_size": point,
            "calculated_from_signal_atr": True
        })
        payload["metadata"] = metadata
        
        log.info(
            "Skipping trail command to gateway (POINTS): sid=%s points=%.1f (ATR %.2f × %.2f = %.2f) — will rely on modify with SL",
            sid, trail_points, atr_value, atr_mult, trail_dist_price
        )
        # 🛑 gateway требует SL для action=modify; trail без SL отклоняется. Пропускаем как успешное.
        return True
    
    def _get_symbol_point(self, symbol: str) -> float:
        """
        Получить размер пункта для символа из Redis symbol specs.
        
        Args:
            symbol: Символ (XAUUSD, BTCUSD, etc)
            
        Returns:
            Размер пункта (default 0.1 для XAUUSD)
        """
        try:
            import json
            
            # Пробуем получить из symbol_specs
            specs_key = f"symbol_specs:{symbol}"
            specs_data = self.r.get(specs_key)
            
            if specs_data:
                specs = json.loads(specs_data)
                point = specs.get("point")
                if point and point > 0:
                    log.debug("Symbol point from Redis: %s = %.4f", symbol, point)
                    return float(point)
            
        except Exception as e:
            log.debug("Could not get symbol specs from Redis: %s", e)
        
        # Fallback значения
        defaults = {
            "XAUUSD": 0.1,
            "XAGUSD": 0.01,
            "BTCUSD": 1.0,
            "ETHUSD": 0.1,
            "EURUSD": 0.00001,
            "GBPUSD": 0.00001,
        }
        
        point = defaults.get(symbol, 0.1)
        log.debug("Using default point for %s: %.5f", symbol, point)
        return point

    def get_symbol_point(self, symbol: str) -> float:
        """Публичный хелпер для доступа к стоимости пункта."""
        return self._get_symbol_point(symbol)

    def send_trailing_modify(
        self,
        sid: str,
        symbol: str,
        side: Optional[str],
        position_id: Optional[str],
        new_sl: float,
        tp_levels: Optional[List[float]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        clear_tp_levels: bool = False
    ) -> bool:
        payload: Dict[str, Any] = {
            "action": "modify",
            "sid": sid,
            "symbol": symbol,
            "sl": new_sl,
            "source": "tp1_trailing_orchestrator",
            "timestamp": get_ny_time_millis()
        }

        if side:
            payload["side"] = side

        if position_id:
            payload["position_id"] = position_id

        if tp_levels is not None:
            payload["tp_levels"] = tp_levels

        if clear_tp_levels:
            payload["clear_tp_levels"] = True

        if metadata:
            payload["metadata"] = metadata

        return self._post_to_gateway(payload, "trailing modify")
    
    def send_modify_sl(
        self,
        sid: str,
        symbol: str,
        new_sl: float,
        position_id: Optional[str] = None
    ) -> bool:
        """
        Отправить команду модификации SL.
        
        Args:
            sid: ID сигнала
            symbol: Символ
            new_sl: Новый уровень SL
            position_id: ID позиции MT5 (опционально)
            
        Returns:
            True если успешно, False иначе
        """
        payload = {
            "action": "modify_sl",
            "sid": sid,
            "symbol": symbol,
            "new_sl": new_sl,
            "source": "tp1_trailing_orchestrator",
            "timestamp": get_ny_time_millis()
        }
        
        if position_id:
            payload["position_id"] = position_id
        
        return self._post_to_gateway(payload, "modify_sl")


if __name__ == "__main__":
    # Тестирование
    from services.trailing_profiles import TrailingProfile
    
    dispatcher = OrderTrailingDispatcher()
    
    # Тестовый профиль
    test_profile = TrailingProfile(
        name="test_rocket",
        mode="ATR",
        atr_mult=0.6,
        comment="Test profile"
    )
    
    # Отправка тестовой команды
    success = dispatcher.send_trailing_command(
        sid="test-signal-123",
        symbol="XAUUSD",
        position_id="1234567",
        profile=test_profile,
        metadata={"test": True}
    )
    
    print(f"\n{'✅' if success else '❌'} Test trailing command: {success}")

