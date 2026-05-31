# cache_service.py
"""
Функционал кеширования и хранения, извлеченный из base_orderflow_handler.py
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List, Union
import time
from datetime import datetime

# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)


class CacheService:
    """
    Сервис для управления кешами и персистентным хранилищем.
    """

    def __init__(self, redis_client: Any, symbol: str):
        self.redis = redis_client
        self.symbol = symbol
        self.logger = setup_logger(f"CacheService:{symbol}")

        # Ключи кеша (единый владелец пивотов для символа)
        self._pivot_bundle_key = f"pivots:{symbol}"  # bundle: {"ts_ms","date","hlc","pivots"}
        self._atr_cache_key = f"atr:{symbol}"        # unchanged
        
        self._last_hlc_warning_ts = 0.0

    def _utc_date_str(self, ts_ms: int) -> str:
        return datetime.utcfromtimestamp(ts_ms / 1000).date().strftime("%Y-%m-%d")

    def _coerce_float(self, x: Any) -> float:
        """Безопасное приведение к float."""
        try:
            if x is None:
                return 0.0
            if isinstance(x, (int, float)):
                return float(x)
            s = str(x).strip()
            return float(s) if s else 0.0
        except Exception:
            return 0.0

    def _normalize_epoch_ms(self, ts_ms: Any) -> int:
        """
        Нормализация epoch ms. 
        Защита от minutes-of-day и прочих non-epoch значений.
        """
        now = int(time.time() * 1000)
        try:
            v = int(ts_ms)
        except Exception:
            return now

        if v <= 0:
            return now
        # epoch seconds -> ms
        if 1_000_000_000 <= v < 100_000_000_000:
            v *= 1000

        # окно валидности (2000..now+7d)
        if v < 946_684_800_000 or v > now + 7 * 86_400_000:
            return now
        return v

    def _decode_redis_str(self, raw: Any) -> Optional[str]:
        if raw is None:
            return None
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            return raw
        return str(raw)

    def _build_pivots_bundle(self, *, ts_ms: int, hlc: Dict[str, float]) -> Dict[str, Any]:
        pivots = self._compute_pivots(hlc)
        return {
            "ts_ms": int(ts_ms),
            "date": self._utc_date_str(ts_ms),
            "hlc": hlc,
            "pivots": pivots,
        }

    def _json_load(self, raw: Any) -> Optional[Dict[str, Any]]:
        """
        Надежный загрузчик JSON для Redis GET пейлоадов.
        Поддерживает: bytes/str/dict. Возвращает dict или None при ошибке.
        """
        if raw is None:
            return None
        if isinstance(raw, dict):
            return raw
        try:
            import json
            from json import JSONDecodeError
            if isinstance(raw, (bytes, bytearray)):
                s = raw.decode("utf-8", errors="ignore")
                return json.loads(s)
            if isinstance(raw, str):
                return json.loads(raw)
        except Exception as e:
            # ограничиваем логи; не спамить огромными пейлоадами
            try:
                preview = raw[:200] if isinstance(raw, (bytes, bytearray, str)) else str(raw)[:200]
            except Exception:
                preview = "<unprintable>"
            self.logger.warning("Bad JSON in cache payload: %r err=%s", preview, e)
            return None
        # Неизвестный тип
        return None

    def _unwrap_pivots_payload(self, payload: Dict[str, Any]) -> Dict[str, float]:
        """
        Обратная совместимость:
          - new format: {"ts_ms":..., "pivots": {...}}
          - old format: {"pivot":..., "r1":..., ...}
        """
        if isinstance(payload.get("pivots"), dict):
            payload = payload["pivots"]
        out: Dict[str, float] = {}
        for k, v in (payload or {}).items():
            try:
                if isinstance(v, (int, float)):
                    out[k] = float(v)
                elif isinstance(v, str) and v.strip() != "":
                    out[k] = float(v)
            except Exception:
                continue
        return out

    def _update_pivots(self, ts_ms: int) -> None:
        """Обновление кеша дневного бандла пивотов (единый владелец)."""
        try:
            # Расчет дневных пивотов из HLC вчерашнего дня
            yesterday_hlc = self._load_yesterday_hlc()
            if not yesterday_hlc:
                now_mono = time.monotonic()
                if now_mono - self._last_hlc_warning_ts > 600: # раз в 10 минут
                    self.logger.warning("Pivot update skipped: yesterday_hlc not found in Redis.")
                    self._last_hlc_warning_ts = now_mono
                return

            high = self._coerce_float(yesterday_hlc.get('high'))
            low = self._coerce_float(yesterday_hlc.get('low'))
            close = self._coerce_float(yesterday_hlc.get('close'))

            if high <= 0 or low <= 0 or close <= 0 or high < low:
                return

            import json
            bundle = self._build_pivots_bundle(
                ts_ms=int(ts_ms), 
                hlc={"high": high, "low": low, "close": close}
            )

            # Кешируем бандл на ~2 дня
            self.redis.setex(self._pivot_bundle_key, 172800, json.dumps(bundle))

        except Exception as e:
            self.logger.warning(f"Failed to update pivots: {e}")

    def _compute_pivots(self, hlc: Dict[str, float]) -> Dict[str, float]:
        high = float(hlc.get("high", 0.0) or 0.0)
        low = float(hlc.get("low", 0.0) or 0.0)
        close = float(hlc.get("close", 0.0) or 0.0)
        if high <= 0 or low <= 0 or close <= 0:
            return {}
        pivot = (high + low + close) / 3.0
        r1 = 2.0 * pivot - low
        s1 = 2.0 * pivot - high
        r2 = pivot + (high - low)
        s2 = pivot - (high - low)
        return {
            "pivot": pivot,
            "r1": r1,
            "s1": s1,
            "r2": r2,
            "s2": s2,
            # удобство (опционально)
            "high": high,
            "low": low,
            "close": close,
        }

    def _load_yesterday_hlc(self) -> Optional[Dict[str, float]]:
        """Load yesterday's HLC from storage."""
        try:
            keys = [
                f"yesterday_hlc:{self.symbol}",
                f"daily_hlc:{self.symbol}",
                f"prev_day:{self.symbol}",
            ]

            for key in keys:
                raw = self.redis.get(key)
                if raw:
                    obj = self._json_load(raw)
                    if isinstance(obj, dict):
                        return obj

        except Exception as e:
            self.logger.warning(f"Failed to load yesterday HLC from Redis: {e}")

        # Fallback to Postgres (Sync)
        try:
            from services.persistence_manager import get_persistence_manager
            pm = get_persistence_manager()
            
            # Use synchronous method
            data = pm.get_latest_daily_ohlc_sync(self.symbol)
            
            if data:
                self.logger.info(f"Restored yesterday_hlc from Postgres for {self.symbol}: {data['date']}")
                
                # Restore to Redis to avoid repeated DB hits
                # Use standard key
                key = f"yesterday_hlc:{self.symbol}"
                
                import json
                # Ensure it's JSON serialization compliant (get_latest_daily_ohlc_sync returns dict with floats/strs)
                self.redis.setex(key, 172800, json.dumps(data))
                
                return data

        except Exception as e:
            # Import error or DB error
            self.logger.warning(f"Failed to load yesterday HLC from Postgres fallback: {e}")

        return None

    def _calculate_hlc_from_ticks(self) -> Dict[str, float]:
        """Расчет HLC из тиков (заглушка - возвращает нулевые значения)."""
        return {
            'high': 0.0,
            'low': 0.0,
            'close': 0.0,
        }

    def _get_default_hlc(self) -> Dict[str, float]:
        """Получение дефолтного HLC, когда данных нет."""
        # Заглушечные дефолтные значения
        return {
            'high': 1.0,
            'low': 1.0,
            'close': 1.0,
        }

    def _nearest_pivot_key(self, price: float, pivots: Dict[str, float]) -> str:
        """Поиск ключа ближайшего уровня пивота."""
        if not pivots or price <= 0:
            return "none"

        min_dist = float('inf')
        nearest_key = "none"

        for key, level_price in pivots.items():
            if isinstance(level_price, (int, float)):
                dist = abs(price - level_price)
                if dist < min_dist:
                    min_dist = dist
                    nearest_key = key

        return nearest_key

    def _breakout_cross_info(self, price: float, up: bool, pivots: Dict[str, float]) -> Optional[str]:
        """Получение информации о пробое/пересечении пивотов."""
        if not pivots:
            return None

        # Проверка, пробивает ли цена уровни пивотов
        direction = "up" if up else "down"

        for level_name, level_price in pivots.items():
            if isinstance(level_price, (int, float)):
                if up and price > level_price:
                    return f"{direction}_break_{level_name}"
                elif not up and price < level_price:
                    return f"{direction}_break_{level_name}"

        return None

    def ensure_pivots_bundle(self, ts_ms: int) -> None:
        """
        Гарантия наличия бандла пивотов и соответствия текущей дате UTC.
        """
        ts_ms = self._normalize_epoch_ms(ts_ms)
        try:
            bundle = self._json_load(self.redis.get(self._pivot_bundle_key))
            if not bundle:
                self._update_pivots(ts_ms)
                return

            cached_date = str(bundle.get("date") or "")
            today = self._utc_date_str(ts_ms)
            if cached_date != today:
                self._update_pivots(ts_ms)
        except Exception as e:
            self.logger.warning("ensure_pivots_bundle failed: %s", e)

    def get_pivots_bundle(self) -> Optional[Dict[str, Any]]:
        """Получение закешированного бандла пивотов: {"ts_ms","date","hlc","pivots"}."""
        try:
            obj = self._json_load(self.redis.get(self._pivot_bundle_key))
            return obj if isinstance(obj, dict) else None
        except Exception as e:
            self.logger.warning("Failed to get pivots bundle: %s", e)
            return None

    def get_pivots(self) -> Optional[Dict[str, float]]:
        """Обратная совместимость: возврат только dict пивотов."""
        b = self.get_pivots_bundle()
        if not b:
            return None
        return self._unwrap_pivots_payload(b)

    def invalidate_cache(self) -> None:
        """Инвалидация всех кешей."""
        try:
            self.redis.delete(self._pivot_bundle_key)
            self.redis.delete(self._atr_cache_key)
        except Exception as e:
            self.logger.warning(f"Failed to invalidate cache: {e}")
