"""
Performance Optimizer - Оптимизация производительности multi-symbol системы

Функционал:
- Connection pooling для Redis
- Кеширование Pivot Points (общие для всех handlers)
- Shared ATR cache
- Memory optimization
"""

import json
import time
from threading import Lock
from typing import Any

import redis


class RedisConnectionPool:
    """
    Singleton connection pool для Redis.
    
    Все handlers используют один пул соединений вместо создания
    своих собственных → экономия ресурсов.
    """

    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Connection pools (shared across all handlers)
        self._pools: dict[str, redis.ConnectionPool] = {}
        self._initialized = True

    def get_pool(self, redis_url: str, max_connections: int = 50) -> redis.ConnectionPool:
        """
        Получает или создает connection pool для URL.
        
        Args:
            redis_url: Redis URL
            max_connections: Максимальное количество соединений
            
        Returns:
            Connection pool
        """
        if redis_url not in self._pools:
            # NOTE: socket_keepalive_options удалены - вызывали Error 22 (EINVAL)
            # в некоторых Docker окружениях. Базовый socket_keepalive=True достаточно.
            self._pools[redis_url] = redis.ConnectionPool.from_url(
                redis_url,
                max_connections=max_connections,
                decode_responses=True,
                socket_keepalive=True,
                health_check_interval=0,  # Отключаем автоматическую проверку здоровья для предотвращения рекурсии
                socket_connect_timeout=10,
                socket_timeout=30,
            )

        return self._pools[redis_url]

    def get_client(self, redis_url: str) -> redis.Redis:
        """
        Создает Redis client с использованием shared pool.
        
        Args:
            redis_url: Redis URL
            
        Returns:
            Redis client
        """
        pool = self.get_pool(redis_url)
        # Создаем клиент с отключенной автоматической отправкой CLIENT SETINFO
        # Используем lib_name=None для отключения CLIENT SETINFO (redis-py 5.0+)
        try:
            client = redis.Redis(connection_pool=pool, lib_name=None)
        except TypeError:
            # Если lib_name не поддерживается, используем обычный клиент
            client = redis.Redis(connection_pool=pool)

        return client


class SharedCache:
    """
    Shared cache для общих данных между handlers.
    
    Избегает дублирования:
    - Pivot Points (общие для всех символов одного класса)
    - ATR значения (могут быть shared если один TF)
    - Другие метрики
    """

    def __init__(self, ttl: int = 300):
        """
        Args:
            ttl: Time to live для кеша в секундах
        """
        self._cache: dict[str, tuple[Any, float]] = {}
        self._lock = Lock()
        self._ttl = ttl

    def get(self, key: str) -> Any | None:
        """
        Получает значение из кеша.
        
        Args:
            key: Ключ
            
        Returns:
            Значение или None если не найдено/истекло
        """
        with self._lock:
            if key not in self._cache:
                return None

            value, expires_at = self._cache[key]

            # Проверяем TTL
            if time.time() > expires_at:
                del self._cache[key]
                return None

            return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """
        Сохраняет значение в кеш.
        
        Args:
            key: Ключ
            value: Значение
            ttl: TTL в секундах (опционально, default из __init__)
        """
        with self._lock:
            expires_at = time.time() + (ttl or self._ttl)
            self._cache[key] = (value, expires_at)

    def invalidate(self, key: str) -> None:
        """Удаляет значение из кеша"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]

    def clear(self) -> None:
        """Очищает весь кеш"""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Возвращает размер кеша"""
        return len(self._cache)


class PivotPointsCache:
    """
    Специализированный кеш для Pivot Points.
    
    Pivot Points общие для всех symbols в один день → кешируем.
    Пересчитываем только при смене дня.
    """

    def __init__(self, redis_client: redis.Redis):
        """
        Args:
            redis_client: Redis client для хранения
        """
        self._redis = redis_client
        self._cache = SharedCache(ttl=86400)  # 24 hours
        self._lock = Lock()

    def get_pivots(self, date: str) -> dict[str, float] | None:
        """
        Получает Pivot Points для даты.
        
        Args:
            date: Дата в формате YYYY-MM-DD
            
        Returns:
            Словарь с pivot points или None
        """
        # Проверяем локальный кеш (in-memory)
        cache_key = f"pivots:{date}"
        cached = self._cache.get(cache_key)
        if cached:
            return cached

        # Проверяем Redis (shared между handlers)
        try:
            redis_key = f"pivots:cache:{date}"
            data = self._redis.get(redis_key)
            if data:
                pivots = json.loads(data)
                # Сохраняем в локальный кеш
                self._cache.set(cache_key, pivots)
                return pivots
        except Exception:
            pass

        return None

    def set_pivots(self, date: str, pivots: dict[str, float]) -> None:
        """
        Сохраняет Pivot Points для даты.
        
        Args:
            date: Дата в формате YYYY-MM-DD
            pivots: Словарь с pivot points
        """
        cache_key = f"pivots:{date}"

        # Сохраняем в локальный кеш
        self._cache.set(cache_key, pivots)

        # Сохраняем в Redis (shared)
        try:
            redis_key = f"pivots:cache:{date}"
            self._redis.setex(
                redis_key,
                86400,  # 24 hours TTL
                json.dumps(pivots)
            )
        except Exception:
            # Не критично если не удалось сохранить в Redis
            pass


class ATRCache:
    """
    Кеш для ATR значений.
    
    ATR пересчитывается каждую минуту → кешируем на 10-15 секунд
    чтобы избежать повторных расчетов.
    """

    def __init__(self, redis_client: redis.Redis, ttl: int = 15):
        """
        Args:
            redis_client: Redis client
            ttl: TTL для кеша в секундах
        """
        self._redis = redis_client
        self._cache = SharedCache(ttl=ttl)

    def get_atr(self, symbol: str, timeframe: str) -> float | None:
        """
        Получает ATR из кеша.
        
        Args:
            symbol: Символ
            timeframe: Timeframe
            
        Returns:
            ATR значение или None
        """
        cache_key = f"atr:{symbol}:{timeframe}"

        # Локальный кеш
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        # Redis кеш
        try:
            redis_key = f"ta:last:atr:{symbol}"
            data = self._redis.get(redis_key)
            if data:
                try:
                    atr_data = json.loads(data)
                    atr = float(atr_data.get("atr", 0))
                    if atr > 0:
                        # Сохраняем в локальный кеш
                        self._cache.set(cache_key, atr)
                        return atr
                except (json.JSONDecodeError, KeyError, ValueError):
                    pass
        except Exception:
            pass

        return None

    def set_atr(self, symbol: str, timeframe: str, atr: float) -> None:
        """
        Сохраняет ATR в кеш.
        
        Args:
            symbol: Символ
            timeframe: Timeframe
            atr: ATR значение
        """
        cache_key = f"atr:{symbol}:{timeframe}"
        self._cache.set(cache_key, atr)


# Singleton instances
_connection_pool = RedisConnectionPool()
_shared_cache = SharedCache()


def get_optimized_redis_client(redis_url: str | None = None) -> redis.Redis:
    """
    Создает оптимизированный Redis client с connection pooling.
    
    Args:
        redis_url: Redis URL (если None, берется из REDIS_URL env)
        
    Returns:
        Redis client
    """
    if redis_url is None:
        import os
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    return _connection_pool.get_client(redis_url)


def get_shared_cache() -> SharedCache:
    """Возвращает singleton instance shared cache"""
    return _shared_cache

