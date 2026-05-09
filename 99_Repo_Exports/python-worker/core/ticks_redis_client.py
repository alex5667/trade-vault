"""
Ticks Redis Client - клиент для работы с отдельным Redis instance для тиковых данных.

Этот модуль предоставляет удобный интерфейс для работы с выделенным Redis для тиков,
который изолирован от основного Redis для лучшей производительности и масштабируемости.

АРХИТЕКТУРА:
- redis-ticks: отдельный Redis instance для высокочастотных тиковых данных
- scanner-redis: основной Redis для сигналов, конфигурации, и прочих данных
- redis-worker-1/2: worker instances для обработки сигналов

ИСПОЛЬЗОВАНИЕ:
    from core.ticks_redis_client import get_ticks_redis, get_ticks_dual_redis
    
    # Для чтения тиков
    ticks_redis = get_ticks_redis()
    messages = ticks_redis.xreadgroup(...)
    
    # Для записи тиков с fallback
    dual_ticks = get_ticks_dual_redis()
    dual_ticks.xadd("stream:tick_", {...}, maxlen=50000)

CONSUMER GROUPS:
- Отдельные consumer groups для каждого сервиса, читающего тики
- Рекомендуется использовать префикс "ticks-" для consumer groups
- Пример: "ticks-orderflow-group", "ticks-ohlc-group", "ticks-tracker-group"
"""

import os

# redis-py is optional in unit-test environments.
try:
    import redis  # type: ignore
    from redis.exceptions import RedisError  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

    class RedisError(Exception):
        pass


class TicksRedisClient:
    """
    Клиент для работы с отдельным Redis instance для тиков.
    
    Поддерживает:
    - Подключение к redis-ticks instance
    - Fallback на основной Redis при необходимости
    - Автоматическое переподключение
    """

    def __init__(
        self,
        ticks_url: str | None = None,
        ticks_host: str | None = None,
        ticks_port: int = 6379,
        ticks_db: int = 0,
        **kwargs
    ):
        """
        Инициализация клиента для Redis тиков.
        
        Args:
            ticks_url: URL для подключения к redis-ticks (redis://host:port/db)
            ticks_host: Хост redis-ticks (альтернатива ticks_url)
            ticks_port: Порт redis-ticks
            ticks_db: Номер БД redis-ticks
            **kwargs: Дополнительные параметры для redis.Redis
        """
        # Определяем URL для подключения
        if ticks_url:
            self.url = ticks_url
        elif ticks_host:
            self.url = f"redis://{ticks_host}:{ticks_port}/{ticks_db}"
        else:
            # Fallback на ENV переменные
            self.url = os.getenv(
                "REDIS_TICKS_URL",
                f"redis://{os.getenv('REDIS_TICKS_HOST', 'redis-ticks')}:{os.getenv('REDIS_TICKS_PORT', '6379')}/0"
            )

        # Настройки по умолчанию для высокочастотных операций
        # FIX: Убраны socket_keepalive_options (Error 22 - не поддерживаются в контейнере)
        default_kwargs = {
            "socket_timeout": 30,
            "socket_connect_timeout": 10,
            "socket_keepalive": True,
            "health_check_interval": 30,
            "decode_responses": True,
            "max_connections": 100,
        }

        # Объединяем с пользовательскими настройками
        default_kwargs.update(kwargs)

        # Создаем клиент
        if redis is None:
            raise RuntimeError("redis package is not installed. Install it to enable Redis connectivity: pip install redis")
        self.client = redis.from_url(self.url, **default_kwargs)

        print(f"✅ TicksRedisClient инициализирован: {self.url}")

    def __getattr__(self, name):
        """Прозрачный доступ ко всем методам redis.Redis"""
        return getattr(self.client, name)

    def ping(self) -> bool:
        """Проверка подключения к redis-ticks"""
        try:
            return self.client.ping()
        except RedisError as e:
            print(f"❌ Ошибка подключения к redis-ticks: {e}")
            return False

    def close(self):
        """Закрытие подключения"""
        self.client.close()


class DualTicksRedisClient:
    """
    Dual Redis client для записи тиков с fallback механизмом.
    
    Записывает в redis-ticks, при ошибке - fallback на основной Redis.
    """

    def __init__(
        self,
        primary_url: str | None = None,
        fallback_url: str | None = None,
        **kwargs
    ):
        """
        Инициализация dual client.
        
        Args:
            primary_url: URL для redis-ticks (primary)
            fallback_url: URL для основного Redis (fallback)
            **kwargs: Дополнительные параметры для redis.Redis
        """
        # Primary: redis-ticks
        self.primary_url = primary_url or os.getenv(
            "REDIS_TICKS_URL",
            f"redis://{os.getenv('REDIS_TICKS_HOST', 'redis-ticks')}:6379/0"
        )

        # Fallback: основной Redis
        self.fallback_url = fallback_url or os.getenv(
            "REDIS_URL",
            "redis://redis-worker-1:6379/0")

        # Создаем клиенты
        self.primary = redis.from_url(self.primary_url, decode_responses=True, **kwargs)
        self.fallback = redis.from_url(self.fallback_url, decode_responses=True, **kwargs)

        self.fallback_count = 0

        print("✅ DualTicksRedisClient инициализирован")
        print(f"   Primary: {self.primary_url}")
        print(f"   Fallback: {self.fallback_url}")

    def xadd(self, stream: str, fields: dict, **kwargs):
        """
        Добавление записи в stream с fallback.
        
        Args:
            stream: Имя stream
            fields: Данные для записи
            **kwargs: Дополнительные параметры для xadd
        """
        try:
            # Пробуем записать в primary (redis-ticks)
            return self.primary.xadd(stream, fields, **kwargs, maxlen=50000, approximate=True)
        except RedisError as e:
            # Fallback на основной Redis
            self.fallback_count += 1
            if self.fallback_count % 100 == 0:
                print(f"⚠️ Fallback на основной Redis (событие #{self.fallback_count}): {e}")
            return self.fallback.xadd(stream, fields, **kwargs, maxlen=50000, approximate=True)

    def set(self, key: str, value, **kwargs):
        """Установка значения с fallback"""
        try:
            return self.primary.set(key, value, **kwargs)
        except RedisError:
            self.fallback_count += 1
            return self.fallback.set(key, value, **kwargs)

    def get(self, key: str):
        """Получение значения с fallback"""
        try:
            return self.primary.get(key)
        except RedisError:
            return self.fallback.get(key)

    def __getattr__(self, name):
        """Прозрачный доступ к методам primary client"""
        return getattr(self.primary, name)

    def close(self):
        """Закрытие обоих подключений"""
        self.primary.close()
        self.fallback.close()


# Singleton instances
_ticks_redis: TicksRedisClient | None = None
_dual_ticks_redis: DualTicksRedisClient | None = None


def get_ticks_redis(**kwargs) -> TicksRedisClient:
    """
    Получить singleton instance TicksRedisClient.
    
    Args:
        **kwargs: Параметры для TicksRedisClient (используются только при первом вызове)
    
    Returns:
        TicksRedisClient instance
    """
    global _ticks_redis
    if _ticks_redis is None:
        _ticks_redis = TicksRedisClient(**kwargs)
    return _ticks_redis


def get_dual_ticks_redis(**kwargs) -> DualTicksRedisClient:
    """
    Получить singleton instance DualTicksRedisClient.
    
    Args:
        **kwargs: Параметры для DualTicksRedisClient (используются только при первом вызове)
    
    Returns:
        DualTicksRedisClient instance
    """
    global _dual_ticks_redis
    if _dual_ticks_redis is None:
        _dual_ticks_redis = DualTicksRedisClient(**kwargs)
    return _dual_ticks_redis


# Convenience functions
def create_ticks_consumer_group(
    stream: str,
    group: str,
    client: TicksRedisClient | None = None
) -> bool:
    """
    Создать consumer group для чтения тиков.
    
    Args:
        stream: Имя stream (например, "stream:tick_")
        group: Имя consumer group (рекомендуется "ticks-*")
        client: TicksRedisClient instance (опционально)
    
    Returns:
        True если группа создана или уже существует
    """
    if client is None:
        client = get_ticks_redis()

    try:
        client.xgroup_create(stream, group, id='$', mkstream=True)
        print(f"✅ Consumer group '{group}' создана для {stream}")
        return True
    except RedisError as e:
        if "BUSYGROUP" in str(e):
            print(f"ℹ️ Consumer group '{group}' уже существует для {stream}")
            return True
        else:
            print(f"❌ Ошибка создания consumer group '{group}': {e}")
            return False


if __name__ == "__main__":
    # Тестирование подключения
    print("=" * 70)
    print("Тестирование TicksRedisClient")
    print("=" * 70)

    # Тест 1: Подключение
    client = get_ticks_redis()
    if client.ping():
        print("✅ Подключение к redis-ticks успешно")
    else:
        print("❌ Не удалось подключиться к redis-ticks")

    # Тест 2: DualTicksRedisClient
    dual = get_dual_ticks_redis()
    try:
        # Пробуем записать тестовый тик
        dual.xadd(
            "test:tick_stream",
            {
                "ts": "1234567890",
                "bid": "3955.50",
                "ask": "3955.60",
                "last": "3955.55",
                "volume": "1.0",
                "flags": "0"
            },
            maxlen=1000,
            approximate=True
        )
        print("✅ DualTicksRedisClient работает корректно")
    except Exception as e:
        print(f"❌ Ошибка DualTicksRedisClient: {e}")

    # Тест 3: Создание consumer group
    create_ticks_consumer_group("stream:tick_", "ticks-test-group")

    print("=" * 70)
    print("Тестирование завершено")
    print("=" * 70)

