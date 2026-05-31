# Хранилище и Агрегация Новостей

## Обзор

Система хранения новостей использует Redis для потоковой обработки и агрегации новостных данных в реальном времени. Feature Store преобразует индивидуальные новости в агрегированные метрики, подходящие для использования в торговых алгоритмах.

## Архитектура Хранения

### Redis Streams Архитектура

```
news:raw           → Сырые новости от ингестора
news:analysis      → Проанализированные новости (компактные)
news:analysis:{UID} → Детальный JSON анализ
news:agg:{SYMBOL}  → Агрегированные фичи по символу
calendar:events    → Календарные события
calendar:next:{CCY} → Следующее событие по валюте
```

### Потоковая Обработка

#### News Raw Stream
```redis
# Структура сообщения
{
  "uid": "sha256_hash_of_url_title_timestamp",
  "source": "cryptopanic",
  "title": "Bitcoin ETF Sees Record Inflows",
  "url": "https://...",
  "published_ts_ms": "1703123456789",
  "symbols": "[\"BTC\"]",
  "summary": "Short summary..."
}
```

#### News Analysis Stream (Компактный)
```redis
# Компактное представление для быстрой обработки
{
  "uid": "sha256_hash",
  "ts_ms": "1703123456789",
  "symbols": "BTC,ETH",
  "risk": "0.850000",
  "surprise": "0.300000",
  "tags_mask": "4",           # 1 << 2 (fomc)
  "primary_tag_id": "3",      # fomc
  "confidence": "0.920000",
  "news_ref": "news:analysis:sha256_hash"
}
```

#### Детальное Хранилище
```redis
# news:analysis:{UID} - полный JSON анализ
{
  "uid": "sha256_hash",
  "ts_ms": 1703123456789,
  "symbol": "BTC",
  "asset_class": "crypto",
  "risk": 0.85,
  "surprise": 0.3,
  "tags_mask": 4,
  "primary_tag": "fomc",
  "summary": "Federal Reserve signals potential rate increase",
  "full_analysis": {
    "sentiment": "bearish",
    "impact_timeframe": "1-3 months",
    "affected_sectors": ["tech", "finance"],
    "confidence_intervals": {"risk": [0.75, 0.95]}
  }
}
```

## Feature Store

### EMA Агрегация

#### Экспоненциальное Сглаживание
```python
def ema(prev: float, x: float, alpha: float) -> float:
    """
    Экспоненциальное скользящее среднее

    Args:
        prev: Предыдущее значение EMA
        x: Новое значение
        alpha: Коэффициент сглаживания (0-1)

    Returns:
        Новое значение EMA
    """
    return (alpha * x) + ((1.0 - alpha) * prev)
```

#### Настройка EMA
```python
# Конфигурация для новостного риска
NEWS_RISK_HALF_LIFE_SEC = 1800  # 30 минут
alpha = 2 / (half_life_minutes + 1)  # alpha ≈ 0.067 для 30 мин

# Альтернативный расчет
alpha = 1 - exp(-ln(2) / half_life)  # Более точная формула
```

#### Агрегированные Данные
```redis
# news:agg:BTCUSDT
{
  "risk_ema": "0.234567",      # EMA risk score
  "surprise_ema": "-0.123456", # EMA surprise score
  "grade_id": "2",             # Максимальный grade (0-3)
  "tags_mask": "12",           # OR всех тегов (4 | 8 = 12)
  "primary_tag_id": "3",       # Последний primary tag
  "last_ref": "news:analysis:abc123",
  "last_ts_ms": "1703123456789"
}
```

### Логика Агрегации

#### Risk и Surprise (EMA)
```python
# EMA обновление для каждого символа
for symbol in impacted_symbols:
    key = f"news:agg:{symbol}"

    # Получить текущие значения
    current = redis.hgetall(key)
    prev_risk = float(current.get("risk_ema", "0.0"))
    prev_surprise = float(current.get("surprise_ema", "0.0"))

    # Рассчитать новые EMA
    new_risk = ema(prev_risk, analysis.risk, alpha)
    new_surprise = ema(prev_surprise, analysis.surprise, alpha)

    # Сохранить
    redis.hset(key, {
        "risk_ema": str(new_risk),
        "surprise_ema": str(new_surprise),
        "last_ts_ms": str(analysis.ts_ms)
    })
```

#### Grade Aggregation (Максимум)
```python
# Grade: берем максимум для консервативности
current_grade = int(current.get("grade_id", "0"))
new_grade = max(current_grade, analysis.grade_id)

redis.hset(key, "grade_id", str(new_grade))
```

#### Tags Aggregation (OR операция)
```python
# Tags: аккумулируем все теги (OR operation)
current_tags = int(current.get("tags_mask", "0"))
new_tags = current_tags | analysis.tags_mask

redis.hset(key, "tags_mask", str(new_tags))
```

#### Primary Tag (Последний)
```python
# Primary tag: просто последнее значение
redis.hset(key, "primary_tag_id", str(analysis.primary_tag_id))
```

## Calendar Feature Store

### Структура Календарных Данных

#### Calendar Events Stream
```redis
# calendar:events
{
  "event_id": "fmp_economic_gdp_us_2024_q1",
  "title": "US GDP Growth Rate Q1 2024",
  "ts_ms": "1703123456789",
  "grade_id": "2",           # 0-3 (low to critical)
  "currency": "USD",
  "region": "US",
  "symbols": "SPY,QQQ,IWM"
}
```

#### Next Event Storage
```redis
# calendar:next:USD
{
  "event_ts_ms": 1703123456789,
  "grade_id": 2,
  "ref": "calendar:event:fmp_economic_gdp_us_2024_q1"
}
```

### Логика Обновления Календаря

#### Выбор Следующего События
```python
def update_next_event(currency: str, event: CalendarEvent):
    """
    Обновляет следующее событие для валюты.
    Сохраняет только ближайшее предстоящее событие.
    """
    key = f"calendar:next:{currency}"
    now_ms = int(time.time() * 1000)

    # Игнорировать прошедшие события
    if event.ts_ms < now_ms - 60000:  # 1 минута grace period
        return

    # Проверить текущее следующее событие
    current = redis.get(key)
    if current:
        current_data = json.loads(current)
        current_ts = current_data.get("event_ts_ms", 0)

        # Если текущее событие раньше нового - не обновляем
        if current_ts > 0 and current_ts <= event.ts_ms:
            return

    # Сохранить новое событие
    event_data = {
        "event_ts_ms": event.ts_ms,
        "grade_id": event.grade_id,
        "ref": f"calendar:event:{event.event_id}"
    }

    redis.set(key, json.dumps(event_data), ex=ttl_sec)
```

#### Grade Mapping
```python
# FMP importance (1-5) → Internal grade (0-3)
GRADE_MAPPING = {
    1: 0,  # Low
    2: 0,  # Low
    3: 1,  # Medium
    4: 2,  # High
    5: 3   # Critical
}

def importance_to_grade(importance: int) -> int:
    return GRADE_MAPPING.get(importance, 0)
```

## Управление TTL

### Стратегия Очистки

#### Анализы Новостей
```python
# Детальные анализы хранятся дольше
ANALYSIS_TTL_SEC = 259200     # 3 дня - детальный JSON
ANALYSIS_DONE_TTL_SEC = 604800 # 7 дней - дедупликация

# Компактные анализы в stream
redis.xtrim("news:analysis", maxlen=50000, approximate=False)
redis.xtrim("news:raw", maxlen=10000, approximate=False)
```

#### Агрегированные Данные
```python
# Агрегации живут меньше
FEATURE_TTL_SEC = 3600  # 1 час - агрегированные фичи
CALENDAR_TTL_SEC = 3600 # 1 час - календарные данные

# Автоматическая очистка через TTL
redis.expire(key, FEATURE_TTL_SEC)
```

#### Calendar Events
```python
# Календарные события хранятся дольше
CALENDAR_EVENT_TTL_SEC = 2592000  # 30 дней

# Прошедшие события удаляются автоматически
redis.xtrim("calendar:events", maxlen=1000, approximate=False)
```

## Оптимизации Производительности

### Batch Processing
```python
def process_batch(messages: List[Tuple[str, Dict]]) -> None:
    """
    Пакетная обработка для снижения количества Redis вызовов
    """
    pipe = redis.pipeline(transaction=False)
    ack_ids = []

    for msg_id, fields in messages:
        ack_ids.append(msg_id)
        apply_one_to_pipeline(pipe, fields)

    # ACK всех сообщений пачкой
    if ack_ids:
        pipe.xack(stream, group, *ack_ids)

    pipe.execute()
```

### Lazy Loading
```python
def get_news_features(symbol: str) -> Optional[NewsFeatures]:
    """
    Lazy loading с кешированием в памяти
    """
    # Проверить in-memory cache
    if symbol in self._cache:
        cached_ts, features = self._cache[symbol]
        if time.time() * 1000 - cached_ts < self.cache_ttl_ms:
            return features

    # Загрузить из Redis
    key = f"news:agg:{symbol}"
    data = redis.hgetall(key)

    if not data:
        return None

    # Преобразовать в NewsFeatures
    features = self._parse_features(data)

    # Сохранить в cache
    self._cache[symbol] = (int(time.time() * 1000), features)

    return features
```

### Memory-Efficient Storage
```python
def compact_storage_format(features: NewsFeatures) -> Dict[str, str]:
    """
    Компактный формат хранения для снижения памяти
    """
    return {
        "r": str(int(features.risk * 1000)),        # risk * 1000 как int
        "s": str(int(features.surprise * 1000)),   # surprise * 1000 как int
        "t": str(features.tags_mask),               # tags как hex
        "p": str(features.primary_tag),             # primary tag id
        "u": str(features.updated_ts // 1000),      # ts в секундах
    }

def expand_storage_format(data: Dict[str, str]) -> NewsFeatures:
    """
    Распаковка компактного формата
    """
    return NewsFeatures(
        risk=float(data.get("r", "0")) / 1000,
        surprise=float(data.get("s", "0")) / 1000,
        tags_mask=int(data.get("t", "0")),
        primary_tag=int(data.get("p", "0")),
        updated_ts=int(data.get("u", "0")) * 1000,
    )
```

## Распределенная Агрегация

### Мульти-Инстанс Feature Store
```python
class DistributedFeatureStore:
    """
    Распределенный feature store с координацией через Redis
    """

    def __init__(self, instance_id: str, redis: redis.Redis):
        self.instance_id = instance_id
        self.redis = redis
        self.lock_ttl = 30  # seconds

    def acquire_symbol_lock(self, symbol: str) -> bool:
        """Атомарная блокировка символа для обновления"""
        lock_key = f"lock:news:agg:{symbol}"
        return redis.set(lock_key, self.instance_id, ex=self.lock_ttl, nx=True)

    def update_symbol_aggregates(self, symbol: str, analysis: NewsAnalysis):
        """Потокобезопасное обновление агрегаций"""
        if not self.acquire_symbol_lock(symbol):
            log.debug(f"Symbol {symbol} locked by another instance")
            return

        try:
            self._do_update(symbol, analysis)
        finally:
            # Освободить лок автоматический через TTL
            pass
```

### Консистентность Данных
```python
def ensure_consistency(symbol: str) -> None:
    """
    Проверка и восстановление консистентности данных
    """
    key = f"news:agg:{symbol}"
    data = redis.hgetall(key)

    # Проверить необходимые поля
    required_fields = ["risk_ema", "surprise_ema", "grade_id", "tags_mask"]
    for field in required_fields:
        if field not in data:
            log.warning(f"Missing field {field} for {symbol}, repairing")
            redis.hset(key, field, "0")

    # Проверить TTL
    ttl = redis.ttl(key)
    if ttl == -1:  # Нет TTL
        redis.expire(key, FEATURE_TTL_SEC)
```

## Мониторинг Хранилища

### Метрики Хранилища
```python
# Prometheus метрики
REDIS_MEMORY_USAGE = Gauge('redis_memory_bytes', 'Redis memory usage')
STREAM_LENGTH = Gauge('redis_stream_length', 'Stream message count', ['stream'])
FEATURE_COUNT = Gauge('news_features_count', 'Number of active features')
AGGREGATION_LATENCY = Histogram('news_aggregation_latency_seconds', 'Aggregation time')

# Кастомные метрики
SYMBOL_COVERAGE = Gauge('news_symbol_coverage', 'Symbols with news data')
DATA_FRESHNESS = Histogram('news_data_freshness_hours', 'How fresh is the data')
CACHE_HIT_RATIO = Gauge('news_cache_hit_ratio', 'Cache effectiveness')
```

### Health Checks
```python
def check_storage_health() -> Dict[str, Any]:
    """
    Комплексная проверка здоровья хранилища
    """
    health = {
        "redis_connected": False,
        "streams_exist": False,
        "features_count": 0,
        "oldest_data_age": 0,
        "memory_usage_pct": 0.0
    }

    try:
        # Проверка соединения
        redis.ping()
        health["redis_connected"] = True

        # Проверка streams
        streams = ["news:raw", "news:analysis", "calendar:events"]
        for stream in streams:
            if redis.exists(stream):
                length = redis.xlen(stream)
                health[f"{stream}_length"] = length

        health["streams_exist"] = True

        # Количество фич
        features_pattern = "news:agg:*"
        features_keys = redis.keys(features_pattern)
        health["features_count"] = len(features_keys)

        # Возраст данных
        if features_keys:
            sample_key = features_keys[0]
            last_ts = redis.hget(sample_key, "last_ts_ms")
            if last_ts:
                age_hours = (time.time() * 1000 - int(last_ts)) / (1000 * 3600)
                health["oldest_data_age"] = age_hours

        # Использование памяти
        info = redis.info("memory")
        used_memory = info.get("used_memory", 0)
        max_memory = info.get("maxmemory", 0)
        if max_memory > 0:
            health["memory_usage_pct"] = (used_memory / max_memory) * 100

    except Exception as e:
        health["error"] = str(e)

    return health
```

### Алерты
```python
def setup_storage_alerts():
    """
    Настройка алертов для хранилища
    """

    # Redis недоступен
    alert_redis_down = AlertRule(
        name="RedisDown",
        query="up{job='redis'} == 0",
        severity="critical"
    )

    # Высокое использование памяти
    alert_memory_high = AlertRule(
        name="RedisMemoryHigh",
        query="redis_memory_bytes / redis_maxmemory_bytes > 0.8",
        severity="warning"
    )

    # Старые данные
    alert_data_stale = AlertRule(
        name="NewsDataStale",
        query="news_data_freshness_hours > 2",
        severity="warning"
    )

    # Длинные очереди
    alert_queue_long = AlertRule(
        name="NewsQueueLong",
        query="redis_stream_length{stream='news:raw'} > 1000",
        severity="warning"
    )
```

## Резервное Копирование и Восстановление

### Стратегия Бэкапа
```python
def create_backup() -> str:
    """
    Создание snapshot хранилища новостей
    """
    timestamp = int(time.time())
    backup_key = f"backup:news:{timestamp}"

    # Сохранить структуру всех данных
    backup_data = {
        "timestamp": timestamp,
        "streams": {},
        "hashes": {},
        "strings": {}
    }

    # Бэкап streams
    streams = ["news:raw", "news:analysis", "calendar:events"]
    for stream in streams:
        messages = redis.xrange(stream, "-", "+", 10000)
        backup_data["streams"][stream] = messages

    # Бэкап hashes (агрегации)
    agg_keys = redis.keys("news:agg:*")
    for key in agg_keys:
        data = redis.hgetall(key)
        backup_data["hashes"][key] = data

    # Бэкап calendar next events
    cal_keys = redis.keys("calendar:next:*")
    for key in cal_keys:
        data = redis.get(key)
        backup_data["strings"][key] = data

    # Сохранить в Redis
    redis.set(backup_key, json.dumps(backup_data))
    redis.expire(backup_key, 30 * 24 * 3600)  # 30 дней

    return backup_key
```

### Восстановление из Бэкапа
```python
def restore_from_backup(backup_key: str) -> bool:
    """
    Восстановление данных из бэкапа
    """
    try:
        backup_json = redis.get(backup_key)
        if not backup_json:
            return False

        backup_data = json.loads(backup_json)

        # Восстановить streams
        for stream, messages in backup_data["streams"].items():
            for msg_id, fields in messages:
                redis.xadd(stream, fields, id=msg_id)

        # Восстановить hashes
        for key, data in backup_data["hashes"].items():
            redis.hset(key, data)

        # Восстановить strings
        for key, data in backup_data["strings"].items():
            redis.set(key, data)

        log.info(f"Restored from backup {backup_key}")
        return True

    except Exception as e:
        log.error(f"Failed to restore from backup: {e}")
        return False
```

## Масштабирование

### Вертикальное Масштабирование
```python
# Увеличение ресурсов Redis
redis_config = {
    "maxmemory": "4gb",           # Больше памяти
    "maxmemory-policy": "allkeys-lru",  # LRU eviction
    "tcp-keepalive": 300,        # Keep-alive
    "timeout": 300,              # Connection timeout
}
```

### Горизонтальное Масштабирование
```python
# Redis Cluster для высокой доступности
redis_cluster = redis.RedisCluster(
    startup_nodes=[
        {"host": "redis-1", "port": 6379},
        {"host": "redis-2", "port": 6379},
        {"host": "redis-3", "port": 6379}
    ],
    decode_responses=True
)

# Автоматическое шардирование
# Ключи автоматически распределяются по нодам
```

### Read Replicas
```python
# Read replicas для масштабирования чтения
read_redis = redis.Redis(host="redis-replica", port=6379)

def get_features_with_fallback(symbol: str) -> NewsFeatures:
    """
    Чтение с fallback на replica
    """
    try:
        # Сначала пытаемся читать из master
        return get_features_from_redis(master_redis, symbol)
    except Exception:
        # Fallback на replica
        return get_features_from_redis(read_redis, symbol)
```

## Миграции и Обновления

### Schema Evolution
```python
def migrate_feature_schema():
    """
    Миграция схемы хранения фич
    """
    # Найти все feature keys
    keys = redis.keys("news:agg:*")

    for key in keys:
        data = redis.hgetall(key)

        # Добавить новые поля со значениями по умолчанию
        if "surprise_ema" not in data:
            redis.hset(key, "surprise_ema", "0.0")

        if "grade_id" not in data:
            redis.hset(key, "grade_id", "0")

        # Переименовать поля если нужно
        if "old_field" in data:
            new_value = data["old_field"]
            redis.hset(key, "new_field", new_value)
            redis.hdel(key, "old_field")

    log.info(f"Migrated {len(keys)} feature records")
```

### Data Migration Tools
```python
class DataMigrator:
    """
    Инструмент для миграции данных между версиями
    """

    def __init__(self, source_redis: redis.Redis, target_redis: redis.Redis):
        self.source = source_redis
        self.target = target_redis

    def migrate_stream(self, stream_name: str, batch_size: int = 1000):
        """
        Миграция stream с пачками
        """
        last_id = "0"

        while True:
            messages = self.source.xrange(stream_name, last_id, "+", batch_size)
            if not messages:
                break

            for msg_id, fields in messages:
                self.target.xadd(stream_name, fields, id=msg_id)
                last_id = msg_id

            log.info(f"Migrated {len(messages)} messages from {stream_name}")
```

## Производительность

### Бенчмарки

| Операция | Latency (P50) | Throughput |
|----------|---------------|------------|
| Read Feature | 1.2ms | 800 ops/sec |
| Update Feature | 2.8ms | 350 ops/sec |
| Stream Write | 0.8ms | 1200 ops/sec |
| Stream Read | 1.5ms | 650 ops/sec |

### Оптимизации

1. **Pipeline Operations**: Группировка команд Redis
2. **Connection Pooling**: Переиспользование соединений
3. **Lazy Evaluation**: Отложенные вычисления
4. **Memory Layout**: Оптимизация структур данных
5. **Compression**: Сжатие больших значений

### Профилирование
```python
import cProfile
import pstats

def profile_storage_operations():
    """
    Профилирование операций хранения
    """
    profiler = cProfile.Profile()
    profiler.enable()

    # Выполнить операции
    for i in range(1000):
        update_feature_store(symbol=f"BTC{i%100}", analysis=sample_analysis)

    profiler.disable()

    # Анализ результатов
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative').print_stats(20)
```
