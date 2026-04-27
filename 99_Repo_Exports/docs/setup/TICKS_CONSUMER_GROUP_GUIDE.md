# Отдельная Consumer Group для Тиковых Данных

## Обзор

Создана отдельная инфраструктура для обработки высокочастотных тиковых данных с выделенным Redis instance и изолированными consumer groups.

## Архитектура

### Redis Instances

```
┌─────────────────────────────────────────────────────────────┐
│                    Redis Architecture                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────┐  ┌──────────────────┐                 │
│  │  scanner-redis   │  │   redis-ticks    │                 │
│  │  (основной)      │  │  (тиковые данные)│                 │
│  ├──────────────────┤  ├──────────────────┤                 │
│  │ - Сигналы        │  │ - stream:tick_*  │                 │
│  │ - Конфиг         │  │ - stream:book_*  │                 │
│  │ - Метаданные     │  │ - Consumer groups│                 │
│  │ - ATR/Pivots     │  │ - High throughput│                 │
│  └──────────────────┘  └──────────────────┘                 │
│                                                               │
│  ┌──────────────────┐  ┌──────────────────┐                 │
│  │ redis-worker-1   │  │ redis-worker-2   │                 │
│  │  (сигналы)       │  │  (backup)        │                 │
│  └──────────────────┘  └──────────────────┘                 │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Особенности redis-ticks

**Оптимизации для высокочастотных данных:**

- **Память**: 12GB (vs 16GB основной)
- **CPU**: 3 cores (vs 4 cores основной)
- **Connections**: maxclients 10000
- **IO threads**: 4 (multi-core processing)
- **Stream optimization**: увеличен node size
- **Persistence**: AOF only (тики эфемерные)
- **Eviction**: allkeys-lru для старых тиков

## Установка и Запуск

### 1. Запуск Docker Compose

```bash
# Запуск всех сервисов включая redis-ticks
docker-compose up -d

# Проверка что redis-ticks запущен
docker-compose ps redis-ticks

# Логи redis-ticks
docker-compose logs -f redis-ticks
```

### 2. Проверка подключения

```bash
# Подключение к redis-ticks через CLI
docker exec -it scanner-redis-ticks redis-cli

# Проверка streams
XINFO STREAM stream:tick_XAUUSD

# Проверка consumer groups
XINFO GROUPS stream:tick_XAUUSD
```

## Использование в Python

### Подключение к redis-ticks

```python
from core.ticks_redis_client import get_ticks_redis, create_ticks_consumer_group

# Получение клиента для redis-ticks
ticks_redis = get_ticks_redis()

# Проверка подключения
if ticks_redis.ping():
    print("✅ Connected to redis-ticks")
```

### Создание Consumer Group

```python
from core.ticks_redis_client import create_ticks_consumer_group

# Создание группы для обработки тиков
stream = "stream:tick_XAUUSD"
group = "ticks-orderflow-group"  # Используйте префикс "ticks-"

create_ticks_consumer_group(stream, group)
```

### Чтение тиков через Consumer Group

```python
from core.ticks_redis_client import get_ticks_redis
import time

# Инициализация
ticks_redis = get_ticks_redis()
stream = "stream:tick_XAUUSD"
group = "ticks-orderflow-group"
consumer = f"consumer-{os.getpid()}"

# Создание consumer group
ticks_redis.xgroup_create(stream, group, id='$', mkstream=True)

# Основной цикл обработки
while True:
    try:
        # Чтение из stream через consumer group
        messages = ticks_redis.xreadgroup(
            group,
            consumer,
            {stream: '>'},
            count=100,
            block=1000
        )

        if not messages:
            continue

        for stream_name, items in messages:
            for msg_id, fields in items:
                try:
                    # Обработка тика
                    tick_data = json.loads(fields.get('data', '{}'))
                    process_tick(tick_data)

                    # ACK сообщения
                    ticks_redis.xack(stream, group, msg_id)

                except Exception as e:
                    print(f"Error processing tick: {e}")
                    ticks_redis.xack(stream, group, msg_id)

    except Exception as e:
        print(f"Error in read loop: {e}")
        time.sleep(1)
```

### Запись тиков с Fallback

```python
from core.ticks_redis_client import get_dual_ticks_redis

# DualTicksRedisClient автоматически делает fallback на основной Redis
dual_ticks = get_dual_ticks_redis()

# Публикация тика (автоматический fallback при ошибке)
dual_ticks.xadd(
    "stream:tick_XAUUSD",
    {
        "ts": str(int(time.time() * 1000)),
        "bid": "3955.50",
        "ask": "3955.60",
        "last": "3955.55",
        "volume": "1.0",
        "flags": "0"
    },
    maxlen=50000,
    approximate=True
)
```

## Миграция Существующих Сервисов

### Шаг 1: Обновление Environment Variables

В `docker-compose.yml` для сервисов, работающих с тиками:

```yaml
environment:
  # Основной Redis для сигналов
  - REDIS_URL=redis://scanner-redis:6379/0

  # ✅ Redis для тиков (отдельный instance)
  - REDIS_TICKS_URL=redis://redis-ticks:6379/0
  - REDIS_TICKS_HOST=redis-ticks
  - REDIS_TICKS_PORT=6379
```

### Шаг 2: Обновление Dependencies

```yaml
depends_on:
  redis:
    condition: service_healthy
  redis-ticks:
    condition: service_healthy
```

### Шаг 3: Обновление Python кода

```python
# СТАРЫЙ КОД (до миграции)
from core.redis_client import get_redis

redis = get_redis()
messages = redis.xreadgroup(...)

# НОВЫЙ КОД (после миграции)
from core.ticks_redis_client import get_ticks_redis

ticks_redis = get_ticks_redis()
messages = ticks_redis.xreadgroup(...)
```

## Consumer Groups и Naming Convention

### Рекомендуемые имена групп

```python
# Префикс "ticks-" для групп, читающих тики
consumer_groups = {
    "ticks-orderflow-group": "OrderFlow handler (multi-symbol)",
    "ticks-ohlc-group": "OHLC aggregator (daily pivots)",
    "ticks-tracker-group": "Signal performance tracker",
    "ticks-hub-group": "Aggregated signal hub",
    "ticks-generator-group": "Technical analysis signal generator",
    "ticks-paper-group": "Paper trading executor",
}
```

### Создание множественных consumer groups

```python
from core.ticks_redis_client import create_ticks_consumer_group

streams = ["stream:tick_XAUUSD", "stream:tick_BTCUSD", "stream:tick_ETHUSD"]
groups = ["ticks-orderflow-group", "ticks-ohlc-group"]

for stream in streams:
    for group in groups:
        create_ticks_consumer_group(stream, group)
```

## Мониторинг

### Метрики redis-ticks

```bash
# Подключение к redis-ticks
docker exec -it scanner-redis-ticks redis-cli

# Общая информация
INFO

# Использование памяти
MEMORY STATS

# Информация о streams
XINFO STREAM stream:tick_XAUUSD

# Consumer groups статистика
XINFO GROUPS stream:tick_XAUUSD
XINFO CONSUMERS stream:tick_XAUUSD ticks-orderflow-group

# Pending messages
XPENDING stream:tick_XAUUSD ticks-orderflow-group
```

### Проверка производительности

```python
from core.ticks_redis_client import get_ticks_redis
import time

ticks_redis = get_ticks_redis()
stream = "stream:tick_XAUUSD"

# Получение длины stream
length = ticks_redis.xlen(stream)
print(f"Stream length: {length}")

# Получение информации о последних записях
latest = ticks_redis.xrevrange(stream, count=10)
for msg_id, fields in latest:
    print(f"{msg_id}: {fields}")

# Метрики consumer group
groups = ticks_redis.xinfo_groups(stream)
for group in groups:
    print(f"Group: {group['name']}")
    print(f"  Consumers: {group['consumers']}")
    print(f"  Pending: {group['pending']}")
    print(f"  Last delivered: {group['last-delivered-id']}")
```

## Troubleshooting

### Проблема: Consumer group не создается

```bash
# Проверка существования stream
docker exec scanner-redis-ticks redis-cli XINFO STREAM stream:tick_XAUUSD

# Удаление и пересоздание группы
docker exec scanner-redis-ticks redis-cli XGROUP DESTROY stream:tick_XAUUSD ticks-orderflow-group
docker exec scanner-redis-ticks redis-cli XGROUP CREATE stream:tick_XAUUSD ticks-orderflow-group $ MKSTREAM
```

### Проблема: Pending messages растут

```bash
# Проверка pending messages
docker exec scanner-redis-ticks redis-cli XPENDING stream:tick_XAUUSD ticks-orderflow-group

# Claim старых pending messages (если consumer умер)
docker exec scanner-redis-ticks redis-cli XAUTOCLAIM stream:tick_XAUUSD ticks-orderflow-group consumer-new 60000 0-0 COUNT 100
```

### Проблема: Out of memory в redis-ticks

```bash
# Проверка использования памяти
docker exec scanner-redis-ticks redis-cli MEMORY STATS

# Ручная очистка старых тиков (используйте с осторожностью)
docker exec scanner-redis-ticks redis-cli XTRIM stream:tick_XAUUSD MAXLEN ~ 10000

# Настройка автоматического trimming через stream-trimmer сервис
# См. docker-compose.yml -> stream-trimmer
```

## Best Practices

### 1. Consumer Group Naming

✅ **Хорошо:**

```python
group = "ticks-orderflow-group"
group = "ticks-ohlc-aggregator-group"
group = "ticks-tracker-XAUUSD-group"
```

❌ **Плохо:**

```python
group = "group1"
group = "consumer-group"
group = "xauusd-group"  # Не указан префикс "ticks-"
```

### 2. ACK Messages

✅ **Всегда делайте ACK:**

```python
try:
    process_tick(tick_data)
    ticks_redis.xack(stream, group, msg_id)
except Exception as e:
    log_error(e)
    ticks_redis.xack(stream, group, msg_id)  # ACK даже при ошибке
```

### 3. Batch Processing

✅ **Читайте батчами для лучшей производительности:**

```python
messages = ticks_redis.xreadgroup(
    group,
    consumer,
    {stream: '>'},
    count=100,  # Batch size
    block=1000  # Timeout
)
```

### 4. Connection Pooling

✅ **Используйте singleton instances:**

```python
from core.ticks_redis_client import get_ticks_redis

# Автоматический connection pooling
ticks_redis = get_ticks_redis()
```

## Performance Considerations

### Настройка для разных сценариев

**High-frequency tick ingestion (MT5 EA):**

```yaml
environment:
  - REDIS_TICKS_MAX_CONNECTIONS=100
  - XAU_TICK_STREAM_MAXLEN=50000
  - XAU_TICK_USE_MAXLEN=false # Используйте batch trimmer
```

**Real-time orderflow analysis:**

```yaml
environment:
  - REDIS_TICKS_MAX_CONNECTIONS=50
  - XAU_READ_COUNT=100
  - XAU_READ_BLOCK_MS=1000
```

**Historical data aggregation (OHLC):**

```yaml
environment:
  - REDIS_TICKS_MAX_CONNECTIONS=25
  - XAU_READ_COUNT=200
  - XAU_READ_BLOCK_MS=5000
```

## Дополнительные Ресурсы

- [Redis Streams Documentation](https://redis.io/docs/data-types/streams/)
- [Consumer Groups Guide](https://redis.io/docs/data-types/streams-tutorial/)
- [Redis Memory Optimization](https://redis.io/docs/management/optimization/memory-optimization/)

## Changelog

- **2025-11-05**: Создана отдельная инфраструктура для тиков
  - Добавлен redis-ticks instance
  - Создан TicksRedisClient и DualTicksRedisClient
  - Обновлены все сервисы для работы с отдельным Redis
  - Добавлена документация
