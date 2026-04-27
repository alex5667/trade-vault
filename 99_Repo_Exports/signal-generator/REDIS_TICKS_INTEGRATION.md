# Signal Generator - Redis Ticks Integration

## ✅ Миграция Завершена

Signal Generator перенастроен для работы с отдельным Redis instance для тиковых данных.

## 🎯 Архитектура: Dual Redis Setup

Signal Generator теперь использует **две отдельные Redis инстанции**:

### 1. **redis-ticks** (для чтения тиков)

- **Host**: `redis-ticks:6379`
- **URL**: `redis://redis-ticks:6379/0`
- **Назначение**: Чтение высокочастотных тиковых данных
- **Stream**: `stream:tick_XAUUSD`
- **Конфигурация**: `redis-ticks.conf` (оптимизирован для streams)

### 2. **scanner-redis-worker-1** (для записи сигналов)

- **Host**: `scanner-redis-worker-1:6379`
- **URL**: `redis://scanner-redis-worker-1:6379/0`
- **Назначение**: Запись сигналов, публикация в Telegram, audit streams
- **Streams**:
  - `signals:ta:XAUUSD` - для aggregated-hub
  - `notify:telegram` - для Telegram уведомлений
  - `signals:audit:XAUUSD` - для аудита сигналов

## 📝 Что Изменено

### 1. **signal_generator.py**

**До:**

```python
REDIS_URL = os.getenv("REDIS_URL", "redis://scanner-redis-worker-1:6379/0")
self.redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
```

**После:**

```python
REDIS_TICKS_URL = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")
REDIS_SIGNALS_URL = os.getenv("REDIS_SIGNALS_URL", "redis://scanner-redis-worker-1:6379/0")

# Два отдельных клиента
self.redis_ticks_client = redis.Redis.from_url(REDIS_TICKS_URL, ...)
self.redis_signals_client = redis.Redis.from_url(REDIS_SIGNALS_URL, ...)
```

**Изменения:**

- ✅ Разделены клиенты для чтения тиков и записи сигналов
- ✅ `fetch_ticks_from_redis()` использует `redis_ticks_client`
- ✅ Все публикации сигналов используют `redis_signals_client`
- ✅ Добавлены оптимизированные параметры подключения для redis-ticks

### 2. **config.env**

Добавлены новые параметры:

```bash
# Redis - Dual Instance Setup
USE_REAL_TICKS=true
REDIS_TICKS_URL=redis://redis-ticks:6379/0
REDIS_SIGNALS_URL=redis://scanner-redis-worker-1:6379/0
TICK_STREAM=stream:tick_XAUUSD
```

### 3. **docker-compose.yml**

**До:**

```yaml
environment:
  - USE_REAL_TICKS=true
  - REDIS_URL=redis://scanner-redis-worker-1:6379/0
depends_on:
  - redis-worker-1
```

**После:**

```yaml
environment:
  # 🎯 Redis Dual Instance Setup
  - USE_REAL_TICKS=true
  - REDIS_TICKS_URL=redis://redis-ticks:6379/0
  - REDIS_SIGNALS_URL=redis://scanner-redis-worker-1:6379/0
depends_on:
  redis-ticks:
    condition: service_healthy
  redis-worker-1:
    condition: service_started
```

## 🚀 Преимущества Dual Redis Setup

### 1. **Изоляция Нагрузки**

- Высокочастотные тики не влияют на основной Redis
- Отдельные memory limits и eviction policies

### 2. **Оптимизированная Производительность**

- `redis-ticks` оптимизирован для streams (IO threads, увеличенный stream node size)
- `scanner-redis-worker` оптимизирован для сигналов и конфигурации

### 3. **Масштабируемость**

- Можно независимо масштабировать redis-ticks
- Легко добавлять новые consumer groups для тиков
- Изолированные ресурсы для каждого типа данных

### 4. **Надежность**

- Независимость от основного Redis
- Отдельные backup/restore процедуры
- Изоляция сбоев

## 📊 Потоки Данных

```
┌─────────────────┐
│  tick-ingest    │
│     server      │
└────────┬────────┘
         │ XADD
         ▼
┌─────────────────┐
│   redis-ticks   │ ◄─────── READ (signal-generator)
│ stream:tick_*   │
└─────────────────┘

┌─────────────────┐
│signal-generator │
│  (Python)       │
└────────┬────────┘
         │ XADD (signals)
         ▼
┌─────────────────┐
│redis-worker-1   │
│ signals:ta:*    │
│ notify:telegram │
│ signals:audit:* │
└─────────────────┘
```

## 🔧 Работа с Сигналами

### Чтение Тиков

```python
def fetch_ticks_from_redis(self) -> int:
    """Fetch real ticks from redis-ticks stream"""
    if not self.redis_ticks_client:
        return 0

    messages = self.redis_ticks_client.xread(
        {TICK_STREAM: self.last_redis_id},
        count=10000 if self.last_redis_id == "0" else 100,
        block=1000
    )
    # Process ticks...
```

### Публикация Сигналов

```python
# Все публикации используют redis_signals_client
if self.redis_signals_client:
    # TA stream для aggregated-hub
    self.redis_signals_client.xadd(
        "signals:ta:XAUUSD",
        {"data": json.dumps(ta_payload)},
        maxlen=1000
    )

    # Telegram notifications
    self.redis_signals_client.xadd(
        "notify:telegram",
        redis_data,
        maxlen=1000
    )

    # Audit stream
    self.redis_signals_client.xadd(
        "signals:audit:XAUUSD",
        {"data": json.dumps(audit)},
        maxlen=200000
    )
```

## 📈 Мониторинг

### Проверка Подключений

```bash
# Проверить статус redis-ticks
make ticks-status

# Проверить streams в redis-ticks
make ticks-streams

# Логи signal-generator
docker logs -f scanner-signal-generator
```

### Ожидаемые Логи

```
✅ Connected to redis-ticks: redis://redis-ticks:6379/0
✅ Connected to redis-signals: redis://scanner-redis-worker-1:6379/0
============================================================
Signal Generator initialized
============================================================
Symbol: XAUUSD
Mode: REAL TICKS from Redis
Strategy: EMA(9/21) + RSI(14) + ATR(14)
Redis Ticks: redis://redis-ticks:6379/0
Redis Signals: redis://scanner-redis-worker-1:6379/0
Tick Stream: stream:tick_XAUUSD
============================================================
```

## ⚠️ Troubleshooting

### signal-generator не подключается к redis-ticks

```bash
# 1. Проверить статус redis-ticks
make ticks-status

# 2. Проверить логи redis-ticks
make ticks-logs

# 3. Проверить что тики публикуются
make ticks-cli
> XLEN stream:tick_XAUUSD

# 4. Проверить логи signal-generator
docker logs scanner-signal-generator | grep redis
```

### Нет тиков в stream

```bash
# Проверить tick-ingest-server
docker logs scanner-tick-ingest-server

# Проверить что тики публикуются в redis-ticks
make ticks-cli
> XINFO STREAM stream:tick_XAUUSD
```

### Сигналы не публикуются

```bash
# Проверить подключение к redis-signals
docker exec scanner-signal-generator redis-cli -h scanner-redis-worker-1 PING

# Проверить наличие streams
docker exec scanner-redis-worker-1 redis-cli KEYS "signals:*"
```

## 📞 Интеграция с Другими Сервисами

Signal Generator совместим с:

✅ **tick-ingest-server** - публикует тики в redis-ticks
✅ **aggregated-hub** - читает сигналы из `signals:ta:XAUUSD`
✅ **telegram-worker** - читает из `notify:telegram`
✅ **signal-performance-tracker** - читает из `signals:audit:XAUUSD`
✅ **go-gateway** - принимает сигналы через `/orders/enqueue`

## 🎓 Environment Variables

### Обязательные:

```bash
USE_REAL_TICKS=true
REDIS_TICKS_URL=redis://redis-ticks:6379/0
REDIS_SIGNALS_URL=redis://scanner-redis-worker-1:6379/0
TICK_STREAM=stream:tick_XAUUSD
```

### Опциональные:

```bash
TA_STREAM=signals:ta:XAUUSD
NOTIFY_STREAM=notify:telegram
SIGNAL_AUDIT_STREAM=signals:audit:XAUUSD
```

## ✅ Checklist для Запуска

- [x] Остановить signal-generator: `docker-compose stop signal-generator`
- [x] Проверить redis-ticks работает: `make ticks-status`
- [x] Проверить тики публикуются: `make ticks-streams`
- [x] Запустить signal-generator: `docker-compose up -d signal-generator`
- [x] Проверить логи: `docker logs -f scanner-signal-generator`
- [x] Проверить подключение к обоим Redis
- [x] Дождаться первого сигнала
- [x] Проверить публикацию в `signals:ta:XAUUSD`

---

**Дата**: 2025-11-05
**Версия**: 2.0
**Статус**: ✅ ЗАВЕРШЕНО
