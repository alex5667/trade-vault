# ✅ Отдельный Redis для Тиковых Данных - НАСТРОЙКА ЗАВЕРШЕНА

## 🎯 Что было сделано

Создана полноценная инфраструктура для обработки тиковых данных в отдельном Redis instance:

### 1. ✅ Новый Redis Instance для Тиков

**Контейнер**: `scanner-redis-ticks`

**Характеристики:**

- **Память**: 12GB (vs 16GB основной Redis)
- **CPU**: 3 cores
- **Порт**: 6379 (внутри Docker сети)
- **Оптимизирован для**: высокочастотных записей, streams, consumer groups

**Конфигурация:**

```
Файл: redis-ticks.conf
- Persistence: AOF only (тики временные)
- Eviction: allkeys-lru
- IO threads: 4 (multi-core)
- Stream node size: увеличен для производительности
- Memory limit: 10GB
```

### 2. ✅ Python Client для Redis Ticks

**Файл**: `python-worker/core/ticks_redis_client.py`

**Классы:**

- `TicksRedisClient` - основной клиент для redis-ticks
- `DualTicksRedisClient` - клиент с fallback на основной Redis
- Singleton instances для переиспользования соединений

**Использование:**

```python
from core.ticks_redis_client import get_ticks_redis, get_dual_ticks_redis

# Для чтения тиков
ticks_redis = get_ticks_redis()
messages = ticks_redis.xreadgroup(...)

# Для записи с fallback
dual_ticks = get_dual_ticks_redis()
dual_ticks.xadd("stream:tick_XAUUSD", {...})
```

### 3. ✅ Обновлены Docker Compose Сервисы

**Сервисы, использующие redis-ticks:**

✅ **tick-ingest-server** - публикует тики

- `REDIS_URL=redis://redis-ticks:6379/0`
- `REDIS_SIGNALS_HOST=redis-ticks`

✅ **multi-symbol-orderflow** - читает тики

- `REDIS_TICKS_URL=redis://redis-ticks:6379/0`
- Consumer groups для каждого символа

✅ **py-obi-service** - работает с тиками для OBI

- `REDIS_URL=redis://redis-ticks:6379/0`

✅ **signal-performance-tracker** - читает тики для мониторинга

- `REDIS_TICKS_URL=redis://redis-ticks:6379/0`

✅ **ohlc-aggregator** - агрегирует тики в OHLC

- `REDIS_URL=redis://redis-ticks:6379/0`

✅ **aggregated-hub** - читает тики для сигналов

- `REDIS_TICKS_URL=redis://redis-ticks:6379/0`

✅ **signal-generator** - использует тики для TA

- `REDIS_URL=redis://redis-ticks:6379/0`

✅ **paper-executor** - читает тики для симуляции

- `REDIS_TICKS_URL=redis://redis-ticks:6379/0`

### 4. ✅ Makefile Команды

**Новые команды для управления redis-ticks:**

```bash
# Статус
make ticks-status

# Логи
make ticks-logs

# CLI доступ
make ticks-cli

# Информация
make ticks-info

# Streams
make ticks-streams

# Consumer groups
make ticks-groups

# Active consumers
make ticks-consumers

# Статистика
make ticks-stats

# Память
make ticks-memory

# Очистка старых тиков
make ticks-trim

# Тест подключения
make ticks-test

# Справка
make ticks-help
```

### 5. ✅ Документация

**Создана полная документация:**

- `TICKS_CONSUMER_GROUP_GUIDE.md` - подробное руководство
- `TICKS_REDIS_SETUP_COMPLETE.md` - эта сводка

## 🚀 Быстрый Старт

### 1. Запуск системы

```bash
# Остановка текущих сервисов
docker-compose down

# Запуск с новым redis-ticks
docker-compose up -d

# Проверка что redis-ticks запущен
make ticks-status
```

### 2. Проверка работоспособности

```bash
# Тест подключения
make ticks-test

# Проверка streams
make ticks-streams

# Проверка consumer groups
make ticks-groups
```

### 3. Мониторинг

```bash
# Логи redis-ticks
make ticks-logs

# Информация
make ticks-info

# Память
make ticks-memory
```

## 📊 Архитектура Consumer Groups

### Рекомендуемая структура

```
stream:tick_XAUUSD
├── ticks-orderflow-group (multi-symbol-orderflow)
├── ticks-ohlc-group (ohlc-aggregator)
├── ticks-tracker-group (signal-performance-tracker)
└── ticks-hub-group (aggregated-hub)

stream:tick_BTCUSD
├── ticks-orderflow-group (multi-symbol-orderflow)
└── ticks-hub-group (aggregated-hub)
```

### Naming Convention

✅ **Правильно:**

- `ticks-orderflow-group`
- `ticks-ohlc-aggregator-XAUUSD-group`
- `ticks-tracker-group`

❌ **Неправильно:**

- `group1`
- `consumer-group`
- `xauusd-group` (без префикса "ticks-")

## 🔧 Миграция Существующего Кода

### До миграции:

```python
from core.redis_client import get_redis

redis = get_redis()
messages = redis.xreadgroup(
    "xauusd-signal-group",
    "consumer-1",
    {"stream:tick_XAUUSD": '>'},
    count=100
)
```

### После миграции:

```python
from core.ticks_redis_client import get_ticks_redis

ticks_redis = get_ticks_redis()
messages = ticks_redis.xreadgroup(
    "ticks-orderflow-group",  # Новое имя группы
    "consumer-1",
    {"stream:tick_XAUUSD": '>'},
    count=100
)
```

## 📈 Performance Benefits

### Что улучшилось:

1. **Изоляция нагрузки**

   - Высокочастотные тики не влияют на основной Redis
   - Отдельные memory limits и eviction policies

2. **Оптимизированная конфигурация**

   - IO threads для multi-core processing
   - Увеличенный stream node size
   - LRU eviction для старых тиков

3. **Масштабируемость**

   - Можно независимо масштабировать redis-ticks
   - Легко добавлять новые consumer groups
   - Изолированные ресурсы для каждого типа данных

4. **Надежность**
   - Fallback mechanism через DualTicksRedisClient
   - Независимость от основного Redis
   - Отдельные backup/restore процедуры

## ⚠️ Important Notes

### Environment Variables

Все сервисы, работающие с тиками, теперь используют:

```yaml
REDIS_TICKS_URL=redis://redis-ticks:6379/0
REDIS_TICKS_HOST=redis-ticks
REDIS_TICKS_PORT=6379
```

### Dependencies

В `docker-compose.yml` добавлена зависимость:

```yaml
depends_on:
  redis-ticks:
    condition: service_healthy
```

### Volume

Создан отдельный volume для redis-ticks:

```yaml
volumes:
  scanner-redis-ticks-data:
    driver: local
```

## 🎓 Дополнительные Ресурсы

- **Полная документация**: `TICKS_CONSUMER_GROUP_GUIDE.md`
- **Python Client API**: `python-worker/core/ticks_redis_client.py`
- **Redis Streams Docs**: https://redis.io/docs/data-types/streams/
- **Consumer Groups Guide**: https://redis.io/docs/data-types/streams-tutorial/

## ✅ Checklist для Запуска

- [ ] Остановить текущие сервисы: `docker-compose down`
- [ ] Запустить с новым redis-ticks: `docker-compose up -d`
- [ ] Проверить статус: `make ticks-status`
- [ ] Проверить streams: `make ticks-streams`
- [ ] Проверить consumer groups: `make ticks-groups`
- [ ] Проверить логи сервисов: `make logs`
- [ ] Протестировать подключение: `make ticks-test`
- [ ] Проверить память: `make ticks-memory`

## 📞 Troubleshooting

### redis-ticks не запускается

```bash
# Проверка логов
make ticks-logs

# Проверка конфигурации
docker exec scanner-redis-ticks cat /usr/local/etc/redis/redis.conf

# Перезапуск
docker-compose restart redis-ticks
```

### Consumer groups не создаются

```bash
# Подключение к CLI
make ticks-cli

# Проверка stream
XINFO STREAM stream:tick_XAUUSD

# Создание группы вручную
XGROUP CREATE stream:tick_XAUUSD ticks-test-group $ MKSTREAM
```

### Out of memory

```bash
# Проверка памяти
make ticks-memory

# Trim старых тиков
make ticks-trim

# Увеличение maxmemory в redis-ticks.conf
```

---

**Дата**: 2025-11-05
**Версия**: 1.0
**Статус**: ✅ ЗАВЕРШЕНО
