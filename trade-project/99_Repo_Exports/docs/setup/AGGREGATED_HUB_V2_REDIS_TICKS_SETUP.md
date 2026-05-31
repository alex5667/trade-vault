# ✅ AggregatedSignalHub V2 - Интеграция с redis-ticks

## 🎯 Что было сделано

Обновлен `aggregated_signal_hub_v2.py` для работы с отдельным Redis instance для тиковых данных.

### 📋 Ключевые изменения

#### 1. Разделение Redis клиентов

**До:**

- Один Redis клиент для всех операций

**После:**

- `redis-ticks` → чтение тиков и принтов (высокочастотные данные)
- `scanner-redis` → запись сигналов, чтение ATR, DOM, pivots

#### 2. Новые зависимости

```python
from core.ticks_redis_client import get_ticks_redis, TicksRedisClient
```

#### 3. Конфигурация (HubConfig)

```python
@dataclass
class HubConfig:
    # Redis URLs - РАЗДЕЛЕНО
    redis_url: str = "redis://scanner-redis-worker-1:6379/0"  # Для сигналов, ATR, DOM
    redis_ticks_url: str = "redis://redis-ticks:6379/0"       # Для тиков и принтов
```

#### 4. Consumer Groups с префиксом "ticks-"

**До:**

```python
group_name = f"hub_v2_{symbol}"
```

**После:**

```python
group_name = f"ticks-hub-v2-{symbol}"
```

Согласно архитектуре redis-ticks (см. `TICKS_REDIS_SETUP_COMPLETE.md`).

## 🚀 Запуск

### 1. Environment Variables

```yaml
# docker-compose.yml
aggregated-hub:
  environment:
    # Основной Redis для сигналов, ATR, DOM
    - REDIS_URL=redis://scanner-redis-worker-1:6379/0

    # Redis для тиков (высокочастотные данные)
    - REDIS_TICKS_URL=redis://redis-ticks:6379/0

    # Streams
    - TICK_STREAM=stream:tick_XAUUSD
    - PRINTS_STREAM=trades:prints_XAUUSD

    # Остальные настройки...
```

### 2. Проверка подключения

```bash
# Проверка redis-ticks
make ticks-status
make ticks-test

# Проверка consumer groups
make ticks-groups

# Логи aggregated-hub
docker logs -f scanner-aggregated-hub
```

### 3. Запуск с docker-compose

```bash
# Полная система
docker-compose up -d

# Только aggregated-hub (если уже запущено)
docker-compose restart aggregated-hub
```

## 📊 Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                 AggregatedSignalHub V2                      │
└─────────────────────────────────────────────────────────────┘
                           │
          ┌────────────────┴────────────────┐
          │                                 │
          ▼                                 ▼
┌──────────────────────┐         ┌──────────────────────┐
│    redis-ticks       │         │   scanner-redis      │
│  (scanner-redis-ticks)│         │ (scanner-redis-worker)│
├──────────────────────┤         ├──────────────────────┤
│ ✓ Тики               │         │ ✓ Сигналы            │
│ ✓ Принты (trades)    │         │ ✓ ATR                │
│ ✓ Consumer groups    │         │ ✓ DOM (book:levels)  │
│                      │         │ ✓ Pivots             │
│ Port: 6379           │         │ Port: 6379           │
│ Memory: 10GB         │         │ Memory: 16GB         │
└──────────────────────┘         └──────────────────────┘
```

## 🔍 Consumer Groups

### Naming Convention

Все consumer groups для redis-ticks используют префикс **"ticks-"**:

```
stream:tick_XAUUSD
├── ticks-hub-v2-XAUUSD       ← AggregatedSignalHub V2
├── ticks-orderflow-group     ← multi-symbol-orderflow
├── ticks-ohlc-aggregator     ← OHLC aggregator
└── ticks-tracker-group       ← signal-performance-tracker
```

### Проверка consumer groups

```bash
# Через Makefile
make ticks-groups

# Напрямую в Redis CLI
docker exec scanner-redis-ticks redis-cli XINFO GROUPS stream:tick_XAUUSD
```

## 📝 Логи и мониторинг

### Startup лог

При запуске aggregated-hub теперь выводится:

```
================================================================================
AggregatedSignalHubV2 starting
Symbol: XAUUSD
Mode: live
================================================================================
Redis Configuration:
  Signals/ATR/DOM: redis://scanner-redis-worker-1:6379/0
  Ticks/Prints:    redis://redis-ticks:6379/0
================================================================================
Streams:
  Tick stream:   stream:tick_XAUUSD
  Prints stream: trades:prints_XAUUSD
================================================================================
Thresholds:
  Confidence threshold: 0.25
  Min signal interval:  180s
================================================================================
✅ Connected to redis-ticks: redis://redis-ticks:6379/0
✅ Pro detector (true delta) enabled
✅ Legacy detector enabled with 'update' method
✅ Cluster analyzer (DOM) enabled
✅ Consumer group created/exists: stream:tick_XAUUSD (group=ticks-hub-v2-XAUUSD)
🚀 Entering main processing loop...
```

### Проблемы с подключением

Если redis-ticks недоступен, будет fallback на основной Redis:

```
⚠️  Failed to connect to redis-ticks, falling back to main Redis: ...
⚠️  TicksRedisClient not available, using main Redis for ticks
```

## 🛠️ Troubleshooting

### redis-ticks не отвечает

```bash
# 1. Проверка статуса
make ticks-status

# 2. Логи
make ticks-logs

# 3. Перезапуск
docker-compose restart redis-ticks

# 4. Тест подключения
make ticks-test
```

### Consumer group уже существует

Это нормально. При первом запуске group создается автоматически:

```
ℹ️ Consumer group 'ticks-hub-v2-XAUUSD' уже существует для stream:tick_XAUUSD
```

### Нет тиков в stream

```bash
# Проверка streams
make ticks-streams

# Проверка длины stream
docker exec scanner-redis-ticks redis-cli XLEN stream:tick_XAUUSD

# Если 0, проверьте tick-ingest-server
docker logs scanner-tick-ingest-server
```

## 📚 Связанные документы

- `TICKS_REDIS_SETUP_COMPLETE.md` - Полная документация по redis-ticks
- `TICKS_CONSUMER_GROUP_GUIDE.md` - Руководство по consumer groups
- `python-worker/core/ticks_redis_client.py` - Python клиент для redis-ticks
- `redis-ticks.conf` - Конфигурация redis-ticks

## ✅ Checklist

- [x] Обновлен `aggregated_signal_hub_v2.py`
  - [x] Импорт `TicksRedisClient`
  - [x] Разделение Redis клиентов
  - [x] Consumer groups с префиксом "ticks-"
  - [x] Обновлена документация в коде
- [x] Environment variables в `docker-compose.yml`
  - [x] `REDIS_TICKS_URL`
  - [x] Зависимость от `redis-ticks`
- [x] Документация создана
- [ ] Протестировать в production

## 🎓 Примеры использования

### Standalone (вне Docker)

```bash
# Экспорт переменных окружения
export REDIS_URL=redis://localhost:6379/0
export REDIS_TICKS_URL=redis://localhost:6380/0
export SYMBOL=XAUUSD
export TICK_STREAM=stream:tick_XAUUSD
export HUB_CONFIDENCE_THR=0.25

# Запуск
cd python-worker
python aggregated_signal_hub_v2.py --mode=live
```

### Replay mode (offline testing)

```bash
# С использованием CSV файла
python aggregated_signal_hub_v2.py \
  --mode=replay \
  --replay-csv=/path/to/trades.csv \
  --replay-speed=1.0 \
  --max-rows=10000
```

---

**Дата**: 2025-11-05  
**Версия**: 1.0  
**Статус**: ✅ ЗАВЕРШЕНО
