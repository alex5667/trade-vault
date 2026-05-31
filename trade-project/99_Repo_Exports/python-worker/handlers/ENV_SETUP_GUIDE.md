# Environment Setup Guide - BaseOrderFlowHandler

## 📋 Обзор

Этот гайд показывает как применить рекомендуемые environment variables для `BaseOrderFlowHandler`.

## 🎯 Три способа настройки

### Способ 1: Через .env файл (рекомендуется для разработки)

#### Шаг 1: Скопируйте example файл

```bash
cd python-worker
cp env.example .env
```

#### Шаг 2: Отредактируйте .env

```bash
# Откройте в редакторе
nano .env

# Или используйте готовые значения (уже в env.example)
```

#### Шаг 3: Запустите handler

```bash
# .env файл автоматически загрузится
python -m handlers.crypto_orderflow_handler BTCUSDT
```

### Способ 2: Через docker-compose.yml (рекомендуется для production)

#### Вариант A: Прямо в environment секции

```yaml
services:
  python-worker-btcusdt:
    build: ./python-worker
    environment:
      # Redis
      - REDIS_URL=redis://scanner-redis:6379/0
      - REDIS_TICKS_URL=redis://redis-ticks:6379/0
      
      # BaseOrderFlowHandler - рекомендуемые значения
      - BREAKOUT_REQUIRE_OBI=true
      - OBI_SUSTAINED_USE_FRACTION=true
      - OBI_SUSTAINED_MIN_SAMPLES=3
      - OBI_SUSTAINED_MIN_FRACTION=0.6
      - BREAKOUT_Z_THRESHOLD=3.0
      - ABSORPTION_Z_THRESHOLD=3.0
      - EXTREME_Z_MULT=1.6
      - ABSORPTION_REQUIRE_WEAK_PROGRESS=true
      - ABSORPTION_USE_MICRO_PROXY=true
      - ABSORPTION_MICRO_ADVERSE_MIN=0.60
      - ABSORPTION_MICRO_REALIZED_EMA_MAX=-0.50
      
      # Instrument specific
      - BTCUSDT_DELTA_Z_THRESHOLD=2.7
      - BTCUSDT_OBI_THRESHOLD=0.35
```

#### Вариант B: Через env_file

```yaml
services:
  python-worker-btcusdt:
    build: ./python-worker
    env_file:
      - ./python-worker/.env
    environment:
      # Переопределить специфичные
      - SYMBOL=BTCUSDT
```

### Способ 3: Через export в shell (для тестирования)

```bash
# Экспортируйте переменные
export BREAKOUT_REQUIRE_OBI=true
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=3
export OBI_SUSTAINED_MIN_FRACTION=0.6
export BREAKOUT_Z_THRESHOLD=3.0
export ABSORPTION_Z_THRESHOLD=3.0
export EXTREME_Z_MULT=1.6

# Запустите handler
python -m handlers.crypto_orderflow_handler BTCUSDT
```

## 📊 Конфигурации по инструментам

### BTCUSDT (Crypto высокочастотный)

```bash
# Базовые параметры
export BTCUSDT_DELTA_Z_THRESHOLD=2.7
export BTCUSDT_OBI_THRESHOLD=0.35
export BTCUSDT_WEAK_PROGRESS_ATR=0.15

# Специфичные пороги
export BREAKOUT_Z_THRESHOLD=3.2
export ABSORPTION_Z_THRESHOLD=2.5
export EXTREME_Z_MULT=2.0

# Строгий sustained
export OBI_SUSTAINED_MIN_SAMPLES=5
export OBI_SUSTAINED_MIN_FRACTION=0.7

# Микроструктура
export ABSORPTION_USE_MICRO_PROXY=true
export ABSORPTION_MICRO_ADVERSE_MIN=0.65
export ABSORPTION_REQUIRE_WEAK_PROGRESS=false

# Bucketing
export DELTA_BUCKET_MS=1000
```

### ETHUSDT (Crypto высокочастотный)

```bash
# Базовые параметры
export ETHUSDT_DELTA_Z_THRESHOLD=2.5
export ETHUSDT_OBI_THRESHOLD=0.4
export ETHUSDT_WEAK_PROGRESS_ATR=0.15

# Аналогично BTCUSDT
export BREAKOUT_Z_THRESHOLD=3.2
export ABSORPTION_Z_THRESHOLD=2.5
export EXTREME_Z_MULT=2.0
export OBI_SUSTAINED_MIN_SAMPLES=5
export OBI_SUSTAINED_MIN_FRACTION=0.7
export ABSORPTION_USE_MICRO_PROXY=true
```

### XAUUSD (Commodities низкочастотный)

```bash
# Базовые параметры
export XAUUSD_DELTA_Z_THRESHOLD=3.0
export XAUUSD_OBI_THRESHOLD=0.5
export XAUUSD_WEAK_PROGRESS_ATR=0.10

# Стандартные пороги
export BREAKOUT_Z_THRESHOLD=3.0
export ABSORPTION_Z_THRESHOLD=2.8
export EXTREME_Z_MULT=1.6

# Мягкий sustained (меньше данных)
export OBI_SUSTAINED_MIN_SAMPLES=2
export OBI_SUSTAINED_MIN_FRACTION=0.5

# Без микроструктуры
export ABSORPTION_USE_MICRO_PROXY=false
export ABSORPTION_REQUIRE_WEAK_PROGRESS=true

# Больший bucket
export DELTA_BUCKET_MS=2000
```

## 🔧 Примеры docker-compose.yml

### Полный пример для crypto handler

```yaml
version: '3.8'

services:
  # Python Worker для BTCUSDT
  python-worker-btcusdt:
    build:
      context: ./python-worker
      dockerfile: Dockerfile
    container_name: scanner-python-worker-btcusdt
    environment:
      # === REDIS CONNECTIONS ===
      - REDIS_URL=redis://scanner-redis:6379/0
      - REDIS_TICKS_URL=redis://redis-ticks:6379/0
      - REDIS_SIGNALS_URL=redis://redis-worker-1:6379/0
      
      # === SYMBOL ===
      - SYMBOL=BTCUSDT
      
      # === BASE ORDERFLOW HANDLER ===
      # Строгий breakout
      - BREAKOUT_REQUIRE_OBI=true
      - BREAKOUT_Z_THRESHOLD=3.2
      
      # Absorption
      - ABSORPTION_Z_THRESHOLD=2.5
      - ABSORPTION_REQUIRE_WEAK_PROGRESS=false
      - ABSORPTION_USE_MICRO_PROXY=true
      - ABSORPTION_MICRO_ADVERSE_MIN=0.65
      - ABSORPTION_MICRO_REALIZED_EMA_MAX=-0.50
      
      # Extreme
      - EXTREME_Z_MULT=2.0
      
      # OBI Sustained
      - OBI_SUSTAINED_USE_FRACTION=true
      - OBI_SUSTAINED_MIN_SAMPLES=5
      - OBI_SUSTAINED_MIN_FRACTION=0.7
      
      # Other
      - DELTA_BUCKET_MS=1000
      - OBI_MAX_STALE_MS=2000
      - MAX_TICK_LAG_MS=5000
      
      # === INSTRUMENT SPECIFIC ===
      - BTCUSDT_DELTA_Z_THRESHOLD=2.7
      - BTCUSDT_OBI_THRESHOLD=0.35
      - BTCUSDT_WEAK_PROGRESS_ATR=0.15
      - BTCUSDT_MIN_SIGNAL_INTERVAL_SEC=60
      
      # === SIGNAL OUTBOX ===
      - USE_SIGNAL_OUTBOX=true
      - SIGNAL_DEDUP_TTL_MS=60000
      
      # === GPU ===
      - GPU_ENABLED=true
      
    depends_on:
      - redis
      - redis-ticks
      - redis-worker-1
    networks:
      - scanner-network
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '2.0'
```

### Пример с env_file

```yaml
services:
  python-worker-btcusdt:
    build: ./python-worker
    container_name: scanner-python-worker-btcusdt
    env_file:
      - ./python-worker/.env
    environment:
      # Переопределить только специфичные
      - SYMBOL=BTCUSDT
      - REDIS_URL=redis://scanner-redis:6379/0
    depends_on:
      - redis
    networks:
      - scanner-network
    restart: unless-stopped
```

## 🧪 Тестирование конфигурации

### Проверка переменных

```bash
# Проверить что переменные загружены
python -c "import os; print('BREAKOUT_REQUIRE_OBI:', os.getenv('BREAKOUT_REQUIRE_OBI'))"
python -c "import os; print('OBI_SUSTAINED_USE_FRACTION:', os.getenv('OBI_SUSTAINED_USE_FRACTION'))"
```

### Проверка в логах

При старте handler должен показать:

```
Init BaseOrderFlowHandler for BTCUSDT | ... |
Z: main=2.50 breakout=3.20 absorption=2.50 extreme=5.40 | OBI_thr=0.350 | bucket=1000ms |
breakout_strict_obi=True | OBI_sustained: use_frac=True min_samples=5 min_frac=0.70 | absorption_req_weak=False
```

### Dry-run тест

```bash
# Запустить handler в test mode
export DRY_RUN=true
python -m handlers.crypto_orderflow_handler BTCUSDT

# Проверить что параметры применились
# Смотрите в логи на строку "Init BaseOrderFlowHandler"
```

## 📈 Мониторинг

### Prometheus метрики

После применения конфигурации отслеживайте:

```promql
# Количество сигналов по типам
sum by (signal_kind) (rate(signals_generated_total[5m]))

# Win rate по типам
sum by (signal_kind) (signals_win_total) / sum by (signal_kind) (signals_total)

# Доля sustained OBI
rate(obi_sustained_total[5m]) / rate(obi_checks_total[5m])
```

### Grafana dashboard

Создайте панели для:
- Количество breakout/absorption/extreme сигналов
- Win rate по типам
- Средний Z-score по типам
- Доля sustained OBI
- Latency генерации сигналов

## ⚠️ Troubleshooting

### Проблема: Переменные не применяются

**Причина:** .env файл не загружается

**Решение:**
```bash
# Проверьте что файл существует
ls -la python-worker/.env

# Проверьте права
chmod 644 python-worker/.env

# Используйте python-dotenv
pip install python-dotenv

# В коде
from dotenv import load_dotenv
load_dotenv()
```

### Проблема: Значения не те что ожидаются

**Причина:** Переопределение в docker-compose

**Решение:**
```bash
# Проверьте приоритет:
# 1. environment в docker-compose (высший)
# 2. env_file
# 3. .env в коде (низший)

# Убедитесь что нет конфликтов
docker-compose config | grep BREAKOUT_REQUIRE_OBI
```

### Проблема: Handler не видит новые параметры

**Причина:** Старая версия кода

**Решение:**
```bash
# Пересоберите образ
docker-compose build python-worker-btcusdt

# Перезапустите
docker-compose restart python-worker-btcusdt

# Проверьте логи
docker-compose logs -f python-worker-btcusdt | grep "Init BaseOrderFlowHandler"
```

## 📚 Дополнительная документация

- **`RECOMMENDED_CONFIG.md`** — полное руководство по параметрам
- **`CONFIG_CHEATSHEET.md`** — быстрая шпаргалка
- **`OBI_SUSTAINED_IMPROVEMENTS.md`** — детали OBI sustained
- **`PER_SIGNAL_Z_THRESHOLDS.md`** — детали Z-порогов
- **`env.example`** — пример .env файла
- **`docker-compose.orderflow.env.example`** — пример для docker-compose

## 🚀 Быстрый старт

### Минимальная конфигурация (начать с этого)

```bash
# 1. Создать .env
cat > python-worker/.env << 'EOF'
BREAKOUT_REQUIRE_OBI=true
OBI_SUSTAINED_USE_FRACTION=true
OBI_SUSTAINED_MIN_SAMPLES=3
OBI_SUSTAINED_MIN_FRACTION=0.6
EOF

# 2. Запустить
python -m handlers.crypto_orderflow_handler BTCUSDT

# 3. Проверить логи
# Должно быть: "breakout_strict_obi=True | OBI_sustained: use_frac=True"
```

### Полная конфигурация (после тестов)

```bash
# 1. Скопировать example
cp python-worker/env.example python-worker/.env

# 2. Отредактировать под инструмент
nano python-worker/.env

# 3. Запустить
python -m handlers.crypto_orderflow_handler BTCUSDT

# 4. Мониторить метрики
# Grafana → OrderFlow Dashboard
```

---

**Версия:** 1.0  
**Дата:** 2025-11-29  
**Статус:** Production Ready

