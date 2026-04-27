# ⚙️ Configuration Guide

## Обзор

Система использует множество конфигурационных файлов для настройки различных компонентов. Этот документ описывает все доступные опции и рекомендуемые настройки.

## 📦 Docker Compose Configuration

### Основной файл: docker-compose.yml

Файл содержит определения всех сервисов системы.

#### Структура сервисов

```yaml
services:
  # ===== REDIS CLUSTER (3 instances) =====
  redis: # Main Redis (port 6379, external)
  redis-worker-1: # Worker Redis 1 (internal)
  redis-worker-2: # Worker Redis 2 (internal)

  # ===== GO WORKERS (10 instances) =====
  go-worker-1m: # 1 minute timeframe
  go-worker-5m: # 5 minutes
  go-worker-15m: # 15 minutes
  go-worker-1h: # 1 hour
  go-worker-4h: # 4 hours
  go-worker-1d: # 1 day
  go-worker-1w: # 1 week
  go-worker-1month: # 1 month
  go-worker-3month: # 3 months
  go-worker-1y: # 1 year

  # ===== PYTHON SERVICES =====
  python-worker: # Legacy order flow handler (deprecated)
  multi-symbol-orderflow: # New multi-symbol handler
  ohlc-aggregator: # Daily OHLC from ticks
  atr-worker: # ATR calculator

  # ===== SIGNAL GENERATION =====
  signal-generator: # Technical Analysis signals
  aggregated-hub: # Signal aggregation v2
  signal-hub: # Signal hub pro

  # ===== INFRASTRUCTURE =====
  tick-ingest-server: # MT5 tick ingest (HTTP)
  py-obi-service: # Order Book Imbalance
  go-gateway: # Order routing + Telegram
  paper-executor: # Paper trading

  # ===== TELEGRAM =====
  telegram-worker: # Channel listener
  signal-parser-worker: # Signal parser
  notify-worker: # Notifications sender

  # ===== MONITORING =====
  prometheus: # Metrics storage
  grafana: # Dashboards

  # ===== UTILITIES =====
  redis-cleanup: # Stream cleanup
  stream-trimmer: # Batch trimmer
  regime-worker: # Market regime detection
  regime-quantiles-job: # Regime quantiles
  dom-ingester: # DOM data ingester
```

### Ключевые параметры конфигурации

#### 1. Resource Limits

**Рекомендуемые лимиты для production**:

```yaml
deploy:
  resources:
    limits:
      memory: 2G # Максимум памяти
      cpus: '2.0' # Максимум CPU cores
    reservations:
      memory: 512M # Гарантированная память
      cpus: '0.5' # Гарантированный CPU
```

**Breakdown по сервисам**:

| Сервис                 | Memory Limit | CPU Limit | Примечание     |
| ---------------------- | ------------ | --------- | -------------- |
| redis                  | 16G          | 4.0       | Main storage   |
| redis-worker-1/2       | 3G           | 2.0       | Stream workers |
| go-worker-1m           | 1.5G         | 1.5       | Highest load   |
| go-worker-5m           | 1.2G         | 1.0       | Medium load    |
| go-worker-15m+         | 0.4-1.0G     | 0.25-0.8  | Lower load     |
| python-worker          | 1G           | 1.0       | Order flow     |
| multi-symbol-orderflow | 2G           | 2.0       | Multi-symbol   |
| signal-generator       | 512M         | 0.5       | TA analysis    |
| aggregated-hub         | 768M         | 0.75      | Hub v2         |
| go-gateway             | 512M         | 0.5       | Gateway        |
| telegram-worker        | 1G           | 1.0       | Multi-threaded |

#### 2. Health Checks

**Типичный health check**:

```yaml
healthcheck:
  test: ['CMD', 'curl', '-f', 'http://localhost:8090/healthz']
  interval: 30s # Проверка каждые 30 сек
  timeout: 5s # Таймаут ответа
  retries: 3 # Количество попыток
  start_period: 10s # Период прогрева
```

**Варианты проверок**:

```yaml
# HTTP endpoint
test: ['CMD', 'curl', '-f', 'http://localhost:PORT/health']

# Redis ping
test: ['CMD', 'redis-cli', 'ping']

# Process check
test: ['CMD-SHELL', 'pgrep -f main.py || exit 1']

# Wget (для Alpine images)
test: ['CMD', 'wget', '--spider', 'http://localhost:PORT/metrics']
```

#### 3. Restart Policies

```yaml
restart: unless-stopped   # Production сервисы (всегда перезапуск)
restart: on-failure:5     # Workers (5 попыток, потом stop)
restart: no              # One-time tasks
```

#### 4. Dependencies

**Порядок запуска** (критически важен):

```yaml
depends_on:
  redis:
    condition: service_healthy # Ждать health check
  go-worker-1m:
    condition: service_started # Просто запущен
```

**Типичная последовательность**:

1. Redis cluster (20-30s)
2. Go workers (40s)
3. Python workers (60s+)
4. Supporting services

## 🔴 Redis Configuration

### Файлы конфигурации

- `redis-external-access.conf` - Main Redis (6379)
- `redis-worker-stable.conf` - Worker Redis instances
- `redis-cleanup.conf` - Cleanup service
- `redis-optimized.conf` - Optimized variant
- `redis-stable.conf` - Stable variant

### redis-external-access.conf (Main Redis)

#### Сетевые настройки

```conf
port 6379
bind 0.0.0.0              # Слушать все интерфейсы
protected-mode no         # Отключен (Docker network)
tcp-keepalive 30          # Keepalive каждые 30 сек
tcp-backlog 4096          # Очередь TCP соединений
timeout 0                 # Никогда не закрывать idle connections
```

#### Управление памятью

```conf
maxmemory 13gb                    # 13GB лимит (из 16GB)
maxmemory-policy volatile-lru     # Evict только ключи с TTL
maxmemory-samples 5               # Sampling для LRU
```

**Политики eviction**:

- `volatile-lru`: Удаляет ключи с EXPIRE по LRU (рекомендуется)
- `allkeys-lru`: Удаляет любые ключи по LRU
- `volatile-ttl`: Удаляет ключи с наименьшим TTL
- `noeviction`: Ошибка при переполнении (не рекомендуется)

#### Лимиты клиентов

```conf
maxclients 10000                              # Максимум подключений

# Output buffers
client-output-buffer-limit normal 0 0 0       # Безлимит для normal
client-output-buffer-limit replica 256mb 64mb 60
client-output-buffer-limit pubsub 64mb 16mb 60

# Query buffers
client-query-buffer-limit 256mb
proto-max-bulk-len 256mb
```

#### Persistence (AOF)

```conf
# RDB отключен для производительности
save ""
stop-writes-on-bgsave-error no

# AOF включен для надежности
appendonly yes
appendfilename "appendonly.aof"
appendfsync everysec              # Fsync каждую секунду (баланс)
no-appendfsync-on-rewrite yes     # Не fsync во время rewrite
auto-aof-rewrite-percentage 100   # Rewrite когда рост 100%
auto-aof-rewrite-min-size 64mb    # Минимум 64MB для rewrite
```

**Варианты appendfsync**:

- `always`: Каждая команда (медленно, максимальная надежность)
- `everysec`: Каждую секунду (рекомендуется, баланс)
- `no`: ОС решает (быстро, минимальная надежность)

#### Оптимизация производительности

```conf
# Lazy free (асинхронное удаление)
lazyfree-lazy-eviction yes
lazyfree-lazy-expire yes
lazyfree-lazy-server-del yes
replica-lazy-flush yes

# Active defragmentation
activedefrag yes
active-defrag-ignore-bytes 100mb
active-defrag-threshold-lower 10
active-defrag-threshold-upper 20
active-defrag-cycle-min 5
active-defrag-cycle-max 75

# I/O threads (Redis 6.0+)
io-threads 4
io-threads-do-reads yes
```

#### Logging

```conf
loglevel notice          # notice | verbose | debug | warning
logfile ""               # stdout (для Docker)
syslog-enabled no
```

### redis-worker-stable.conf (Workers)

Упрощенная конфигурация для внутренних workers:

```conf
port 6379
bind 0.0.0.0
protected-mode no

maxmemory 2.5gb
maxmemory-policy volatile-lru
maxclients 5000

# AOF включен
appendonly yes
appendfsync everysec

# Оптимизации
lazyfree-lazy-eviction yes
activedefrag yes
io-threads 2
```

## 🔧 Environment Variables

### Go Workers

```bash
# Redis connections
REDIS_HOST=redis-worker-1         # Primary Redis host
REDIS_PORT=6379                   # Redis port
REDIS_DB=0                        # Database number

# Connection pool
REDIS_POOL_SIZE=150               # Max connections
REDIS_MIN_IDLE_CONNS=15           # Min idle connections
REDIS_MAX_RETRIES=5               # Retry attempts
REDIS_RETRY_MIN_BACKOFF=100ms     # Min backoff
REDIS_RETRY_MAX_BACKOFF=3s        # Max backoff

# Timeouts
REDIS_DIAL_TIMEOUT=10s            # Connection timeout
REDIS_READ_TIMEOUT=30s            # Read timeout
REDIS_WRITE_TIMEOUT=30s           # Write timeout
REDIS_POOL_TIMEOUT=10s            # Pool timeout

# Binance WebSocket
BINANCE_WS_TIMEFRAME=kline_1m     # Timeframe (kline_1m, kline_5m...)

# Dual Redis publishing
REDIS_CANDLES_HOST=redis-worker-1
REDIS_CANDLES_PORT=6379
REDIS_CANDLES_HOST_2=redis-worker-2
REDIS_CANDLES_PORT_2=6379

# Prometheus
PROMETHEUS_PORT=2112              # Metrics port (unique per worker)
```

### Python Workers

```bash
# Redis
REDIS_HOST=scanner-redis-worker-1
REDIS_PORT=6379
REDIS_DB=0
REDIS_URL=redis://scanner-redis:6379/0

# Connection settings
REDIS_SOCKET_TIMEOUT=30
REDIS_SOCKET_CONNECT_TIMEOUT=10
REDIS_MAX_CONNECTIONS=100
REDIS_RETRY_ON_TIMEOUT=true
REDIS_HEALTH_CHECK_INTERVAL=30

# Dual Redis
REDIS_SIGNALS_HOST=redis-worker-1
REDIS_SIGNALS_PORT=6379
REDIS_SIGNALS_HOST_2=redis-worker-2
REDIS_SIGNALS_PORT_2=6379
```

### Multi-Symbol OrderFlow Handler

```bash
# ===== СИМВОЛЫ =====
# Статический режим (требует перезапуск для изменения)
SYMBOLS=XAUUSD

# Динамический режим (hot-reload без перезапуска)
# DYNAMIC_SYMBOLS=true
# SYMBOL_CONFIG_STREAM=config:symbols

# ===== XAU CONFIGURATION =====
XAU_TICK_STREAM=stream:tick_XAUUSD
XAU_BOOK_STREAM=stream:book_XAUUSD

# Delta analysis
XAU_DELTA_WINDOW=120              # Window size
XAU_DELTA_Z_THRESHOLD=3.0         # Z-score threshold
XAU_WEAK_PROGRESS_ATR=0.10        # ATR multiplier

# OBI detection
XAU_OBI_THRESHOLD=0.5             # OBI threshold (0-1)
XAU_OBI_MIN_DURATION=2.0          # Min duration (seconds)

# Iceberg detection
XAU_ICEBERG_REFRESH=2             # Refresh count
XAU_ICEBERG_DURATION=1.5          # Duration (seconds)
XAU_ICEBERG_REFRESH_MIN_ABS=1.0   # Min absolute volume

# Filters
XAU_DIST_ATR_THRESHOLD=0.5        # Distance threshold
XAU_MIN_SIGNAL_INTERVAL=60        # Min interval (seconds)

# Stream reading
XAU_READ_COUNT=100                # Messages per read
XAU_READ_BLOCK_MS=1000            # Block timeout (ms)

# ===== BTCUSD CONFIGURATION =====
BTCUSD_TICK_STREAM=stream:tick_BTCUSD
BTCUSD_BOOK_STREAM=stream:book_BTCUSD

BTC_DELTA_WINDOW=120
BTC_DELTA_Z_THRESHOLD=2.5         # Ниже чем XAU (крипта волатильнее)
BTC_WEAK_PROGRESS_ATR=0.15        # Выше чем XAU
BTC_OBI_THRESHOLD=0.4
BTC_MIN_SIGNAL_INTERVAL=30        # Чаще чем XAU

# ===== ETHUSD CONFIGURATION =====
ETHUSD_TICK_STREAM=stream:tick_ETHUSD
ETHUSD_BOOK_STREAM=stream:book_ETHUSD
# ... (аналогично BTCUSD)

# ===== ОБЩИЕ НАСТРОЙКИ =====
# ATR
ATR_SOURCE=redis                  # redis | ticks
ATR_TF=1m                         # Timeframe для ATR

# Risk Management
STOP_MODE=ATR                     # ATR | PCT | POINTS
STOP_ATR_MULT=0.6
STOP_PCT=0.2
STOP_POINTS=1.0

TP_MODE=RR                        # RR | ATR | PCT
TP_RR=1,2,3                       # Risk:Reward ratios
TP_ATR_MULTS=0.6,1.0,1.5

# Publishing
NOTIFY_STREAM=notify:telegram
USE_TELEGRAM_BUTTONS=0
SNAP_PREFIX=signal:snap
SNAP_TTL=21600                    # 6 hours

# Health check
HEALTH_CHECK_INTERVAL=60          # Seconds
```

### Signal Generator (TA)

```bash
# Services
GATEWAY_URL=http://scanner-go-gateway:8090
OBI_SERVICE_URL=http://py-obi-service:8088

# Trading
SYMBOL=XAUUSD
TIMEFRAME=M1                      # M1, M5, M15, H1, H4, D1
CHECK_INTERVAL=30                 # Seconds between checks

# Data source
USE_REAL_TICKS=true               # true | false
REDIS_URL=redis://scanner-redis-worker-1:6379/0
TICK_STREAM=stream:tick_XAUUSD

# Technical Indicators
EMA_FAST=9
EMA_SLOW=21
RSI_PERIOD=14
RSI_OVERSOLD=35                   # Buy signal below
RSI_OVERBOUGHT=65                 # Sell signal above
ATR_PERIOD=14
ATR_SL_MULTIPLIER=1.5
ATR_TP_MULTIPLIERS=2.0,3.0,4.0

# Risk
DEFAULT_LOT=0.01
MAX_LOT=0.1
RISK_PERCENT=5.0

# Streams
TA_STREAM=signals:ta:XAUUSD
```

### Aggregated Hub V2

```bash
REDIS_URL=redis://scanner-redis-worker-1:6379/0
SYMBOL=XAUUSD

# Input streams
TICK_STREAM=stream:tick_XAUUSD
PRINTS_STREAM=trades:prints_XAUUSD
ORDERFLOW_STREAM=signals:orderflow:XAUUSD
TA_STREAM=signals:ta:XAUUSD

# Output
NOTIFY_STREAM=notify:telegram
GATEWAY_URL=http://scanner-go-gateway:8090
GATEWAY_PUSH_PATH=/orders/push

# Logging
LOG_LEVEL=DEBUG                   # DEBUG | INFO | WARNING | ERROR

# Hub V2 thresholds
HUB_CONFIDENCE_THR=0.25           # Min confidence (0-1)
HUB_MIN_SIG_INT_SEC=180           # Min signal interval (seconds)

# Confidence blending weights (should sum to 1.0)
W_DELTA_PRO=0.50                  # Delta confidence weight
W_SPEED=0.15                      # Speed confidence weight
W_CLUSTER=0.25                    # Cluster confidence weight
W_LEGACY=0.10                     # Legacy OrderFlow weight

# Anti-dither
HUB_SIDE_LOCK_SEC=20              # Lock side for N seconds

# Writer config
MIN_CONF=20.0                     # Min confidence percent
HUB_COOLDOWN=300                  # Cooldown (seconds)
RISK_PCT=1.0
SL_MULT=1.5
TP_MULTS=2.0,3.0,4.0

# Parquet labels (для ML)
PARQUET_LABELS_DIR=/data/labels

# Redis keys
BOOK_LAST_KEY=book:levels:XAUUSD
PIVOTS_KEY=pivots:latest

# Risk position sizing
USE_RISK_SIZER=true
ACCOUNT_BALANCE=10000.0
SYMBOL_POINT=0.1
TICK_VALUE_PER_LOT=1.0
LOT_STEP=0.01
MIN_LOT=0.01
MAX_LOT=10.0
```

### Go Gateway

```bash
# Server
PORT=8090

# Telegram Bot
TELEGRAM_BOT_TOKEN=<your_bot_token>
TELEGRAM_CHAT_ID=<your_chat_id>

# OBI Service
OBI_HOST=http://py-obi-service:8088

# Redis
REDIS_URL=redis://scanner-redis:6379/0

# Trading
SYMBOL=XAUUSD
BOOK_LAST_KEY=book:levels:XAUUSD
PIVOTS_LAST_KEY=pivots:latest
ATR_LAST_KEY=ta:last:atr:XAUUSD

# Mode
PAPER_MODE=1                      # 1 = paper, 0 = live

# SSE stream
SSE_INTERVAL_MS=1000              # SSE update interval
DOM_LEVELS_LIMIT=20               # DOM levels to show

# Fallback specs
SPEC_POINT=0.1
SPEC_TICK_VALUE_PER_LOT=1.0
SPEC_CONTRACT_SIZE=0
```

### Telegram Worker

```bash
PYTHONUNBUFFERED=1
SESSIONS_DIR=/app/sessions

# Redis
REDIS_URL=redis://scanner-redis-worker-1:6379/0

# Streams
STREAM_NAME=signal:telegram:raw
RAW_STREAM=signal:telegram:raw
NOTIFY_STREAM=notify:telegram

# TTL
PARSED_TTL_SECONDS=3600

# Channels (comma-separated или Redis key)
CHANNELS=                         # Пустое = из Redis
CHANNELS_REDIS_KEY=telegram:channels:usernames
CHANNELS_REFRESH_SEC=60

# Whitelist
TG_WHITELIST=                     # Пустое = все каналы

# Multi-threading
MAX_THREADS=5
CHANNELS_PER_THREAD=20
```

**Примечание**: Telegram credentials (`TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_PHONE`) должны быть в файле `telegram-worker/.env`.

### ATR Worker

```bash
REDIS_URL=redis://scanner-redis-worker-1:6379/0
CANDLES_STREAM=candles:data
ATR_GROUP=atr-worker-group
ATR_CONSUMER=atr-worker-1
ATR_PERIOD=14                     # ATR period
ATR_SYMBOLS=XAUUSD                # Comma-separated
ATR_TFS=1m,5m,15m                 # Timeframes
PYTHONUNBUFFERED=1
```

## 📝 Makefile Configuration

### Основные переменные

```makefile
SHELL := /bin/bash
export TZ := UTC                  # Timezone для всех команд
```

### Примеры команд

```bash
# Запуск системы
make up                           # С логами
make up-bg                        # В фоне

# Управление
make down                         # Остановка
make restart                      # Перезапуск
make rebuild                      # Пересборка

# Мониторинг
make status                       # Статус контейнеров
make logs                         # Все логи
make health                       # Health checks
make diagnose                     # Диагностика

# По сервисам
make signal-status                # Статус signal-generator
make hub-logs                     # Логи aggregated-hub
make redis-stats                  # Redis статистика

# Dynamic symbols
make symbol-add SYMBOL=BTCUSD
make symbol-remove SYMBOL=BTCUSD
make symbol-list
```

## 🎛️ Оптимизация производительности

### System-level Limits

```bash
# /etc/sysctl.conf
net.core.somaxconn=65535
vm.overcommit_memory=1
net.ipv4.tcp_max_syn_backlog=4096
```

### Docker Compose System Settings

```yaml
ulimits:
  nofile:
    soft: 65536
    hard: 65536
  memlock:
    soft: -1
    hard: -1

sysctls:
  net.core.somaxconn: 65535

shm_size: 2gb # Для AOF rewrite
```

### Go Worker Optimization

```bash
# В Go коде
runtime.GOMAXPROCS(4)             # Использовать 4 CPU cores
```

### Python Worker Optimization

```bash
# В environment
PYTHONUNBUFFERED=1                # Отключить буферизацию
OMP_NUM_THREADS=2                 # Numpy/Pandas threads
```

## 🔐 Secrets Management

### .env файлы

**telegram-worker/.env**:

```bash
TELEGRAM_API_ID=<your_api_id>
TELEGRAM_API_HASH=<your_api_hash>
TELEGRAM_PHONE=<your_phone>
TELEGRAM_BOT_TOKEN=<bot_token>
TELEGRAM_CHAT_ID=<chat_id>
```

**Важно**: Добавить в `.gitignore`:

```
telegram-worker/.env
*.session
```

## 📊 Monitoring Configuration

### Prometheus (prometheus.yml)

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'go-workers'
    static_configs:
      - targets:
          - 'go-worker-1m:2112'
          - 'go-worker-5m:2113'
        # ... etc

  - job_name: 'redis'
    static_configs:
      - targets:
          - 'redis:6379'
```

### Grafana

```bash
# Access
URL: http://localhost:3001
Login: admin
Password: admin

# Data Source
Type: Prometheus
URL: http://prometheus:9090
```

---

**Все конфигурации оптимизированы для production использования с учетом:**

- ✅ Производительности
- ✅ Надежности
- ✅ Масштабируемости
- ✅ Мониторинга
