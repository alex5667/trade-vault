# 🏗️ Architecture Documentation

## Обзор архитектуры

Scanner Infrastructure построен на микросервисной архитектуре с четким разделением ответственности между компонентами. Система использует event-driven подход с Redis Streams в качестве message broker.

## 🎯 Архитектурные принципы

### 1. Разделение по уровням

```
┌─────────────────────────────────────────────────────────────────┐
│ УРОВЕНЬ 1: DATA INGESTION (Получение данных)                   │
│ - Go Workers (Binance WebSocket)                                │
│ - Tick Ingest Server (MT5 HTTP)                                 │
│ - Telegram Worker (Telegram API)                                │
└─────────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│ УРОВЕНЬ 2: DATA STORAGE (Хранение и кеширование)              │
│ - Redis Main (6379)                                             │
│ - Redis Worker-1 (внутренний)                                  │
│ - Redis Worker-2 (внутренний)                                  │
└─────────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│ УРОВЕНЬ 3: PROCESSING (Обработка и анализ)                    │
│ - Python Order Flow Handlers                                    │
│ - ATR Workers                                                    │
│ - Regime Workers                                                 │
│ - OHLC Aggregators                                              │
└─────────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│ УРОВЕНЬ 4: SIGNAL GENERATION (Генерация сигналов)             │
│ - Signal Generator (Technical Analysis)                         │
│ - Multi-Symbol OrderFlow Handler                                │
│ - Signal Parser (Telegram)                                       │
└─────────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│ УРОВЕНЬ 5: AGGREGATION (Агрегация и фильтрация)               │
│ - Aggregated Hub V2                                             │
│ - Signal Hub Pro                                                │
│ - DOM Ingester                                                  │
└─────────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│ УРОВЕНЬ 6: EXECUTION (Исполнение и уведомления)               │
│ - Go Gateway (Order Queue, Telegram Bot)                        │
│ - Paper Executor                                                │
│ - Notify Worker                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2. Event-Driven Architecture

Все компоненты общаются через Redis Streams:

- **Loose coupling**: Сервисы не знают друг о друге напрямую
- **Scalability**: Можно добавлять consumer'ы без изменения producers
- **Reliability**: Consumer groups обеспечивают guaranteed delivery
- **Replay**: Возможность повторной обработки исторических данных

### 3. Polyglot Architecture

- **Go**: High-performance I/O (WebSocket, HTTP), низкая latency
- **Python**: Data analysis, ML, научные библиотеки (numpy, pandas)

## 📊 Детальная архитектура компонентов

### Go Workers Layer

#### Назначение

Получение рыночных данных с Binance WebSocket API для всех таймфреймов.

#### Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                    Go Worker Instance                        │
│                                                              │
│  ┌──────────────┐        ┌──────────────┐                  │
│  │  WebSocket   │───────▶│  Kline       │                  │
│  │  Connection  │        │  Parser      │                  │
│  └──────────────┘        └──────┬───────┘                  │
│                                  │                           │
│  ┌──────────────┐        ┌──────▼───────┐                  │
│  │  Connection  │        │  Redis       │                  │
│  │  Manager     │        │  Publisher   │                  │
│  └──────────────┘        └──────┬───────┘                  │
│                                  │                           │
│  ┌──────────────┐        ┌──────▼───────┐                  │
│  │  Health      │        │  Prometheus  │                  │
│  │  Check       │        │  Metrics     │                  │
│  └──────────────┘        └──────────────┘                  │
└─────────────────────────────────────────────────────────────┘
```

#### Ключевые характеристики

- **10 независимых workers**: по одному на каждый таймфрейм
- **Dual Redis publishing**: данные дублируются на redis-worker-1 и redis-worker-2
- **Auto-reconnect**: автоматическое переподключение при разрыве
- **Graceful shutdown**: корректная остановка с закрытием соединений
- **Resource limits**: CPU и память ограничены через Docker

#### Публикуемые данные

**Stream**: `candles:data`

```json
{
	"symbol": "BTCUSDT",
	"timeframe": "1m",
	"open_time": 1699999999000,
	"close_time": 1700000059000,
	"open": "43250.50",
	"high": "43280.00",
	"low": "43240.00",
	"close": "43265.75",
	"volume": "125.45",
	"quote_volume": "5425678.90",
	"trades": 1523,
	"taker_buy_base": "65.23",
	"taker_buy_quote": "2821345.67"
}
```

### Python Workers Layer

#### Multi-Symbol OrderFlow Handler

Унифицированный обработчик для анализа order flow по множеству символов.

```
┌─────────────────────────────────────────────────────────────┐
│              Multi-Symbol OrderFlow Handler                  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Symbol Configuration Manager                  │  │
│  │  - XAUUSD config                                      │  │
│  │  - BTCUSD config                                      │  │
│  │  - ETHUSD config                                      │  │
│  └─────────────────┬────────────────────────────────────┘  │
│                    │                                         │
│  ┌─────────────────▼────────────────────────────────────┐  │
│  │         Handler Factory (>85% code reuse)            │  │
│  │  - BaseOrderFlowHandler                              │  │
│  │  - Symbol-specific parameters                        │  │
│  └─────────────────┬────────────────────────────────────┘  │
│                    │                                         │
│       ┌────────────┼────────────┐                           │
│       │            │            │                           │
│  ┌────▼────┐  ┌────▼────┐  ┌───▼─────┐                    │
│  │ XAUUSD  │  │ BTCUSD  │  │ ETHUSD  │                    │
│  │ Handler │  │ Handler │  │ Handler │                    │
│  └────┬────┘  └────┬────┘  └───┬─────┘                    │
│       │            │            │                           │
│       └────────────┼────────────┘                           │
│                    │                                         │
│  ┌─────────────────▼────────────────────────────────────┐  │
│  │         Signal Aggregation & Publishing              │  │
│  │  - signals:orderflow:XAUUSD                          │  │
│  │  - signals:orderflow:BTCUSD                          │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Основные компоненты**:

1. **Delta Analyzer**: Анализ дельты объемов
2. **OBI Detector**: Order Book Imbalance detection
3. **Iceberg Detector**: Обнаружение скрытых ордеров
4. **Cluster Analyzer**: Анализ кластеров объема
5. **Speed Monitor**: Скорость движения цены

#### Order Flow Detection Logic

```python
# Pseudocode для Order Flow анализа

class OrderFlowAnalyzer:
    def analyze_tick_batch(self, ticks):
        # 1. Вычисление дельты
        delta = sum(tick.volume if tick.is_buyer else -tick.volume
                   for tick in ticks)

        # 2. Z-score дельты
        z_score = (delta - mean(recent_deltas)) / std(recent_deltas)

        # 3. Проверка порогов
        if abs(z_score) > DELTA_Z_THRESHOLD:
            # Сильная дельта обнаружена
            side = "LONG" if z_score > 0 else "SHORT"
            confidence = calculate_confidence(z_score, obi, speed)

            # 4. Дополнительные фильтры
            if self.check_obi_support(side):
                if self.check_price_movement(side):
                    if self.check_cooldown():
                        # Генерация сигнала
                        return Signal(
                            side=side,
                            confidence=confidence,
                            delta_z=z_score,
                            features={...}
                        )
```

### Redis Architecture

#### Топология

```
┌─────────────────────────────────────────────────────────────┐
│                     Redis Main (6379)                        │
│  Role: Primary storage, external access                     │
│  - maxmemory: 16GB                                          │
│  - maxmemory-policy: allkeys-lru                            │
│  - AOF: yes (appendonly)                                    │
│  - RDB: disabled (save "")                                  │
└────────────────────┬────────────────────────────────────────┘
                     │
        ┌────────────┴────────────┐
        │                         │
┌───────▼────────┐       ┌────────▼────────┐
│ Redis Worker-1 │       │ Redis Worker-2  │
│ (internal)     │       │ (internal)      │
│                │       │                 │
│ - Candles      │       │ - Signals       │
│ - Ticks        │       │ - Backup        │
│ - maxmem: 3GB  │       │ - maxmem: 3GB   │
└────────────────┘       └─────────────────┘
```

#### Ключевые паттерны использования

**1. Streams для событий**

```
candles:data          - Market candles (XADD maxlen ~100000)
stream:tick_XAUUSD    - Tick data (XADD maxlen 50000)
stream:book_XAUUSD    - Order book snapshots
signals:orderflow:*   - Order Flow signals
signals:ta:*          - Technical Analysis signals
notify:telegram       - Notifications queue
```

**2. Hashes для состояния**

```
book:levels:XAUUSD    - Current order book state
pivots:latest         - Daily pivot points
ta:last:atr:XAUUSD    - Latest ATR value
symbol_specs:XAUUSD   - Symbol specifications
```

**3. Consumer Groups**

```
candles:data         -> regime-worker-group
stream:tick_XAUUSD   -> xauusd-signal-group, ohlc-group
signals:orderflow:*  -> hub-group
```

### Signal Generation Architecture

#### Technical Analysis Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                   Signal Generator                           │
│                                                              │
│  ┌──────────────┐                                           │
│  │ Tick Stream  │                                           │
│  │ Consumer     │                                           │
│  └──────┬───────┘                                           │
│         │                                                    │
│  ┌──────▼───────┐      ┌──────────────┐                    │
│  │ Candle       │─────▶│ Indicators:  │                    │
│  │ Builder      │      │ - EMA(9,21)  │                    │
│  │ (M1, M5...)  │      │ - RSI(14)    │                    │
│  └──────────────┘      │ - MACD       │                    │
│                        │ - ATR(14)    │                    │
│                        └──────┬───────┘                    │
│                               │                             │
│  ┌────────────────────────────▼────────────────────────┐   │
│  │         Strategy Logic                              │   │
│  │  IF (EMA_fast > EMA_slow) AND                       │   │
│  │     (RSI < RSI_OVERSOLD) AND                        │   │
│  │     (MACD_histogram > 0)                            │   │
│  │  THEN signal = LONG                                 │   │
│  └────────────────────────────┬────────────────────────┘   │
│                               │                             │
│  ┌────────────────────────────▼────────────────────────┐   │
│  │         Risk Calculator                             │   │
│  │  - SL = price - (ATR * 1.5)                        │   │
│  │  - TP1 = price + (ATR * 2.0)                       │   │
│  │  - TP2 = price + (ATR * 3.0)                       │   │
│  │  - TP3 = price + (ATR * 4.0)                       │   │
│  │  - Lot = (account * risk%) / (SL_points)          │   │
│  └────────────────────────────┬────────────────────────┘   │
│                               │                             │
│  ┌────────────────────────────▼────────────────────────┐   │
│  │         Publish to signals:ta:XAUUSD                │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

#### Aggregated Hub V2

Комбинирует сигналы из разных источников с взвешенным scoring.

```
┌─────────────────────────────────────────────────────────────┐
│                  Aggregated Hub V2                           │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ OrderFlow    │  │ Technical    │  │ Cluster      │      │
│  │ Signals      │  │ Analysis     │  │ Analysis     │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                 │                  │               │
│         └─────────────────┼──────────────────┘               │
│                           │                                  │
│  ┌────────────────────────▼────────────────────────────┐    │
│  │           Weighted Confidence Blending              │    │
│  │                                                      │    │
│  │  confidence = (                                      │    │
│  │    W_DELTA_PRO * delta_confidence +                 │    │
│  │    W_SPEED * speed_confidence +                     │    │
│  │    W_CLUSTER * cluster_confidence +                 │    │
│  │    W_TA * ta_confidence                             │    │
│  │  )                                                   │    │
│  │                                                      │    │
│  │  Weights: 50% / 15% / 25% / 10%                    │    │
│  └────────────────────────────┬─────────────────────────┘   │
│                               │                              │
│  ┌────────────────────────────▼─────────────────────────┐   │
│  │           Filters & Thresholds                       │   │
│  │  - confidence >= 0.25                                │   │
│  │  - min_signal_interval >= 180s                       │   │
│  │  - side_lock (anti-dither) = 20s                     │   │
│  └────────────────────────────┬─────────────────────────┘   │
│                               │                              │
│  ┌────────────────────────────▼─────────────────────────┐   │
│  │           Risk Position Sizer                        │   │
│  │  - Account balance                                   │   │
│  │  - Risk percent (1-2%)                              │   │
│  │  - ATR-based SL/TP                                  │   │
│  │  - Lot calculation                                   │   │
│  └────────────────────────────┬─────────────────────────┘   │
│                               │                              │
│  ┌────────────────────────────▼─────────────────────────┐   │
│  │           Publish to Go Gateway                      │   │
│  │  POST /orders/push                                   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Go Gateway Architecture

Центральный компонент для управления ордерами и уведомлениями.

```
┌─────────────────────────────────────────────────────────────┐
│                      Go Gateway                              │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              HTTP Server (port 8090)                  │   │
│  └────┬────────────────┬───────────────┬──────────────┬─┘   │
│       │                │               │              │      │
│  ┌────▼──────┐  ┌──────▼──────┐  ┌────▼─────┐  ┌────▼───┐  │
│  │ /orders   │  │ /notify     │  │ /health  │  │ /sse   │  │
│  │ /poll     │  │             │  │          │  │ stream │  │
│  │ /enqueue  │  │             │  │          │  │        │  │
│  │ /confirm  │  │             │  │          │  │        │  │
│  │ /push     │  │             │  │          │  │        │  │
│  └────┬──────┘  └──────┬──────┘  └──────────┘  └────────┘  │
│       │                │                                     │
│  ┌────▼────────────────▼─────────────────────────────────┐  │
│  │              Order Queue (in-memory)                  │  │
│  │  - Thread-safe                                        │  │
│  │  - FIFO processing                                    │  │
│  │  - Confirmation tracking                              │  │
│  └─────────────────────┬─────────────────────────────────┘  │
│                        │                                     │
│  ┌─────────────────────▼─────────────────────────────────┐  │
│  │           Telegram Bot Integration                    │  │
│  │  - Rich messages with order details                  │  │
│  │  - Charts rendering                                   │  │
│  │  - Interactive buttons (опционально)                 │  │
│  └─────────────────────┬─────────────────────────────────┘  │
│                        │                                     │
│  ┌─────────────────────▼─────────────────────────────────┐  │
│  │           Paper Trading Executor                      │  │
│  │  - Virtual positions                                  │  │
│  │  - P&L calculation                                    │  │
│  │  - Performance tracking                               │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## 🔄 Потоки данных (Data Flows)

### Flow 1: Market Data Pipeline

```
Binance WebSocket
      │
      ├─→ Go Worker 1m  ─┐
      ├─→ Go Worker 5m  ─┤
      ├─→ Go Worker 15m ─┤
      ├─→ Go Worker 1h  ─┼─→ Redis Stream: candles:data
      ├─→ Go Worker 4h  ─┤
      ├─→ Go Worker 1d  ─┤
      └─→ Go Worker ...  ─┘
                          │
           ┌──────────────┴───────────────┐
           │                              │
      Python Workers              Signal Generator
      (Order Flow)                (TA Analysis)
           │                              │
           ├─→ signals:orderflow:*        │
           └──────────────┬───────────────┘
                          │
                    Aggregated Hub
                          │
                    Go Gateway
                          │
                    ┌─────┴─────┐
              Telegram      Paper Executor
```

### Flow 2: Tick Data Pipeline (MT5)

```
MT5 Terminal (Wine)
      │
      └─→ HTTP POST /tick
                │
         Tick Ingest Server
                │
         Redis Stream: stream:tick_XAUUSD
                │
      ┌─────────┴──────────┐
      │                    │
Order Flow Handler    OHLC Aggregator
      │                    │
signals:orderflow    pivots:latest
```

### Flow 3: Telegram Signals Pipeline

```
Telegram Channels (40+)
      │
Telegram Worker (multi-threaded)
      │
Redis Stream: signal:telegram:raw
      │
Signal Parser Worker
      │
Notify Worker
      │
Telegram Bot (notifications)
```

## 🔧 Масштабирование и производительность

### Horizontal Scaling

**Multi-Symbol OrderFlow**:

```yaml
deploy:
  replicas: 3 # 3 независимых инстанса
```

Каждый инстанс обрабатывает свой набор символов через Redis Consumer Groups.

### Vertical Scaling

**Resource Limits** (docker-compose.yml):

```yaml
deploy:
  resources:
    limits:
      memory: 2G
      cpus: '2.0'
    reservations:
      memory: 512M
      cpus: '0.5'
```

### Redis Optimization

1. **Memory Management**:

   - `maxmemory-policy: allkeys-lru`
   - Stream trimming (batch every 5 min)
   - TTL на временные ключи

2. **Connection Pooling**:

   - Go: 150 connections per worker
   - Python: 100 connections per worker

3. **Persistence Strategy**:
   - AOF для критичных данных
   - RDB отключен для производительности

## 🛡️ Отказоустойчивость

### Health Checks

Все сервисы имеют health checks:

```yaml
healthcheck:
  test: ['CMD', 'curl', '-f', 'http://localhost:8090/healthz']
  interval: 30s
  timeout: 5s
  retries: 3
  start_period: 10s
```

### Auto-Restart Policies

- `unless-stopped`: Для production сервисов
- `on-failure:5`: Для workers (5 попыток)
- `no`: Для одноразовых задач

### Graceful Shutdown

Все сервисы обрабатывают SIGTERM:

```go
// Go example
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
<-sigChan
cleanup()
```

```python
# Python example
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)
```

## 📈 Мониторинг и метрики

### Prometheus Metrics

**Go Workers**:

- `binance_ws_messages_total`: Количество сообщений
- `binance_ws_errors_total`: Ошибки WebSocket
- `redis_publish_duration_seconds`: Latency публикации

**Python Workers**:

- `orderflow_signals_generated_total`: Сгенерированные сигналы
- `orderflow_delta_zscore`: Z-score дельты
- `orderflow_processing_duration_seconds`: Время обработки

**Redis**:

- `redis_connected_clients`: Подключенные клиенты
- `redis_used_memory_bytes`: Использование памяти
- `redis_stream_length`: Длина streams

### Grafana Dashboards

1. **System Overview**: CPU, Memory, Network
2. **Market Data**: Candles rate, Latency
3. **Signals**: Generation rate, Confidence distribution
4. **Redis**: Memory, Commands/sec, Streams length

## 🔐 Безопасность

### Network Isolation

```yaml
networks:
  scanner-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.18.0.0/16
```

Все сервисы изолированы в Docker сети. Только необходимые порты exposed.

### Secrets Management

- `.env` файлы не в git (`.gitignore`)
- Environment variables для sensitive data
- Telegram sessions encrypted

### Redis Security

- Bind to Docker network only (кроме 6379)
- No password (network isolation достаточно)
- Optional: можно добавить `requirepass` в production

---

**Архитектура обеспечивает**:

- ✅ Высокую производительность (low latency)
- ✅ Масштабируемость (horizontal + vertical)
- ✅ Надежность (health checks, auto-restart)
- ✅ Мониторинг (Prometheus + Grafana)
- ✅ Гибкость (event-driven, loose coupling)
