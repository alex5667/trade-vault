# 🔍 ПОЛНЫЙ АУДИТ DATA FLOW ДЛЯ XAUUSD

**Аналитическая команда**: Senior TypeScript/NestJS Developer + Senior Trading Systems Analyst  
**Опыт**: 40 лет совместного опыта  
**Дата**: 2025-11-04  
**Проект**: Scanner Infrastructure

---

## 📊 EXECUTIVE SUMMARY

Проведен полный аудит пути данных для символа **XAUUSD** от получения тиков до отправки аналитики в Telegram бот. Система использует **3 основных сервиса** с event-driven архитектурой через Redis Streams.

### Статус системы

- ✅ **12 сервисов** запущены и работают
- ✅ **Redis** 3 инстанса (main + 2 workers) здоровы
- ✅ **Go Gateway** (порт 8090) healthy
- ⚠️ **Tick Ingest Server** (порт 8087) unhealthy (нет входящих данных)
- ⚠️ **ATR Worker** unhealthy
- ℹ️ **Redis Streams пусты** - нет тиков от MT5 в данный момент

---

## 🎯 3 ОСНОВНЫХ СЕРВИСА ОБРАБОТКИ XAUUSD

### 1️⃣ **Tick Ingest Server** (Python FastAPI)

**Роль**: Точка входа тиков от MT5  
**Порт**: 8087  
**Контейнер**: `scanner-tick-ingest`

#### Функциональность

- Принимает HTTP POST `/tick` от MT5 TickBridge EA (через Wine)
- Принимает HTTP POST `/book` для Order Book данных
- Публикует в Redis Stream: `stream:tick_XAUUSD`
- Поддерживает Dual Redis (worker-1 + worker-2) для отказоустойчивости

#### Формат входных данных

```json
{
	"symbol": "XAUUSD",
	"ts": 1761588727889,
	"bid": 2055.25,
	"ask": 2055.35,
	"last": 2055.3,
	"volume": 1.5,
	"flags": 6
}
```

#### Redis Output

- **Stream**: `stream:tick_XAUUSD`
- **MAXLEN**: 50000 (управляется batch trimmer)
- **Consumers**:
  - `xauusd-signal-group` (OrderFlow Handler)
  - `xauusd-ohlc-group` (OHLC Aggregator)

#### Файлы

- `/python-worker/services/tick_ingest_server.py` - FastAPI server
- `/mt5/TickBridge.mq5` - MQL5 EA для отправки тиков

---

### 2️⃣ **Multi-Symbol OrderFlow Handler** (Python)

**Роль**: Анализ Order Flow для XAUUSD  
**Контейнер**: `scanner_infra_multi-symbol-orderflow_1`  
**Архитектура**: Унифицированная (>85% code reuse)

#### Функциональность

Читает тики из `stream:tick_XAUUSD` и анализирует:

1. **Delta Analysis**

   - Вычисление дельты покупок/продаж
   - Z-score нормализация
   - Threshold: `XAU_DELTA_Z_THRESHOLD=3.0`

2. **OBI Detection (Order Book Imbalance)**

   - Анализ дисбаланса стакана
   - Threshold: `XAU_OBI_THRESHOLD=0.5`
   - Min duration: `XAU_OBI_MIN_DURATION=2.0s`

3. **Iceberg Order Detection**

   - Обнаружение скрытых крупных ордеров
   - Refresh count: `XAU_ICEBERG_REFRESH=2`

4. **Cluster Analysis**

   - Анализ кластеров объема на price levels
   - Hot zones detection

5. **Speed Monitor**
   - Скорость движения цены
   - Weak Progress detection: `XAU_WEAK_PROGRESS_ATR=0.10`

#### Алгоритм генерации сигналов

```python
# Псевдокод
if z_delta > THRESHOLD:
    if obi_supports_direction():
        if speed_confirms():
            if cluster_analysis_positive():
                if cooldown_passed():
                    generate_signal()
```

#### Redis Output

- **Stream**: `signals:orderflow:XAUUSD`
- **Stream**: `notify:telegram` (для прямых уведомлений)
- **Format**: Unified XAUUSD format через `XAUUSDSignalFormatter`

#### Конфигурация

```env
XAU_TICK_STREAM=stream:tick_XAUUSD
XAU_DELTA_WINDOW=120
XAU_DELTA_Z_THRESHOLD=3.0
XAU_OBI_THRESHOLD=0.5
XAU_MIN_SIGNAL_INTERVAL=60
```

#### Файлы

- `/python-worker/handlers/base_orderflow_handler.py` - Base class
- `/python-worker/handlers/xau_orderflow_handler.py` - XAUUSD implementation
- `/python-worker/main_multi_symbol.py` - Entry point

---

### 3️⃣ **Aggregated Hub V2** (Python)

**Роль**: Агрегация сигналов из OrderFlow + Technical Analysis  
**Контейнер**: `scanner-aggregated-hub`

#### Функциональность

Комбинирует сигналы из нескольких источников:

1. **Читает из streams**:

   - `signals:orderflow:XAUUSD` (от Multi-Symbol OrderFlow)
   - `signals:ta:XAUUSD` (от Signal Generator - TA)

2. **Weighted Confidence Blending**:

   ```python
   confidence = (
       W_DELTA_PRO * delta_confidence +    # 50%
       W_SPEED * speed_confidence +        # 15%
       W_CLUSTER * cluster_confidence +    # 25%
       W_LEGACY * ta_confidence            # 10%
   )
   ```

3. **Фильтрация**:

   - Confidence threshold: `HUB_CONFIDENCE_THR=0.25` (25%)
   - Min signal interval: `HUB_MIN_SIG_INT_SEC=180` (3 min)
   - Anti-dither: `HUB_SIDE_LOCK_SEC=20` (блокировка смены направления)

4. **Risk Management**:
   - Использует ATR для SL/TP расчета
   - Position sizing на основе account balance
   - Multi-level TP (3 уровня)

#### Redis Output

- **Stream**: `notify:telegram` (финальные сигналы)
- **HTTP**: POST `http://scanner-go-gateway:8090/orders/push`

#### Конфигурация

```env
ORDERFLOW_STREAM=signals:orderflow:XAUUSD
TA_STREAM=signals:ta:XAUUSD
HUB_CONFIDENCE_THR=0.25
HUB_MIN_SIG_INT_SEC=180
W_DELTA_PRO=0.50
W_SPEED=0.15
W_CLUSTER=0.25
W_LEGACY=0.10
```

#### Файлы

- `/python-worker/aggregated_signal_hub_v2.py` - Main hub
- `/python-worker/core/filtered_signal_writer.py` - Signal writer
- `/python-worker/core/xauusd_signal_formatter.py` - Unified formatter

---

## 📡 ДОПОЛНИТЕЛЬНЫЕ СЕРВИСЫ В PIPELINE

### 4️⃣ **Signal Generator** (Technical Analysis)

**Роль**: Генерация сигналов на основе TA индикаторов  
**Контейнер**: `scanner-signal-generator`  
**Язык**: Python

#### Индикаторы

- **EMA** (9, 21) - Exponential Moving Average
- **RSI** (14) - Relative Strength Index
  - Oversold: 35
  - Overbought: 65
- **MACD** - Moving Average Convergence Divergence
- **ATR** (14) - Average True Range

#### Strategy

```python
if ema_fast > ema_slow and rsi < oversold and macd_histogram > 0:
    signal = LONG
```

#### Redis Output

- **Stream**: `signals:ta:XAUUSD`
- Используется Aggregated Hub для комбинирования с OrderFlow

---

### 5️⃣ **Go Gateway** (HTTP API + Telegram)

**Роль**: Order routing и Telegram интеграция  
**Контейнер**: `scanner-go-gateway`  
**Порт**: 8090  
**Язык**: Go 1.22+

#### API Endpoints

- `POST /orders/push` - Принимает сигналы от Hub
- `POST /orders/enqueue` - Добавляет в очередь
- `GET /orders/poll` - Читает очередь (для MT5)
- `POST /orders/confirm` - Подтверждение исполнения
- `POST /notify` - OBI events от py-obi-service
- `GET /healthz` - Health check

#### Telegram Bot

```go
type Telegram struct {
    bot    *tgbotapi.BotAPI
    chatID int64
}

func (t *Telegram) SendText(msg string)
func (t *Telegram) SendOBIPhoto(symbol, caption string)
```

**ВАЖНО**: Прямая отправка в Telegram из Go Gateway **ОТКЛЮЧЕНА** для избежания дублирования. Все уведомления идут через `notify-worker`.

#### Файлы

- `/go-gateway/main.go` - Main server
- `/go-gateway/internal/` - Internal packages

---

### 6️⃣ **Notify Worker** (Telegram Sender)

**Роль**: Чтение из Redis и отправка в Telegram  
**Контейнер**: `scanner-notify-worker`  
**Язык**: Python (async)

#### Функциональность

1. **Читает**: `notify:telegram` stream через consumer group
2. **Форматирует**: Unified XAUUSD format
3. **Отправляет**: В Telegram bot
4. **ACK**: Подтверждает обработку

#### Consumer Group

- **Group**: `notify-group`
- **Consumer**: `notify-consumer-{PID}`
- **Retry**: Exponential backoff (max 5 retries)

#### Обработка XAUUSD сигналов

```python
# Определяет XAUUSD сигнал по полям
is_xauusd_signal = "text" in entry and "side" in entry and "price" in entry

if is_xauusd_signal:
    # Использует готовый текст из XAUUSDSignalFormatter
    text = entry.get("text")
    await send_to_telegram(text)
```

#### Файлы

- `/telegram-worker/notify_worker.py` - Main worker
- `/telegram-worker/notifier.py` - Telegram sender

---

## 🔄 ПОЛНЫЙ DATA FLOW (END-TO-END)

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. MT5 TERMINAL (Wine on Linux)                                     │
│    └─> TickBridge.mq5 EA                                            │
│        └─> HTTP POST http://scanner-tick-ingest:8087/tick           │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 2. TICK INGEST SERVER (Python FastAPI, port 8087)                  │
│    ├─> Validates XAUUSD tick                                        │
│    ├─> Publishes to Redis Stream: stream:tick_XAUUSD               │
│    └─> Dual Redis (worker-1 + worker-2)                            │
└────────────────────────┬────────────────────────────────────────────┘
                         │
            ┌────────────┴───────────┐
            │                        │
            ▼                        ▼
┌──────────────────────┐   ┌──────────────────────┐
│ 3a. ORDERFLOW        │   │ 3b. OHLC AGGREGATOR  │
│     HANDLER          │   │                      │
│ (Python)             │   │ (Python)             │
│                      │   │                      │
│ XREADGROUP           │   │ XREADGROUP           │
│ stream:tick_XAUUSD   │   │ stream:tick_XAUUSD   │
│                      │   │                      │
│ ├─> Delta Analysis   │   │ ├─> Daily H/L/C      │
│ ├─> OBI Detection    │   │ └─> Pivot levels     │
│ ├─> Iceberg Orders   │   │     └─> pivots:latest│
│ ├─> Cluster Analysis │   └──────────────────────┘
│ └─> Speed Monitor    │
│                      │
│ PUBLISHES:           │
│ signals:orderflow:   │
│      XAUUSD          │
│ notify:telegram      │
└──────┬───────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 4. SIGNAL GENERATOR (Technical Analysis, Python)                   │
│    ├─> Reads: stream:tick_XAUUSD                                   │
│    ├─> Builds candles (M1, M5)                                     │
│    ├─> Calculates: EMA(9,21), RSI(14), MACD, ATR(14)              │
│    └─> Publishes: signals:ta:XAUUSD                               │
└────────────────────────┬────────────────────────────────────────────┘
                         │
       ┌─────────────────┴─────────────────┐
       │                                   │
       ▼                                   ▼
┌──────────────────────┐         ┌──────────────────────┐
│ signals:orderflow:   │         │ signals:ta:XAUUSD    │
│      XAUUSD          │         │                      │
└──────┬───────────────┘         └──────┬───────────────┘
       │                                │
       └────────────────┬───────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 5. AGGREGATED HUB V2 (Python)                                      │
│                                                                      │
│    READS:                                                            │
│    ├─> signals:orderflow:XAUUSD                                    │
│    └─> signals:ta:XAUUSD                                           │
│                                                                      │
│    WEIGHTED BLENDING:                                               │
│    confidence = 50% delta + 15% speed + 25% cluster + 10% ta       │
│                                                                      │
│    FILTERS:                                                          │
│    ├─> confidence >= 25%                                            │
│    ├─> interval >= 180s                                             │
│    └─> side_lock 20s                                                │
│                                                                      │
│    RISK MANAGEMENT:                                                  │
│    ├─> ATR-based SL/TP                                             │
│    ├─> Position sizing                                              │
│    └─> 3 TP levels                                                  │
│                                                                      │
│    PUBLISHES:                                                        │
│    ├─> notify:telegram (unified XAUUSD format)                     │
│    └─> HTTP POST http://go-gateway:8090/orders/push               │
└────────────────────────┬────────────────────────────────────────────┘
                         │
            ┌────────────┴───────────┐
            │                        │
            ▼                        ▼
┌──────────────────────┐   ┌──────────────────────┐
│ 6a. NOTIFY WORKER    │   │ 6b. GO GATEWAY       │
│     (Python)         │   │     (Go)             │
│                      │   │                      │
│ XREADGROUP           │   │ POST /orders/push    │
│ notify:telegram      │   │                      │
│ consumer group       │   │ ├─> Order Queue      │
│                      │   │ ├─> Paper Executor   │
│ ├─> Parses XAUUSD    │   │ └─> Redis stream:    │
│ │   format           │   │     paper:orders     │
│ ├─> Formats message  │   │                      │
│ └─> Sends via        │   │ (Telegram DISABLED)  │
│     Telegram Bot API │   │                      │
└──────┬───────────────┘   └──────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 7. TELEGRAM BOT                                                     │
│    └─> Отправка сообщения в TELEGRAM_CHAT_ID                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔧 КЛЮЧЕВЫЕ REDIS STREAMS

### Stream 1: `stream:tick_XAUUSD`

**Назначение**: Хранение тиков от MT5  
**MAXLEN**: 50000  
**Producer**: `tick-ingest-server`  
**Consumers**:

- `xauusd-signal-group` → Multi-Symbol OrderFlow Handler
- `xauusd-ohlc-group` → OHLC Aggregator

**Формат**:

```json
{
	"ts": "1761588727889",
	"bid": "2055.25",
	"ask": "2055.35",
	"last": "2055.30",
	"volume": "1.5",
	"flags": "6"
}
```

### Stream 2: `signals:orderflow:XAUUSD`

**Назначение**: OrderFlow сигналы  
**MAXLEN**: 1000  
**Producer**: Multi-Symbol OrderFlow Handler  
**Consumer**: Aggregated Hub V2

**Формат**: JSON payload с полным контекстом

### Stream 3: `signals:ta:XAUUSD`

**Назначение**: Technical Analysis сигналы  
**MAXLEN**: 1000  
**Producer**: Signal Generator  
**Consumer**: Aggregated Hub V2

### Stream 4: `notify:telegram`

**Назначение**: Финальные уведомления  
**MAXLEN**: 500-1000  
**Producers**:

- Multi-Symbol OrderFlow Handler (прямые сигналы)
- Aggregated Hub V2 (агрегированные сигналы)
  **Consumer**: Notify Worker

**Формат**: Unified XAUUSD format через `XAUUSDSignalFormatter`

```json
{
	"text": "💥 🟢 XAUUSD LONG @ 2055.50, Volume 0.10 lot\n...",
	"sid": "1730000000000:LONG:205550",
	"symbol": "XAUUSD",
	"side": "LONG",
	"entry": "2055.50",
	"sl": "2050.00",
	"tp_levels": "[2060.0, 2065.0, 2070.0]",
	"lot": "0.10",
	"confidence": "85.0",
	"source": "OrderFlow",
	"ts": "1730000000000"
}
```

---

## 📊 UNIFIED XAUUSD SIGNAL FORMAT

Все сервисы используют **единый формат** через `XAUUSDSignalFormatter`:

### Dataclass

```python
@dataclass
class XAUUSDSignal:
    sid: str                    # Signal ID
    symbol: str                 # XAUUSD
    side: str                   # LONG/SHORT
    entry: float                # Entry price
    sl: float                   # Stop Loss
    tp_levels: List[float]      # Take Profits (3 levels)
    lot: float                  # Volume
    source: str                 # OrderFlow/TechnicalAnalysis/AggregatedHub
    reason: str                 # Signal reason
    confidence: float           # 0-100%
    atr: float                  # ATR value
    ts: int                     # Timestamp (ms)
    indicators: Dict            # Additional indicators
```

### Telegram Message Format

```
💥 🟢 XAUUSD LONG @ 2055.50, Volume 0.10 lot
📝 Extreme delta activity; z=-6.5; OBI=0.65
🛑 SL 2050.00 | TP1 2060.00 (RR 7.5); TP2 2065.00 (RR 14.2); TP3 2070.00 (RR 21.7)
🕐 15:30:45 04.11.2025 UTC
🔧 Source: OrderFlow | ID: 1730000000000:LONG:205550
📊 Z=-6.5 | ATR=0.60 | Conf=85%
```

### Methods

- `format_telegram_message()` - Для Telegram
- `format_redis_payload()` - Для Redis stream
- `format_audit_payload()` - Для audit/analytics
- `format_order_payload()` - Для API `/orders/push`
- `create_signal_id()` - Генерация уникального ID

---

## ⚙️ КОНФИГУРАЦИЯ И ПАРАМЕТРЫ

### Environment Variables (docker-compose.yml)

#### Multi-Symbol OrderFlow

```yaml
SYMBOLS=XAUUSD,BTCUSD,ETHUSD
XAU_TICK_STREAM=stream:tick_XAUUSD
XAU_BOOK_STREAM=stream:book_XAUUSD
XAU_DELTA_WINDOW=120
XAU_DELTA_Z_THRESHOLD=3.0
XAU_WEAK_PROGRESS_ATR=0.10
XAU_OBI_THRESHOLD=0.5
XAU_OBI_MIN_DURATION=2.0
XAU_MIN_SIGNAL_INTERVAL=60
```

#### Aggregated Hub V2

```yaml
HUB_CONFIDENCE_THR=0.25
HUB_MIN_SIG_INT_SEC=180
HUB_SIDE_LOCK_SEC=20
W_DELTA_PRO=0.50
W_SPEED=0.15
W_CLUSTER=0.25
W_LEGACY=0.10
```

#### Risk Management

```yaml
STOP_MODE=ATR
STOP_ATR_MULT=0.6
TP_MODE=RR
TP_RR=1,2,3
RISK_PCT=1.0
ACCOUNT_BALANCE=10000.0
```

---

## 🔍 ДИАГНОСТИКА И МОНИТОРИНГ

### Проверка системы

#### 1. Проверка Redis Streams

```bash
# Tick stream
redis-cli XLEN stream:tick_XAUUSD
redis-cli XINFO GROUPS stream:tick_XAUUSD

# OrderFlow signals
redis-cli XLEN signals:orderflow:XAUUSD

# Notifications
redis-cli XLEN notify:telegram
redis-cli XINFO GROUPS notify:telegram
```

#### 2. Проверка сервисов

```bash
# Статус контейнеров
docker ps --filter "name=scanner" --format "table {{.Names}}\t{{.Status}}"

# Логи ключевых сервисов
docker logs scanner-tick-ingest --tail 50
docker logs scanner_infra_multi-symbol-orderflow_1 --tail 50
docker logs scanner-aggregated-hub --tail 50
docker logs scanner-notify-worker --tail 50
docker logs scanner-go-gateway --tail 50
```

#### 3. Проверка Telegram

```bash
# Test notification
curl -X POST http://localhost:8090/notify \
  -H "Content-Type: application/json" \
  -d '{
    "ts": 1730000000000,
    "symbol": "XAUUSD",
    "type": "test",
    "duration_ms": 1000,
    "obi": 0.5,
    "threshold": 0.25
  }'
```

#### 4. Makefile команды

```bash
make check-xauusd-services  # Проверка 3 сервисов
make check-redis-streams    # Проверка всех streams
make check-telegram         # Проверка Telegram
make full-system-check      # Полная диагностика
```

---

## 🐛 ТЕКУЩИЕ ПРОБЛЕМЫ И РЕКОМЕНДАЦИИ

### ⚠️ Обнаруженные проблемы

1. **Tick Ingest Server: UNHEALTHY**

   - **Причина**: Нет входящих тиков от MT5
   - **Проверка**:
     ```bash
     curl http://localhost:8087/health
     ```
   - **Решение**:
     - Убедиться что MT5 запущен под Wine
     - Проверить TickBridge EA в MT5
     - Проверить порт 8087 доступен из MT5

2. **ATR Worker: UNHEALTHY**

   - **Причина**: Не может вычислить ATR без candles
   - **Зависит от**: Go Workers (candles:data stream)
   - **Решение**: Проверить `candles:data` stream

3. **Redis Streams пусты**
   - **Причина**: Нет данных от MT5
   - **Эффект**: Нет сигналов, нет уведомлений
   - **Решение**: Наладить поток тиков от MT5

### ✅ Рекомендации

#### Немедленные действия (HIGH PRIORITY)

1. **Проверить MT5 → Tick Ingest подключение**

   ```bash
   # Тест эндпоинта
   curl -X POST http://localhost:8087/tick \
     -H "Content-Type: application/json" \
     -d '{
       "symbol": "XAUUSD",
       "ts": 1730000000000,
       "bid": 2055.25,
       "ask": 2055.35,
       "last": 2055.30,
       "volume": 1.5,
       "flags": 6
     }'

   # Проверить в Redis
   redis-cli XLEN stream:tick_XAUUSD
   ```

2. **Настроить MT5 TickBridge EA**

   - Файл: `/mt5/TickBridge.mq5`
   - URL: `http://scanner-tick-ingest:8087/tick`
   - Символ: `XAUUSD`
   - Интервал: 200ms (5 Hz)

3. **Мониторинг в реальном времени**

   ```bash
   # Watch Redis streams
   watch -n 1 'redis-cli XLEN stream:tick_XAUUSD'

   # Watch logs
   docker logs -f scanner_infra_multi-symbol-orderflow_1
   ```

#### Средний приоритет (MEDIUM)

1. **Добавить алерты на пустые streams**

   - Prometheus alert если XLEN = 0 более 5 минут
   - Telegram уведомление о проблемах

2. **Улучшить health checks**

   - Tick Ingest: проверять last tick timestamp
   - ATR Worker: проверять доступность candles

3. **Добавить backfill механизм**
   - Если stream был пуст, заполнить историей
   - Использовать исторические данные для ATR

#### Низкий приоритет (LOW)

1. **Оптимизация**

   - Stream trimming - настроить более агрессивно
   - Consumer groups - добавить replicas для высокой нагрузки

2. **Документация**
   - Добавить диаграммы в Grafana
   - Runbook для типичных проблем

---

## 📈 МЕТРИКИ И KPI

### Latency Targets

- **MT5 → Redis**: < 10ms
- **Redis → OrderFlow**: < 50ms
- **OrderFlow → Hub**: < 100ms
- **Hub → Telegram**: < 500ms
- **Total E2E**: < 1s

### Throughput

- **Ticks/sec**: 5-10 (MT5)
- **Signals/hour**: 5-20 (зависит от рынка)
- **Messages/day**: ~100-300 (Telegram)

### Availability

- **Redis**: 99.9% (3 инстанса, failover)
- **Services**: 99.5% (Docker restart policies)
- **Telegram**: 99% (retry mechanism, exponential backoff)

---

## 🔐 SECURITY & BEST PRACTICES

### 1. Redis Security

- ✅ Bind to Docker network only
- ✅ Network isolation (scanner-network)
- ⚠️ No password (достаточно network isolation для dev)
- 🔒 **TODO**: Add `requirepass` для production

### 2. Telegram Bot

- ✅ Token в environment variables
- ✅ Single chat ID restriction
- ✅ No bot commands (write-only)

### 3. Docker

- ✅ Health checks на всех сервисах
- ✅ Resource limits (CPU, Memory)
- ✅ Restart policies (`unless-stopped`, `on-failure`)
- ✅ Network isolation

### 4. Code Quality

- ✅ Unified formatter (`XAUUSDSignalFormatter`)
- ✅ Type hints (Python dataclasses)
- ✅ Error handling (try-except, exponential backoff)
- ✅ Logging (structured, with timestamps)

---

## 📚 ФАЙЛОВАЯ СТРУКТУРА

### Tick Ingest

```
/python-worker/services/
  ├─ tick_ingest_server.py       # FastAPI server
  └─ ohlc_aggregator.py           # Daily OHLC + Pivots
```

### OrderFlow Handler

```
/python-worker/
  ├─ main_multi_symbol.py         # Entry point
  ├─ main_multi_symbol_dynamic.py # Dynamic symbol mgmt
  └─ handlers/
      ├─ base_orderflow_handler.py  # Base class (>85% reuse)
      └─ xau_orderflow_handler.py   # XAUUSD implementation
```

### Signal Hub

```
/python-worker/
  ├─ aggregated_signal_hub_v2.py  # V2 Hub (weighted blending)
  ├─ aggregated_signal_hub.py     # V1 Hub (legacy)
  └─ core/
      ├─ filtered_signal_writer.py    # Signal writer + API push
      ├─ xauusd_signal_formatter.py   # Unified formatter
      └─ dual_redis_client.py         # Dual Redis client
```

### Go Gateway

```
/go-gateway/
  ├─ main.go                      # Main server
  └─ internal/
      ├─ paper/                   # Paper executor
      ├─ risk/                    # Risk management
      └─ runtime/                 # SSE streaming
```

### Telegram

```
/telegram-worker/
  ├─ notify_worker.py             # Main worker (async)
  ├─ notifier.py                  # Telegram sender
  └─ app/
      └─ config.py                # Configuration
```

---

## 🎯 ЗАКЛЮЧЕНИЕ

### Сильные стороны архитектуры

✅ **Event-Driven**: Loose coupling, легко масштабируется  
✅ **Unified Format**: `XAUUSDSignalFormatter` для всех сервисов  
✅ **Fault Tolerant**: Dual Redis, Consumer Groups, Retry logic  
✅ **Modular**: >85% code reuse (BaseOrderFlowHandler)  
✅ **Observable**: Prometheus metrics, Grafana dashboards  
✅ **Well-Documented**: Code comments, type hints, docstrings

### Области для улучшения

⚠️ **Зависимость от MT5**: Single point of failure  
⚠️ **Нет backfill**: При падении теряем данные  
⚠️ **Мониторинг**: Нет алертов на пустые streams  
⚠️ **Testing**: Отсутствуют E2E тесты

### Следующие шаги

1. **Починить MT5 → Tick Ingest** (HIGH)
2. **Добавить мониторинг streams** (HIGH)
3. **Настроить алерты** (MEDIUM)
4. **Написать E2E тесты** (MEDIUM)
5. **Добавить backfill механизм** (LOW)

---

**Отчет подготовлен**: 2025-11-04  
**Команда**: Senior Developer + Trading Analyst (40 лет опыта)  
**Статус**: ✅ Архитектура здорова, требуется наладка MT5 подключения
