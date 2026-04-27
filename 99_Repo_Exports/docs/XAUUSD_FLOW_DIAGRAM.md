# 📊 XAUUSD DATA FLOW - ВИЗУАЛЬНАЯ ДИАГРАММА

## 🎯 Полный путь данных от MT5 до Telegram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                          🖥️  MT5 TERMINAL (Wine)                           │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  TickBridge.mq5 EA                                            │        │
│  │  • Poll XAUUSD ticks every 200ms (5 Hz)                       │        │
│  │  • Format: {symbol, ts, bid, ask, last, volume, flags}        │        │
│  │  • HTTP POST http://scanner-tick-ingest:8087/tick             │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                  │                                          │
└──────────────────────────────────┼──────────────────────────────────────────┘
                                   │
                                   │ HTTP POST
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│             🐍 TICK INGEST SERVER (Python FastAPI :8087)                   │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  POST /tick                                                    │        │
│  │  • Validates XAUUSD tick data                                  │        │
│  │  • Normalizes timestamps                                       │        │
│  │  • XADD stream:tick_XAUUSD (Dual Redis)                       │        │
│  │    - redis-worker-1                                            │        │
│  │    - redis-worker-2                                            │        │
│  │  • MAXLEN: 50000 (batch trimmer)                              │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                  │                                          │
└──────────────────────────────────┼──────────────────────────────────────────┘
                                   │
                                   │ Redis XADD
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                    🔴 REDIS STREAM: stream:tick_XAUUSD                     │
│                                                                             │
│  Consumer Groups:                                                           │
│  • xauusd-signal-group → Multi-Symbol OrderFlow Handler                    │
│  • xauusd-ohlc-group → OHLC Aggregator                                     │
│                                                                             │
└─────────────────┬───────────────────────────────────────┬───────────────────┘
                  │                                       │
          XREADGROUP                              XREADGROUP
                  │                                       │
                  ▼                                       ▼
┌────────────────────────────────────┐    ┌──────────────────────────────────┐
│                                    │    │                                  │
│  🐍 MULTI-SYMBOL ORDERFLOW        │    │  🐍 OHLC AGGREGATOR             │
│     HANDLER                        │    │                                  │
│                                    │    │  • Builds Daily H/L/C            │
│  ┌──────────────────────────────┐ │    │  • Calculates Pivots             │
│  │ Delta Analyzer                │ │    │  • Publishes pivots:latest       │
│  │ • Z-score: threshold 3.0      │ │    │                                  │
│  │ • Window: 120 ticks           │ │    └──────────────────────────────────┘
│  └──────────────────────────────┘ │
│                                    │
│  ┌──────────────────────────────┐ │
│  │ OBI Detector                  │ │
│  │ • Threshold: 0.5              │ │
│  │ • Min duration: 2.0s          │ │
│  └──────────────────────────────┘ │
│                                    │
│  ┌──────────────────────────────┐ │
│  │ Iceberg Detector              │ │
│  │ • Refresh count: 2            │ │
│  └──────────────────────────────┘ │
│                                    │
│  ┌──────────────────────────────┐ │
│  │ Cluster Analyzer              │ │
│  │ • Hot zones detection         │ │
│  └──────────────────────────────┘ │
│                                    │
│  ┌──────────────────────────────┐ │
│  │ Speed Monitor                 │ │
│  │ • Weak progress: 0.10 ATR     │ │
│  └──────────────────────────────┘ │
│                                    │
│  IF: z_delta > 3.0 &&             │
│      obi > 0.5 &&                 │
│      !weak_progress &&            │
│      cooldown_ok (60s)            │
│  THEN: PUBLISH SIGNAL             │
│                                    │
└──────────────┬─────────────────────┘
               │
               │ XADD
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│              🔴 REDIS STREAM: signals:orderflow:XAUUSD                     │
│                                                                             │
│  + Direct notifications to: notify:telegram                                 │
│                                                                             │
└─────────────────┬───────────────────────────────────────────────────────────┘
                  │
          XREADGROUP
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                    🐍 AGGREGATED HUB V2                                    │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Reads 2 streams:                                              │        │
│  │  • signals:orderflow:XAUUSD                                    │        │
│  │  • signals:ta:XAUUSD (from Signal Generator)                   │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Weighted Blending:                                            │        │
│  │  confidence = 50% delta + 15% speed +                          │        │
│  │               25% cluster + 10% ta                             │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Filters:                                                      │        │
│  │  • confidence >= 25%                                           │        │
│  │  • interval >= 180s                                            │        │
│  │  • side_lock: 20s                                              │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Risk Management:                                              │        │
│  │  • ATR-based SL/TP                                             │        │
│  │  • Position sizing (1% risk)                                   │        │
│  │  • 3 TP levels (RR: 1, 2, 3)                                   │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  Publishes:                                                                 │
│  • notify:telegram (unified XAUUSD format)                                  │
│  • POST http://go-gateway:8090/orders/push                                 │
│                                                                             │
└─────────────────┬───────────────────────────────────────────────────────────┘
                  │
          ┌───────┴────────┐
          │                │
          ▼                ▼
┌──────────────────┐  ┌──────────────────────────────────────────┐
│                  │  │                                          │
│  🔴 REDIS        │  │  🐹 GO GATEWAY (:8090)                  │
│  notify:telegram │  │                                          │
│                  │  │  POST /orders/push                       │
│  XADD with       │  │  • Order Queue (in-memory)               │
│  unified format  │  │  • Paper Executor                        │
│                  │  │  • Redis: paper:orders stream            │
│                  │  │                                          │
│  Consumer:       │  │  ⚠️ Telegram notifications DISABLED      │
│  notify-group    │  │  (handled by notify-worker)              │
│                  │  │                                          │
└────────┬─────────┘  └──────────────────────────────────────────┘
         │
         │ XREADGROUP
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                    🐍 NOTIFY WORKER (Python async)                         │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  XREADGROUP notify:telegram                                    │        │
│  │  • Consumer group: notify-group                                │        │
│  │  • Consumer: notify-consumer-{PID}                             │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Parses XAUUSD unified format:                                 │        │
│  │  • Checks: "text", "side", "price" fields                      │        │
│  │  • Uses pre-formatted message                                  │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Error Handling:                                               │        │
│  │  • Max retries: 5                                              │        │
│  │  • Exponential backoff                                         │        │
│  │  • XACK after success                                          │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  Sends to Telegram Bot API ↓                                               │
│                                                                             │
└─────────────────┬───────────────────────────────────────────────────────────┘
                  │
                  │ Telegram Bot API
                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                          💬 TELEGRAM BOT                                   │
│                                                                             │
│  Sends message to TELEGRAM_CHAT_ID:                                         │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  💥 🟢 XAUUSD LONG @ 2055.50, Volume 0.10 lot                 │        │
│  │  📝 Extreme delta activity; z=-6.5; OBI=0.65                  │        │
│  │  🛑 SL 2050.00 | TP1 2060.00 (RR 7.5); TP2 2065.00 (RR 14.2) │        │
│  │  🕐 15:30:45 04.11.2025 UTC                                   │        │
│  │  🔧 Source: OrderFlow | ID: 1730000000000:LONG:205550         │        │
│  │  📊 Z=-6.5 | ATR=0.60 | Conf=85%                              │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 ПАРАЛЛЕЛЬНЫЕ ПОТОКИ

### Поток A: Technical Analysis (параллельно с OrderFlow)

```
stream:tick_XAUUSD
       │
       │ XREADGROUP
       ▼
┌──────────────────────┐
│  SIGNAL GENERATOR    │
│  (TA - Python)       │
│                      │
│  • EMA (9, 21)       │
│  • RSI (14)          │
│  • MACD              │
│  • ATR (14)          │
│                      │
│  Strategy:           │
│  IF ema_crossover && │
│     rsi_condition && │
│     macd_confirm     │
│  THEN signal         │
└──────┬───────────────┘
       │
       │ XADD
       ▼
signals:ta:XAUUSD
       │
       └─────────→ Aggregated Hub V2
```

### Поток B: ATR Calculation

```
candles:data (от Go Workers)
       │
       │ XREADGROUP
       ▼
┌──────────────────────┐
│  ATR WORKER          │
│                      │
│  • Reads candles     │
│  • Calculates ATR(14)│
│  • Per symbol/TF     │
│                      │
└──────┬───────────────┘
       │
       │ SET
       ▼
ta:last:atr:XAUUSD:1m
       │
       └─────────→ Used by OrderFlow + Hub
```

---

## 🔍 КРИТИЧЕСКИЕ ТОЧКИ ПРОВЕРКИ

### 1. MT5 → Tick Ingest

```bash
# Проверка
curl -X POST http://localhost:8087/tick \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"XAUUSD","ts":1730000000000,"bid":2055.25,"ask":2055.35,"last":2055.30,"volume":1.5,"flags":6}'

# Должно вернуть
{"status":"ok","stream_id":"..."}

# Проверка в Redis
redis-cli XLEN stream:tick_XAUUSD
# Должно быть > 0
```

### 2. Tick Stream → OrderFlow

```bash
# Проверка consumer group
redis-cli XINFO GROUPS stream:tick_XAUUSD

# Проверка pending
redis-cli XPENDING stream:tick_XAUUSD xauusd-signal-group

# Логи
docker logs scanner_infra_multi-symbol-orderflow_1 --tail 50
```

### 3. OrderFlow → Signals

```bash
# Проверка сигналов
redis-cli XLEN signals:orderflow:XAUUSD

# Последний сигнал
redis-cli XREVRANGE signals:orderflow:XAUUSD + - COUNT 1
```

### 4. Hub → Notifications

```bash
# Проверка notify stream
redis-cli XLEN notify:telegram

# Последнее уведомление
redis-cli XREVRANGE notify:telegram + - COUNT 1

# Логи notify-worker
docker logs scanner-notify-worker --tail 50
```

### 5. Telegram Delivery

```bash
# Проверка лимитов Telegram API
# Rate limit: 30 messages/second
# Per chat: 1 message/second (recommended)

# Проверка consumer group
redis-cli XINFO GROUPS notify:telegram

# Логи
docker logs scanner-notify-worker -f
```

---

## ⚡ LATENCY BREAKDOWN

```
MT5 Terminal
    │ (0-5ms)
    ├─→ HTTP POST
    │
Tick Ingest Server
    │ (2-8ms)
    ├─→ XADD to Redis
    │
Redis Stream
    │ (1-3ms)
    ├─→ XREADGROUP
    │
OrderFlow Handler
    │ (10-40ms - analysis)
    ├─→ XADD signals
    │
Aggregated Hub
    │ (5-20ms - blending)
    ├─→ XADD notify
    │
Notify Worker
    │ (50-200ms - Telegram API)
    └─→ Telegram Bot API
        │
        └─→ User sees message

TOTAL: ~70-280ms (p50: ~150ms)
```

---

## 🎯 ЕДИНЫЙ ФОРМАТ СООБЩЕНИЯ

Все сервисы используют `XAUUSDSignalFormatter`:

```python
@dataclass
class XAUUSDSignal:
    sid: str                # "1730000000000:LONG:205550"
    symbol: str             # "XAUUSD"
    side: str               # "LONG" | "SHORT"
    entry: float            # 2055.50
    sl: float               # 2050.00
    tp_levels: List[float]  # [2060.0, 2065.0, 2070.0]
    lot: float              # 0.10
    source: str             # "OrderFlow" | "AggregatedHub"
    reason: str             # "Extreme delta; z=-6.5"
    confidence: float       # 85.0 (0-100%)
    atr: float              # 0.60
    ts: int                 # 1730000000000 (ms)
    indicators: Dict        # {"z": -6.5, "obi": 0.65}
```

### Методы форматирования

- `format_telegram_message()` → Красивое сообщение для Telegram
- `format_redis_payload()` → Для Redis stream (notify:telegram)
- `format_audit_payload()` → Для analytics/ML
- `format_order_payload()` → Для API /orders/push

---

## 🔧 МОНИТОРИНГ

### Prometheus Metrics

```
# OrderFlow Handler
orderflow_signals_generated_total{symbol="XAUUSD"}
orderflow_delta_zscore{symbol="XAUUSD"}
orderflow_processing_duration_seconds

# Aggregated Hub
hub_signals_published_total{symbol="XAUUSD"}
hub_confidence_distribution
hub_filter_rejections_total{reason="confidence|cooldown|side_lock"}

# Notify Worker
notify_messages_sent_total
notify_errors_total{type="rate_limit|timeout|network"}
notify_retry_attempts_total

# Redis
redis_stream_length{stream="stream:tick_XAUUSD"}
redis_stream_length{stream="notify:telegram"}
redis_consumer_lag{group="xauusd-signal-group"}
```

### Grafana Dashboards

1. **XAUUSD Overview**

   - Ticks/sec
   - Signals generated
   - Notifications sent
   - E2E latency

2. **Signal Quality**

   - Confidence distribution
   - Win rate (if backtested)
   - Source breakdown (OrderFlow vs TA)

3. **System Health**
   - Stream lengths
   - Consumer lag
   - Error rates
   - Service health

---

## 🚨 АЛЕРТЫ

### Critical Alerts

1. **No Ticks for 5 minutes**

   ```
   redis_stream_length{stream="stream:tick_XAUUSD"} == 0
   for: 5m
   ```

2. **Consumer Lag > 1000**

   ```
   redis_consumer_lag{group="xauusd-signal-group"} > 1000
   for: 2m
   ```

3. **Notification Errors > 5%**

   ```
   rate(notify_errors_total[5m]) / rate(notify_messages_sent_total[5m]) > 0.05
   for: 5m
   ```

4. **Service Unhealthy**
   ```
   up{job="scanner-tick-ingest"} == 0
   for: 1m
   ```

---

## 📈 ОПТИМИЗАЦИЯ

### Текущие настройки

| Parameter           | Value        | Можно улучшить                  |
| ------------------- | ------------ | ------------------------------- |
| Tick poll rate      | 200ms (5 Hz) | ✅ Оптимально для XAUUSD        |
| Delta window        | 120 ticks    | ⚠️ Можно динамически менять     |
| Z-score threshold   | 3.0          | ⚠️ Можно калибровать на истории |
| Min signal interval | 60s          | ✅ Достаточно                   |
| Hub confidence      | 25%          | ⚠️ Можно повысить для качества  |
| Telegram rate       | ~1/sec       | ✅ В пределах лимитов           |

### Рекомендации

1. **Adaptive Thresholds**: Калибровка на основе волатильности
2. **ML Enhancement**: Добавить ML модель для фильтрации
3. **Multi-timeframe**: Учитывать сигналы с разных TF
4. **Context Enrichment**: Добавить макро-данные (news, sentiment)

---

**Диаграмма подготовлена**: 2025-11-04  
**Команда**: Senior Developer + Trading Analyst  
**Версия**: 2.0
