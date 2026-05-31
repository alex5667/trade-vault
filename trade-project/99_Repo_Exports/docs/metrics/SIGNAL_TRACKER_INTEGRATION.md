# ✅ Signal Performance Tracker - Интеграция завершена

**Дата**: 2025-11-04  
**Статус**: ✅ **ЗАПУЩЕН И ИНТЕГРИРОВАН**

---

## 🎯 Что такое Signal Performance Tracker?

**Название сервиса**: **Signal Performance Tracker**

Это система, которая **после формирования сигнала обрабатывает тики и отслеживает сработку** TP/SL уровней.

### Ключевые особенности:
- 📊 Отслеживает виртуальные позиции по сигналам
- ✅ Проверяет достижение TP1, TP2, TP3
- 🛑 Проверяет срабатывание SL
- 📈 Частичное закрытие: TP1(50%), TP2(30%), TP3(20%)
- 💾 Логирует все события в Redis
- 📊 Обновляет статистику (WinRate, P&L, Profit Factor)
- 📱 Отправляет отчеты каждые 3 часа в Telegram

---

## 🏗️ Архитектура

```
Signal Generation
    │
    ├─> signals:orderflow:XAUUSD ──┐
    └─> signals:ta:XAUUSD ─────────┤
                                    │
                                    ▼
              ┌────────────────────────────────────────┐
              │  SIGNAL PERFORMANCE TRACKER            │
              │                                        │
              │  1. Trade Monitor                      │
              │     • Reads signals                    │
              │     • Creates virtual positions        │
              │     • Reads ticks: stream:tick_XAUUSD  │
              │     • Checks TP/SL conditions          │
              │     • Partial close: 50%/30%/20%       │
              │                                        │
              │  2. Stats Aggregator                   │
              │     • WinRate, P/L                     │
              │     • TP hit rates                     │
              │     • Profit Factor                    │
              │     • By source (OrderFlow/TA/Hub)     │
              │                                        │
              │  3. Reporting Service                  │
              │     • Periodic reports (3h)            │
              │     • Daily summaries                  │
              │     • Telegram notifications           │
              │                                        │
              └────────────────────────────────────────┘
                    │                    │
                    ▼                    ▼
            events:trades        notify:telegram
            trades:closed        (Reports)
```

---

## 📦 Компоненты

### 1. **Trade Monitor** (`services/trade_monitor.py`)

**Размер**: 400+ строк  
**Роль**: Отслеживание виртуальных позиций

**Функции**:
- `process_signal(signal)` - Открывает позицию по сигналу
- `process_tick(tick)` - Обновляет позиции по тику
- `_check_take_profits()` - Проверяет TP1/TP2/TP3
- `_handle_take_profit()` - Частичное закрытие на TP
- `_check_stop_loss()` - Проверяет SL
- `_handle_stop_loss()` - Закрытие по SL
- `_finalize_position()` - Финализация и статистика

**Логика работы**:
```python
for each tick:
    for each open_position:
        # Проверка TP (в порядке очереди)
        if LONG and price >= TP1:
            close 50% at TP1
            tp1_hit = True
        if LONG and price >= TP2:
            close 30% at TP2
            tp2_hit = True
        if LONG and price >= TP3:
            close 20% at TP3
            tp3_hit = True
        
        # Проверка SL
        if LONG and price <= SL:
            close remaining % at SL
            finalize(reason="SL")
```

### 2. **Stats Aggregator** (`services/stats_aggregator.py`)

**Размер**: 21KB (уже существовал)  
**Роль**: Подсчет метрик

**Метрики**:
- total_trades, wins, losses, winrate
- total_pnl, avg_pnl, max_win, max_loss
- tp1_hits, tp2_hits, tp3_hits
- tp1_then_sl, tp2_then_sl, tp3_then_sl (упущенная прибыль)
- profit_factor, sharpe_ratio
- **По источникам**: OrderFlow, TechnicalAnalysis, AggregatedHub-V2

### 3. **Reporting Service** (`services/reporting_service.py`)

**Размер**: 30KB (уже существовал)  
**Роль**: Генерация отчетов

**Функции**:
- `get_strategy_report()` - Получить статистику
- `send_periodic_summary()` - Периодический отчет (каждые 3ч)
- `send_daily_summary()` - Ежедневная сводка
- `send_telegram_notification()` - Отправка в Telegram

### 4. **Signal Performance Tracker** (`services/signal_performance_tracker.py`)

**Размер**: 350+ строк  
**Роль**: Главный оркестратор

**Потоки**:
- **Thread 1**: Signals Listener - читает сигналы
- **Thread 2**: Ticks Listener - читает тики
- **Thread 3**: Periodic Tasks - отчеты каждые 3ч

---

## 🔄 Data Flow

### Поток сигналов

```
signals:orderflow:XAUUSD
signals:ta:XAUUSD
    │
    │ XREADGROUP (consumer: signal-tracker-group)
    ▼
Trade Monitor
    │
    ├─> process_signal()
    │   └─> Создает Position
    │       └─> Сохраняет в order:{id}
    │           └─> Логирует OPEN в events:trades
    │
    └─> open_positions[id] = Position
```

### Поток тиков

```
stream:tick_XAUUSD
    │
    │ XREADGROUP (consumer: signal-tracker-group-ticks)
    ▼
Trade Monitor
    │
    └─> process_tick()
        │
        ├─> Проверяет все open_positions
        │
        ├─> if TP1 reached:
        │   └─> Закрывает 50%
        │       └─> Логирует TP в events:trades
        │           └─> Обновляет stats через StatsAggregator
        │
        ├─> if TP2 reached:
        │   └─> Закрывает 30%
        │
        ├─> if TP3 reached:
        │   └─> Закрывает 20%
        │
        └─> if SL reached:
            └─> Закрывает remaining %
                └─> Логирует SL в events:trades
                    └─> Финализирует позицию
                        └─> trades:closed stream
                            └─> StatsAggregator.update_stats()
```

---

## 📊 Redis Структура данных

### Streams

```
signals:orderflow:XAUUSD      - Входящие OrderFlow сигналы
signals:ta:XAUUSD              - Входящие TA сигналы
stream:tick_XAUUSD             - Тиковые данные
events:trades                  - События: OPEN, TP, SL, CLOSE
trades:closed                  - Закрытые сделки (summary)
```

### Hashes

```
signal:{id}                    - Исходный сигнал
order:{id}                     - Данные позиции/ордера
stats:orderflow:XAUUSD:tick    - Общая статистика
stats:orderflow:XAUUSD:tick:OrderFlow      - По источнику
stats:orderflow:XAUUSD:tick:AggregatedHub  - По источнику
```

### Lists

```
closed:orderflow:XAUUSD:tick              - ID сделок (пагинация)
closed:orderflow:XAUUSD:tick:OrderFlow    - ID по источнику
```

### Sets

```
stats:sources:orderflow:XAUUSD:tick  - Множество источников
```

---

## 🚀 Запуск и проверка

### Docker запуск (автоматический)

```bash
# Запускается автоматически с make up
make up-bg

# Проверить статус
docker ps | grep signal-tracker

# Логи
docker logs -f scanner-signal-tracker
```

### Ручной запуск (для разработки)

```bash
cd python-worker
export REDIS_URL=redis://scanner-redis-worker-1:6379/0
export SYMBOLS=XAUUSD,BTCUSD
export TELEGRAM_BOT_TOKEN=your_token
export TELEGRAM_CHAT_ID=your_chat_id

python -m services.signal_performance_tracker
```

### Проверка работы

```bash
# 1. Проверить контейнер
docker ps | grep signal-tracker

# 2. Проверить логи инициализации
docker logs scanner-signal-tracker | grep "✅"

# 3. Проверить consumer groups
docker exec scanner-redis redis-cli XINFO GROUPS signals:orderflow:XAUUSD

# 4. Проверить events stream
docker exec scanner-redis redis-cli XLEN events:trades

# 5. Проверить закрытые сделки
docker exec scanner-redis redis-cli XLEN trades:closed
```

---

## 📈 Конфигурация

### Файл: `python-worker/config/signal_tracker_config.json`

```json
{
  "streams": {
    "symbols": ["XAUUSD", "BTCUSD", "ETHUSD"],
    "strategies": ["orderflow", "ta"]
  },
  "monitor": {
    "default_lot": 1.0,
    "rr_levels": [1.0, 2.0, 3.0],
    "tp_ratio": [0.50, 0.30, 0.20],
    "notify_on_trade_close": false
  },
  "reporting": {
    "periodic_interval_hours": 3,
    "daily_summary_enabled": true
  }
}
```

### Docker Compose (уже настроено)

```yaml
signal-performance-tracker:
  container_name: scanner-signal-tracker
  environment:
    - REDIS_URL=redis://scanner-redis-worker-1:6379/0
    - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
    - TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
    - TRACKER_CONFIG_PATH=/app/python-worker/config/signal_tracker_config.json
  command: ['sh', '-c', 'sleep 90 && python -m services.signal_performance_tracker']
```

---

## 🧪 Тестирование

### Тест 1: Проверка инициализации

```bash
docker logs scanner-signal-tracker | grep "инициализирован"

# Ожидается:
# ✅ Redis подключение установлено
# 🎯 Trade Monitor инициализирован
# ✅ All threads started
```

### Тест 2: Симуляция сигнала

```bash
# Добавить тестовый сигнал в Redis
redis-cli XADD signals:orderflow:XAUUSD * \
  symbol XAUUSD \
  side LONG \
  entry 2055.50 \
  sl 2050.00 \
  tp_levels '[2060.0, 2065.0, 2070.0]' \
  lot 0.10 \
  source OrderFlow \
  atr 0.60 \
  timestamp $(date +%s)000

# Проверить что позиция открыта
redis-cli XLEN events:trades
redis-cli XREVRANGE events:trades + - COUNT 1
```

### Тест 3: Симуляция тиков для срабатывания TP

```bash
# Отправить тик с ценой выше TP1
curl -X POST http://localhost:8087/tick \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"XAUUSD","ts":'$(date +%s)'000,"bid":2060.0,"ask":2060.1,"last":2060.05,"volume":1.5,"flags":6}'

# Проверить events
redis-cli XREVRANGE events:trades + - COUNT 5
# Должен увидеть TP событие
```

---

## 📊 Мониторинг

### Статистика в Redis

```bash
# Общая статистика
redis-cli HGETALL stats:orderflow:XAUUSD:tick

# По источникам
redis-cli HGETALL stats:orderflow:XAUUSD:tick:OrderFlow
redis-cli HGETALL stats:orderflow:XAUUSD:tick:AggregatedHub-V2

# Закрытые сделки
redis-cli LLEN closed:orderflow:XAUUSD:tick
redis-cli LRANGE closed:orderflow:XAUUSD:tick 0 10
```

### Логи

```bash
# Реальное время
docker logs -f scanner-signal-tracker

# Фильтр по событиям
docker logs scanner-signal-tracker | grep "Position opened"
docker logs scanner-signal-tracker | grep "TP.*hit"
docker logs scanner-signal-tracker | grep "SL hit"
docker logs scanner-signal-tracker | grep "Position closed"
```

### Makefile команды

```bash
make tracker-status   # Статус трекера
make tracker-logs     # Логи трекера
make tracker-restart  # Перезапуск трекера
```

---

## 🔍 Интеграция с XAUUSD Flow

### Полный путь данных

```
MT5 → Tick Ingest → stream:tick_XAUUSD
                          │
                          ├─→ Multi-Symbol OrderFlow
                          │       └─→ signals:orderflow:XAUUSD
                          │
                          ├─→ Signal Generator (TA)
                          │       └─→ signals:ta:XAUUSD
                          │
                          └─→ Signal Performance Tracker ⭐
                                  │
                                  ├─→ Trade Monitor
                                  │   • Opens virtual position
                                  │   • Monitors ticks
                                  │   • Checks TP/SL
                                  │   • Partial close
                                  │
                                  ├─→ Stats Aggregator
                                  │   • Updates metrics
                                  │   • WinRate, P/L
                                  │
                                  └─→ Reporting Service
                                      • Generates reports
                                      • Sends to Telegram
```

### Streams взаимодействие

```
INPUT streams:
  • signals:orderflow:XAUUSD  (reads via XREADGROUP)
  • signals:ta:XAUUSD         (reads via XREADGROUP)
  • stream:tick_XAUUSD        (reads via XREADGROUP)

OUTPUT streams:
  • events:trades             (XADD: OPEN, TP, SL, CLOSE)
  • trades:closed             (XADD: trade summaries)
  • notify:telegram           (XADD: reports)

HASHES:
  • order:{id}                (HSET: position data)
  • stats:{strategy}:{symbol}:{tf}        (HINCRBY: metrics)
  • stats:{strategy}:{symbol}:{tf}:{source}
```

---

## ⚙️ Конфигурация

### Environment Variables (docker-compose.yml)

```yaml
environment:
  - REDIS_URL=redis://scanner-redis-worker-1:6379/0
  - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
  - TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
  - TRACKER_CONFIG_PATH=/app/python-worker/config/signal_tracker_config.json
  
  # Optional overrides:
  - SYMBOLS=XAUUSD,BTCUSD,ETHUSD
  - STRATEGIES=orderflow,ta
  - DEFAULT_LOT=1.0
  - RISK_PCT=1.0
  - NOTIFY_ON_TRADE_CLOSE=false
  - PERIODIC_REPORT_HOURS=3
  - DAILY_SUMMARY=true
```

### Partial Close Ratio

```python
tp_ratio = [0.50, 0.30, 0.20]

# При достижении:
# TP1: Закрывает 50% позиции
# TP2: Закрывает 30% позиции
# TP3: Закрывает 20% позиции
```

### Risk/Reward Levels

```python
rr_levels = [1.0, 2.0, 3.0]

# Если ATR = 0.60:
# TP1 = Entry ± (0.60 × 1.0) = ±0.60
# TP2 = Entry ± (0.60 × 2.0) = ±1.20
# TP3 = Entry ± (0.60 × 3.0) = ±1.80
```

---

## 📱 Telegram Отчеты

### Периодический отчет (каждые 3 часа)

```
📊 XAUUSD Performance Report (3h)

Strategy: orderflow
Symbol: XAUUSD
Period: Last 3 hours

📈 Overall Statistics:
Total Trades: 15
Wins: 9 (60.0%)
Losses: 6 (40.0%)
Total P/L: +125.50
Avg P/L: +8.37

🎯 TP Performance:
TP1 hits: 12/15 (80%)
TP2 hits: 7/15 (47%)
TP3 hits: 3/15 (20%)

💡 By Source:
OrderFlow: 8 trades, WinRate 62.5%
AggregatedHub-V2: 7 trades, WinRate 57.1%
```

### Ежедневная сводка (00:00 UTC)

```
📅 Daily Summary - XAUUSD

Total Trades: 42
WinRate: 64.3%
Total P&L: +342.80
Profit Factor: 2.15

Best Trade: +45.20
Worst Trade: -15.30
Avg Duration: 2.5h
```

---

## 🛠️ Полезные команды

### Проверка статистики

```bash
# Общая статистика
redis-cli HGETALL stats:orderflow:XAUUSD:tick

# По источникам
redis-cli KEYS "stats:orderflow:XAUUSD:tick:*"
redis-cli HGETALL stats:orderflow:XAUUSD:tick:OrderFlow
```

### Просмотр событий

```bash
# Последние 10 событий
redis-cli XREVRANGE events:trades + - COUNT 10

# Последние 10 закрытых сделок
redis-cli XREVRANGE trades:closed + - COUNT 10
```

### Открытые позиции

Позиции хранятся в памяти сервиса. Для доступа нужно:

```bash
# Проверить логи
docker logs scanner-signal-tracker | grep "Position opened"
docker logs scanner-signal-tracker | grep "Position closed"

# Или через Redis
redis-cli KEYS "order:*"
redis-cli HGETALL order:{some-uuid}
```

---

## ✅ Что сделано

- [x] **Trade Monitor** реализован (400+ строк)
- [x] **Signal Performance Tracker** реализован (350+ строк)
- [x] **Config file** создан (`config/signal_tracker_config.json`)
- [x] **Docker integration** - автозапуск при `make up`
- [x] **Multi-threading** - 3 потока (signals, ticks, periodic)
- [x] **Consumer groups** - guaranteed delivery
- [x] **Graceful shutdown** - SIGTERM/SIGINT handling
- [x] **Stats by source** - OrderFlow vs TA vs Hub
- [x] **Partial close** - 50%/30%/20% на TP1/TP2/TP3
- [x] **Periodic reports** - каждые 3 часа в Telegram
- [x] **Daily summaries** - ежедневные сводки

---

## 📚 Файлы

```
python-worker/
  ├── services/
  │   ├── trade_monitor.py                  ← ✅ 400+ строк (СОЗДАН)
  │   ├── signal_performance_tracker.py     ← ✅ 350+ строк (СОЗДАН)
  │   ├── stats_aggregator.py               ← ✅ 21KB (существовал)
  │   ├── reporting_service.py              ← ✅ 30KB (существовал)
  │   └── README_SIGNAL_TRACKER.md          ← Документация
  │
  ├── config/
  │   └── signal_tracker_config.json        ← ✅ Конфиг (СОЗДАН)
  │
  └── run_performance_tracker.py            ← Standalone запуск
```

---

## 🎯 Next Steps

### Immediate
- [x] Создать Trade Monitor ✅
- [x] Создать Signal Performance Tracker ✅
- [x] Создать конфиг ✅
- [x] Запустить сервис ✅
- [ ] Протестировать с real signals
- [ ] Проверить отправку отчетов

### Short-term
- [ ] Dashboard в Grafana для метрик
- [ ] Алерты на низкий WinRate
- [ ] Export в CSV/Excel
- [ ] Web UI для просмотра статистики

---

**Статус**: ✅ **INTEGRATION COMPLETE**  
**Контейнер**: `scanner-signal-tracker` **RUNNING**  
**Автозапуск**: ✅ Настроен при `make up`

🎉 Signal Performance Tracker готов к работе!

