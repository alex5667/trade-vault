# 📊 ЧАСОВЫЕ ОТЧЕТЫ ПО ИСТОЧНИКАМ СИГНАЛОВ

## ✅ ЧТО РЕАЛИЗОВАНО

### 📝 Обновленные файлы:
1. **Makefile** - добавлен `--profile default` для запуска periodic-reporter
2. **periodic_reporter.py** - отдельные отчеты по каждому источнику каждый час
3. **reporting_service.py** - полные метрики в каждом отчете
4. **docker-compose.yml** - интервал 1 час, профиль default

## 🎯 ФОРМАТ ОТЧЕТОВ

Каждый час отправляется **3 отдельных отчета**:

### 1️⃣ OrderFlow
```
📊 HOURLY REPORT: OrderFlow (XAUUSD)
══════════════════════════════════════

📈 ОСНОВНЫЕ МЕТРИКИ
Всего сделок: 253
Выигрышей: 94 (37.2%)
Проигрышей: 159
Общий P/L: -204.12
Средний P/L: -0.80

🎯 TP МЕТРИКИ (частичное закрытие)
TP1 (50%): 121 достигнуто (47.9%)
TP2 (30%): 75 достигнуто (29.8%)
TP3 (20%): 40 достигнуто (16.0%)

⭐ УПУЩЕННАЯ ПРИБЫЛЬ (TP→SL)
TP1→SL: 32 (12.8%) ⚠️
TP2→SL: 21 (8.5%)
TP3→SL: 8 (3.2%)

💡 РЕКОМЕНДАЦИЯ: Высокий процент TP1→SL
```

### 2️⃣ AggregatedHub-V2
```
📊 HOURLY REPORT: AggregatedHub-V2 (XAUUSD)
══════════════════════════════════════
[Те же метрики для AggregatedHub-V2]
```

### 3️⃣ TechnicalAnalysis
```
📊 HOURLY REPORT: TechnicalAnalysis (XAUUSD)
══════════════════════════════════════
[Те же метрики для TechnicalAnalysis]
```

## 📊 МЕТРИКИ В КАЖДОМ ОТЧЕТЕ

### Основные метрики (8):
- `total_trades` - Всего сделок
- `wins` - Выигрышей
- `losses` - Проигрышей
- `winrate` - Win Rate (%)
- `total_pnl` - Общий P/L
- `avg_pnl` - Средний P/L
- `total_pnl_pct` - Общий P/L (%)
- `avg_pnl_pct` - Средний P/L (%)

### TP метрики (6):
- `tp1_hits` / `tp1_rate` - TP1 достигнуто
- `tp2_hits` / `tp2_rate` - TP2 достигнуто
- `tp3_hits` / `tp3_rate` - TP3 достигнуто

### Упущенная прибыль (6):
- `tp1_then_sl` / `tp1_then_sl_rate` - TP1→SL
- `tp2_then_sl` / `tp2_then_sl_rate` - TP2→SL
- `tp3_then_sl` / `tp3_then_sl_rate` - TP3→SL

**Итого: 20+ метрик в каждом отчете**

## ⏰ РАСПИСАНИЕ

| Время | Действие |
|-------|----------|
| Каждый час | 3 отчета (OrderFlow, AggregatedHub-V2, TechnicalAnalysis) |
| 00:00 UTC | 3 ежедневных сводки (по каждому источнику) |

## 🚀 ЗАПУСК

### Вариант 1: Make (рекомендуется)
```bash
make up
# или
make up-bg  # в фоне
```

### Вариант 2: Docker Compose
```bash
docker-compose --profile default up -d
```

### Проверка статуса
```bash
docker ps | grep periodic-reporter
docker logs scanner-periodic-reporter --tail 50 -f
```

## 🔍 МОНИТОРИНГ

### Проверить, когда отправлен последний отчет
```bash
docker logs scanner-periodic-reporter | grep "HOURLY REPORT"
```

### Проверить Redis stream с уведомлениями
```bash
docker exec scanner-redis-worker-1 redis-cli XREVRANGE notify:telegram + - COUNT 10
```

### Проверить метрики конкретного источника
```bash
# OrderFlow
docker exec scanner-redis-worker-1 redis-cli HGETALL "signal:perf:orderflow:XAUUSD:1h"

# AggregatedHub-V2
docker exec scanner-redis-worker-1 redis-cli HGETALL "signal:perf:aggregated:XAUUSD:1h"

# TechnicalAnalysis
docker exec scanner-redis-worker-1 redis-cli HGETALL "signal:perf:ta:XAUUSD:1h"
```

## 📚 СВЯЗАННАЯ ДОКУМЕНТАЦИЯ

- `SIGNAL_TRACKER_METRICS.md` - Полное описание всех метрик
- `METRICS_QUICK_REFERENCE.md` - Краткая справка по метрикам
- `SIGNAL_TRACKER_INTEGRATION.md` - Интеграция трекера сигналов

## 🎓 АРХИТЕКТУРА

```
┌─────────────────────────────────────────────────┐
│         Signal Performance Tracker              │
│  (читает из signals:orderflow/ta/aggregated)    │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│            Redis Hash Storage                   │
│  signal:perf:{source}:XAUUSD:{timeframe}        │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│          Periodic Reporter                      │
│  (каждый час - 3 отдельных отчета)             │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│      Redis Stream: notify:telegram              │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│           Telegram Notify Worker                │
│         (отправка в Telegram бот)               │
└─────────────────────────────────────────────────┘
```

## ✅ ПРОВЕРКА РАБОТЫ

1. **Запустить систему:**
   ```bash
   make up-bg
   ```

2. **Подождать до начала следующего часа** (например, 20:00, 21:00)

3. **Проверить логи:**
   ```bash
   docker logs scanner-periodic-reporter -f
   ```
   
   Должны увидеть:
   ```
   [INFO] Sending hourly report for source: orderflow
   [INFO] Sending hourly report for source: aggregated
   [INFO] Sending hourly report for source: ta
   ```

4. **Проверить Telegram бот** - должны прийти 3 отдельных сообщения

## 🐛 TROUBLESHOOTING

### Отчеты не приходят
```bash
# 1. Проверить, запущен ли periodic-reporter
docker ps | grep periodic-reporter

# 2. Проверить логи на ошибки
docker logs scanner-periodic-reporter --tail 100

# 3. Проверить подключение к Redis
docker exec scanner-periodic-reporter python -c "import redis; r=redis.Redis(host='scanner-redis-worker-1', port=6379); print(r.ping())"

# 4. Проверить, есть ли данные в Redis
docker exec scanner-redis-worker-1 redis-cli KEYS "signal:perf:*"
```

### Отчеты пустые (нет данных)
```bash
# 1. Проверить, работает ли signal-performance-tracker
docker ps | grep signal-tracker

# 2. Проверить логи трекера
docker logs scanner-signal-tracker --tail 100

# 3. Проверить, есть ли сигналы в стримах
docker exec scanner-redis-worker-1 redis-cli XLEN signals:orderflow:XAUUSD
docker exec scanner-redis-worker-1 redis-cli XLEN signals:ta:XAUUSD
docker exec scanner-redis-worker-1 redis-cli XLEN signals:aggregated:XAUUSD
```

### notify-worker не отправляет в Telegram
```bash
# 1. Проверить логи notify-worker
docker logs scanner-notify-worker --tail 100

# 2. Проверить стрим notify:telegram
docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram

# 3. Проверить consumer group
docker exec scanner-redis-worker-1 redis-cli XINFO GROUPS notify:telegram
```

---

**Статус:** ✅ Готово к использованию  
**Версия:** 1.0  
**Дата:** 2025-11-06  
**Команда:** Senior Go/Python Developer + Senior Trading Systems Analyst (40 лет опыта)
