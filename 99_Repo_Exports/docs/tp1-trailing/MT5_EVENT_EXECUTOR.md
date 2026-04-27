# MT5 Event Executor - Приём событий от MT5

## 🎯 Что это?

**Event Executor** - критичный компонент, который замыкает цикл между MT5 и системой трейлинга.

**Функции:**

1. Принимает вебхуки от MT5 EA (POST /events/mt5)
2. Классифицирует события (TP1/TP2/TP3/SL/OPEN/CLOSE)
3. Обновляет состояние сделки (trade:state:{sid})
4. Публикует события в Redis streams (events:trades)
5. Интеграция с trade_events_logger для trade_back

---

## 🏗️ Архитектура

```
MT5 EA → POST /events/mt5 → Event Executor → Redis
                                ↓
                          Классификация
                                ↓
                    ┌──────────────┬──────────────┐
                    ↓              ↓              ↓
            trade:state:{sid}  events:trades  TradeEventsLogger
                    ↓              ↓              ↓
                Analytics     trade_back     Full History
```

---

## 📦 Компоненты

### 1. FastAPI Endpoint

```python
POST /events/mt5

Body:
{
  "symbol": "XAUUSD",
  "deal": 1234567,
  "position": 123456,
  "type": 1,
  "price": 3980.10,
  "profit": 3.25,
  "comment": "signal-XAUUSD-123"  ← sid сигнала
}
```

### 2. Классификатор

Определяет тип события на основе:

- Цена vs TP levels
- Цена vs SL
- Profit (положительная/отрицательная)
- Volume (частичное/полное закрытие)

**Результат:**

- `POSITION_OPENED` - Позиция открыта
- `TP1_HIT` - TP1 достигнут
- `TP2_HIT` - TP2 достигнут
- `TP3_HIT` - TP3 достигнут
- `SL_HIT` - Stop Loss
- `POSITION_CLOSED` - Закрытие (manual или итоговое)

### 3. State Manager

Сохраняет состояние каждой сделки в Redis:

```json
{
  "sid": "signal-XAUUSD-123",
  "tp1_hit": true,
  "tp2_hit": false,
  "tp3_hit": false,
  "sl_hit": false,
  "opened_at": 1730222790000,
  "closed_at": null,
  "pnl_realized": 45.50,
  "volume_opened": 0.03,
  "volume_closed": 0.015,
  "events": [...]
}
```

**Ключ:** `trade:state:{sid}`  
**TTL:** 7 дней

---

## 🚀 Запуск

### Через Makefile

```bash
# Запуск
make mt5-executor-start

# Статус
make mt5-executor-status

# Логи
make mt5-executor-logs

# Тест
make mt5-executor-test
```

### Через Docker Compose

```bash
# Запуск
docker-compose -f docker-compose.yml -f docker-compose.mt5-executor.yml up -d mt5-event-executor

# Проверка
curl http://localhost:8091/health
```

---

## 📡 Интеграция с MT5 EA

### В MT5 добавьте:

```mql5
// URL для event executor
string g_EventExecutorURL = "http://scanner-mt5-event-executor:8091";

// При каждой сделке (OnTradeTransaction)
void OnTradeTransaction(
    const MqlTradeTransaction& trans,
    const MqlTradeRequest& request,
    const MqlTradeResult& result
)
{
    // Формируем JSON
    string json = "";
    json += "{";
    json += "\"symbol\":\"" + trans.symbol + "\",";
    json += "\"deal\":" + IntegerToString(trans.deal) + ",";
    json += "\"position\":" + IntegerToString(trans.position) + ",";
    json += "\"type\":" + IntegerToString(trans.type) + ",";
    json += "\"price\":" + DoubleToString(trans.price, _Digits) + ",";
    json += "\"profit\":" + DoubleToString(trans.profit, 2) + ",";
    json += "\"comment\":\"" + trans.comment + "\",";  // sid здесь!
    json += "\"volume\":" + DoubleToString(trans.volume, 2);
    json += "}";

    // Отправляем в executor
    SendToExecutor(json);
}

bool SendToExecutor(string json)
{
    char data[];
    char result[];
    string headers = "Content-Type: application/json\r\n";

    StringToCharArray(json, data, 0, StringLen(json));

    string url = g_EventExecutorURL + "/events/mt5";

    int res = WebRequest("POST", url, headers, 3000, data, result, headers);

    if(res == 200)
    {
        Print("✅ Event sent to executor");
        return true;
    }
    else
    {
        Print("❌ Failed to send event: status=", res);
        return false;
    }
}
```

---

## 🔍 Примеры использования

### Проверка состояния сделки

```bash
# Через API
curl http://localhost:8091/signal/signal-XAUUSD-123/state

# Через Redis
redis-cli GET trade:state:signal-XAUUSD-123 | jq .
```

### Получение всех событий по сигналу

```bash
# Через API
curl http://localhost:8091/signal/signal-XAUUSD-123/events

# Через Python
python -c "
from services.trade_events_logger import TradeEventsLogger
logger = TradeEventsLogger()
events = logger.get_signal_events('signal-XAUUSD-123')
for event in events:
    print(f\"{event['event_type']}: {event.get('price', 'N/A')}\")
"
```

### Статистика executor

```bash
curl http://localhost:8091/stats | jq .

# Вывод:
{
  "events_in_stream": 1523,
  "trade_states": 245,
  "events_logger_stats": {
    "events_written": 1523,
    "tp1_hits": 120,
    "tp2_hits": 65,
    "tp3_hits": 28,
    "sl_hits": 95,
    "trailing_moves": 380
  }
}
```

---

## 📊 Анализ для trade_back

### TP1→SL паттерн

```python
import redis
import json

r = redis.from_url('redis://scanner-redis:6379/0', decode_responses=True)

tp1_then_sl = 0
tp1_then_tp2 = 0

# Проверяем все trade states
for key in r.keys('trade:state:signal-*'):
    state_json = r.get(key)
    state = json.loads(state_json)

    if state['tp1_hit']:
        if state['sl_hit'] and not state['tp2_hit']:
            tp1_then_sl += 1
        elif state['tp2_hit']:
            tp1_then_tp2 += 1

print(f"TP1→SL:  {tp1_then_sl}")
print(f"TP1→TP2: {tp1_then_tp2}")

if tp1_then_sl + tp1_then_tp2 > 0:
    success_rate = tp1_then_tp2 / (tp1_then_sl + tp1_then_tp2) * 100
    print(f"Success rate: {success_rate:.1f}%")
```

### Полная история сделки

```python
from services.trade_events_logger import TradeEventsLogger

logger = TradeEventsLogger()
outcome = logger.calculate_signal_outcome('signal-XAUUSD-123')

print(json.dumps(outcome, indent=2))

# Вывод:
{
  "sid": "signal-XAUUSD-123",
  "position_opened": true,
  "tp1_hit": true,
  "tp2_hit": true,
  "tp3_hit": false,
  "sl_hit": false,
  "trailing_started": true,
  "trailing_moves": 5,
  "max_sl": 2771.4,
  "min_sl": 2758.7,
  "final_pnl": 150.25,
  "lifetime_ms": 3600000,
  "close_reason": "tp2"
}
```

---

## 🧪 Тестирование

### Эмуляция события от MT5

```bash
# Тест TP1_HIT
curl -X POST http://localhost:8091/events/mt5 \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "XAUUSD",
    "deal": 1234567,
    "position": 123456,
    "type": 1,
    "price": 2769.9,
    "profit": 15.50,
    "comment": "test-signal-123",
    "volume": 0.015
  }'

# Проверка result
curl http://localhost:8091/signal/test-signal-123/state
```

### Создание тестового сигнала

```python
import redis, json, time

r = redis.from_url('redis://localhost:6379/0', decode_responses=True)

signal = {
    'sid': 'test-signal-123',
    'symbol': 'XAUUSD',
    'side': 'LONG',
    'entry': 2765.5,
    'sl': 2758.7,
    'tp_levels': [2769.9, 2773.1, 2776.3],
    'atr': 2.5
}

r.set('signals:test-signal-123', json.dumps(signal), ex=3600)
print('✅ Test signal created')
```

---

## 📈 Метрики и мониторинг

### Health Check

```bash
make mt5-executor-health

# Или напрямую
curl http://localhost:8091/health

# Вывод:
{
  "status": "healthy",
  "redis": "connected",
  "events_logger": true,
  "timestamp": 1730222790000
}
```

### Статистика

```bash
make mt5-executor-stats

# Или напрямую
curl http://localhost:8091/stats

# Вывод:
{
  "events_in_stream": 1523,
  "trade_states": 245,
  "events_logger_stats": {
    "events_written": 1523,
    "tp1_hits": 120,
    "tp2_hits": 65
  }
}
```

---

## 🔧 Конфигурация

### Environment Variables

```bash
# Redis
REDIS_URL=redis://scanner-redis:6379/0
TRADE_EVENTS_STREAM=events:trades
SIGNAL_PREFIX=signals:
TRADE_STATE_PREFIX=trade:state:

# Classification
PRICE_TOLERANCE=0.5  # Допуск по цене

# Service
MT5_EXECUTOR_HOST=0.0.0.0
MT5_EXECUTOR_PORT=8091
```

### Docker

```yaml
mt5-event-executor:
  ports:
    - '8091:8091' # HTTP endpoint для MT5
  environment:
    - REDIS_URL=redis://scanner-redis:6379/0
    - MT5_EXECUTOR_PORT=8091
```

---

## 📊 Что даёт trade_back

### 1. Фиксация TP1→SL

```python
# В trade:state:{sid} будет:
{
  "tp1_hit": true,   ← TP1 был достигнут
  "sl_hit": true,    ← Но итог - SL
  "tp2_hit": false
}

# Это критичная метрика "упущенная прибыль"!
```

### 2. Частичные TP

```python
# События фиксируются по времени:
state["events"] = [
  {"ts": 1730222790, "event_type": "TP1_HIT", "volume": 0.015},
  {"ts": 1730222850, "event_type": "TP2_HIT", "volume": 0.01},
  {"ts": 1730222920, "event_type": "TP3_HIT", "volume": 0.005}
]

# Можно рассчитать:
# - Среднее время до TP1
# - Доля сделок, дошедших до TP2
# - Частичные vs полные закрытия
```

### 3. Готовый stream для trade_back

```python
# events:trades содержит ВСЁ
# Легко заберёт trade_back и запишет в БД/Parquet

# Пример записи:
{
  "sid": "signal-XAUUSD-123",
  "event_type": "TP2_HIT",
  "price": 2773.1,
  "profit": 25.50,
  "ts": 1730222850,
  "state": { полное состояние сделки }
}
```

---

## 🎓 Best Practices

### 1. Всегда передавайте sid в comment

```mql5
// В MT5 при открытии позиции
string comment = "signal-XAUUSD-" + IntegerToString(TimeCurrent());
// Используйте этот же comment для всех частичных закрытий
```

### 2. Передавайте volume

```mql5
// В JSON добавьте:
"volume": trans.volume

// Это позволит отследить частичные закрытия
```

### 3. Мониторьте executor

```bash
# Каждые 5 минут проверяйте
*/5 * * * * make mt5-executor-health

# Если unhealthy - restart
make mt5-executor-start
```

---

## 🐛 Troubleshooting

### Executor не получает события

```bash
# 1. Проверьте доступность
curl http://localhost:8091/health

# 2. Проверьте логи
make mt5-executor-logs

# 3. Проверьте MT5 EA
# В MT5: Tools → Options → Expert Advisors
# Allow WebRequest для: http://scanner-mt5-event-executor:8091
```

### События не классифицируются

```bash
# Проверьте наличие сигнала в Redis
redis-cli GET signals:your-signal-id

# Проверьте формат TP levels
redis-cli GET signals:your-signal-id | jq .tp_levels

# Проверьте PRICE_TOLERANCE
# Возможно нужно увеличить для волатильных инструментов
```

### Дубликаты событий

```bash
# MT5 может слать несколько раз
# Executor обрабатывает это корректно
# Проверьте логи:
make mt5-executor-logs | grep "already classified"
```

---

## ✅ Integration Complete

**Создано:**

- ✅ `mt5_event_executor.py` - FastAPI сервис
- ✅ `docker-compose.mt5-executor.yml` - Docker конфигурация
- ✅ Makefile commands - управление сервисом
- ✅ Интеграция с TradeEventsLogger
- ✅ Документация (этот файл)

**Готово к приёму событий от MT5!** 📡

---

**Version**: 1.0.0  
**Date**: 2025-11-06  
**Port**: 8091
